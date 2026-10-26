package com.dist.rate.engine;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public final class DistributedRateLimiter implements AutoCloseable {

    private static final long CACHE_LINE_SIZE = 64;
    private static final long SLOT_SIZE_BYTES = 8; // 64-bit atomic sequence per slot
    
    private final long capacity;
    private final long refillTokensPerSecond;
    private final int windowSlots;
    private final long windowDurationMillis;
    private final long slotDurationMillis;
    
    private final Arena arena;
    private final MemorySegment ringBufferSegment;
    private final long baseAddress;
    
    private final AtomicBoolean isClosed;
    
    // Padded sequence state to prevent false sharing
    private static final VarHandle SEQ_HANDLE;
    
    static {
        try {
            SEQ_HANDLE = MethodHandles.memorySegmentViewVarHandle(
                java.lang.foreign.ValueLayout.JAVA_LONG,
                ByteOrder.nativeOrder()
            );
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // Padded fields for state tracking
    private long p0, p1, p2, p3, p4, p5, p6, p7;
    private volatile long localGeneration;
    private long q0, q1, q2, q3, q4, q5, q6, q7;

    public DistributedRateLimiter(long capacity, long refillTokensPerSecond, int windowSlots, long windowDurationMillis) {
        if (capacity <= 0 || refillTokensPerSecond <= 0 || windowSlots <= 0 || windowDurationMillis <= 0) {
            throw new IllegalArgumentException("Invalid rate limiter parameters");
        }
        
        this.capacity = capacity;
        this.refillTokensPerSecond = refillTokensPerSecond;
        this.windowSlots = windowSlots;
        this.windowDurationMillis = windowDurationMillis;
        this.slotDurationMillis = windowDurationMillis / windowSlots;
        this.isClosed = new AtomicBoolean(false);
        
        long totalBytes = (long) windowSlots * SLOT_SIZE_BYTES;
        long paddedBytes = ((totalBytes + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
        
        this.arena = Arena.ofShared();
        this.ringBufferSegment = arena.allocate(paddedBytes, CACHE_LINE_SIZE);
        this.baseAddress = ringBufferSegment.address();
        
        // Initialize ring buffer slots with current epoch and max capacity
        long initialEpoch = System.currentTimeMillis();
        long initialPacked = pack(initialEpoch, capacity);
        
        for (int i = 0; i < windowSlots; i++) {
            long offset = (long) i * SLOT_SIZE_BYTES;
            SEQ_HANDLE.set(ringBufferSegment, offset, initialPacked);
        }
    }

    private static long pack(long epochMillis, long tokens) {
        // Upper 32 bits: epoch millis (masked), Lower 32 bits: tokens
        return ((epochMillis & 0xFFFFFFFFL) << 32) | (tokens & 0xFFFFFFFFL);
    }

    private static long unpackEpoch(long packed) {
        return (packed >>> 32) & 0xFFFFFFFFL;
    }

    private static long unpackTokens(long packed) {
        return packed & 0xFFFFFFFFL;
    }

    public boolean tryConsume(long requestedTokens) {
        if (requestedTokens <= 0) {
            return true;
        }
        if (requestedTokens > capacity) {
            return false;
        }
        if (isClosed.get()) {
            throw new IllegalStateException("Rate limiter is closed");
        }

        int spins = 0;
        while (true) {
            long now = System.currentTimeMillis();
            int slotIndex = (int) ((now / slotDurationMillis) % windowSlots);
            long offset = (long) slotIndex * SLOT_SIZE_BYTES;

            long currentPacked = (long) SEQ_HANDLE.get(ringBufferSegment, offset);
            long slotEpoch = unpackEpoch(currentPacked);
            long slotTokens = unpackTokens(currentPacked);

            // Time monotonicity check and sliding window adjustment
            long timeDelta = now - slotEpoch;
            if (timeDelta < -1000) {
                // Clock regression beyond epsilon (1 second), clamp to current
                timeDelta = 0;
            } else if (timeDelta < 0) {
                timeDelta = 0; // Micro-adjust for minor skew
            }

            long refilledTokens = slotTokens;
            if (timeDelta > 0) {
                long tokensToAdd = (timeDelta * refillTokensPerSecond) / 1000L;
                refilledTokens = Math.min(capacity, slotTokens + tokensToAdd);
            }

            if (refilledTokens < requestedTokens) {
                return false; // Rate limit exceeded in current slot
            }

            long nextTokens = refilledTokens - requestedTokens;
            long newPacked = pack(now, nextTokens);

            if (SEQ_HANDLE.compareAndSet(ringBufferSegment, offset, currentPacked, newPacked)) {
                return true;
            }

            // Exponential backoff via spin wait to reduce memory bus contention
            spins++;
            if (spins > 64) {
                Thread.onSpinWait();
                if (spins > 1024) {
                    LockSupport.parkNanos(100);
                }
            }
        }
    }

    public long getAvailableTokens() {
        long now = System.currentTimeMillis();
        long totalTokens = 0;
        
        for (int i = 0; i < windowSlots; i++) {
            long offset = (long) i * SLOT_SIZE_BYTES;
            long currentPacked = (long) SEQ_HANDLE.get(ringBufferSegment, offset);
            long slotEpoch = unpackEpoch(currentPacked);
            long slotTokens = unpackTokens(currentPacked);
            
            long timeDelta = now - slotEpoch;
            if (timeDelta > 0) {
                long tokensToAdd = (timeDelta * refillTokensPerSecond) / 1000L;
                slotTokens = Math.min(capacity, slotTokens + tokensToAdd);
            }
            totalTokens += slotTokens;
        }
        
        return Math.min(capacity, totalTokens / windowSlots);
    }

    public void reconcile(long remoteEpoch, long remoteTokens) {
        if (isClosed.get()) {
            return;
        }
        int spins = 0;
        while (true) {
            int slotIndex = (int) ((remoteEpoch / slotDurationMillis) % windowSlots);
            long offset = (long) slotIndex * SLOT_SIZE_BYTES;

            long currentPacked = (long) SEQ_HANDLE.get(ringBufferSegment, offset);
            long currentEpoch = unpackEpoch(currentPacked);
            long currentTokens = unpackTokens(currentPacked);

            // Vector clock / timestamp reconciliation: accept state if remote is newer or merge tokens
            long resolvedEpoch = Math.max(currentEpoch, remoteEpoch);
            long resolvedTokens = Math.min(currentTokens, remoteTokens);
            long reconciledPacked = pack(resolvedEpoch, resolvedTokens);

            if (SEQ_HANDLE.compareAndSet(ringBufferSegment, offset, currentPacked, reconciledPacked)) {
                return;
            }

            spins++;
            if (spins > 64) {
                Thread.onSpinWait();
            }
        }
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            arena.close();
        }
    }
}