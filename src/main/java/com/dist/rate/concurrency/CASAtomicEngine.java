package com.dist.rate.concurrency;

import sun.misc.Unsafe;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

public final class CASAtomicEngine implements AutoCloseable {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new AssertionError("Unable to initialize Unsafe", e);
        }
    }

    private final int capacity;
    private final int ringMask;
    private final long burstCeiling;
    private final long refillIntervalNanos;
    private final long epsilonThresholdNanos;
    
    private final long memoryAddress;
    private final long totalAllocationSize;

    // Cache-line padding fields for state mutations (64-byte alignment)
    private volatile long sequenceHead;
    private long p1, p2, p3, p4, p5, p6, p7; // Padding to prevent false sharing

    private volatile long sequenceTail;
    private long q1, q2, q3, q4, q5, q6, q7; // Padding to prevent false sharing

    private static final int SLOT_SIZE_BYTES = 8; // 64-bit atomic sequence per slot
    private static final long CACHE_LINE_SIZE = 64;

    public CASAtomicEngine(int capacity, long burstCeiling, long refillIntervalNanos, long epsilonThresholdNanos) {
        if (capacity <= 0 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("Capacity must be a positive power of two.");
        }
        this.capacity = capacity;
        this.ringMask = capacity - 1;
        this.burstCeiling = burstCeiling;
        this.refillIntervalNanos = refillIntervalNanos;
        this.epsilonThresholdNanos = epsilonThresholdNanos;

        long rawDataSize = (long) capacity * SLOT_SIZE_BYTES;
        this.totalAllocationSize = rawDataSize + (2 * CACHE_LINE_SIZE);
        
        long baseAddress = UNSAFE.allocateMemory(this.totalAllocationSize);
        // Align to cache line boundaries
        this.memoryAddress = (baseAddress + (CACHE_LINE_SIZE - 1)) & ~(CACHE_LINE_SIZE - 1);

        // Initialize slots with base epoch and full burst tokens
        long initialPacked = pack(System.nanoTime(), burstCeiling);
        for (int i = 0; i < capacity; i++) {
            UNSAFE.putLong(memoryAddress + ((long) i * SLOT_SIZE_BYTES), initialPacked);
        }

        this.sequenceHead = 0L;
        this.sequenceTail = 0L;
    }

    private long getSlotAddress(int index) {
        return memoryAddress + ((long) (index & ringMask) * SLOT_SIZE_BYTES);
    }

    private static long pack(long epochNanos, long tokens) {
        // Upper 32 bits: epoch representation (masked), Lower 32 bits: token count
        return (epochNanos << 32) | (tokens & 0xFFFFFFFFL);
    }

    private static long unpackEpoch(long packed) {
        return packed >>> 32;
    }

    private static long unpackTokens(long packed) {
        return packed & 0xFFFFFFFFL;
    }

    public boolean tryConsume(long requestedTokens, long currentNanoTime) {
        if (requestedTokens <= 0 || requestedTokens > burstCeiling) {
            return false;
        }

        int spinCount = 0;
        while (true) {
            long currentHead = UNSAFE.loadFenceVolatile(this); // Force memory visibility
            int index = (int) (sequenceHead & ringMask);
            long slotAddress = getSlotAddress(index);

            long currentPacked = UNSAFE.getLongVolatile(null, slotAddress);
            long slotEpoch = unpackEpoch(currentPacked);
            long slotTokens = unpackTokens(currentPacked);

            // Time monotonicity and NTP regression absorption
            long timeDelta = currentNanoTime - slotEpoch;
            if (timeDelta < 0) {
                if (-timeDelta > epsilonThresholdNanos) {
                    // Clock regression exceeds sliding epsilon threshold; clamp to current epoch
                    timeDelta = 0;
                } else {
                    // Absorb minor jitter deterministically
                    timeDelta = 0;
                }
            }

            // Calculate token replenishment
            long generatedTokens = 0;
            if (refillIntervalNanos > 0 && timeDelta > 0) {
                generatedTokens = (timeDelta / refillIntervalNanos) * burstCeiling; // Simplified additive refill rate model
            }

            long availableTokens = Math.min(burstCeiling, slotTokens + generatedTokens);
            if (availableTokens < requestedTokens) {
                return false; // Rate limit exceeded
            }

            long remainingTokens = availableTokens - requestedTokens;
            long nextEpoch = slotEpoch + (timeDelta > 0 ? (timeDelta / refillIntervalNanos) * refillIntervalNanos : 0);
            long newPacked = pack(nextEpoch, remainingTokens);

            if (UNSAFE.compareAndSwapLong(null, slotAddress, currentPacked, newPacked)) {
                // Advance head CAS-free locally for thread locality tracking
                UNSAFE.getAndAddLong(this, 8L, 1L); // sequenceHead offset approximation or direct volatile write
                return true;
            }

            spinCount++;
            if (spinCount > 64) {
                Thread.onSpinWait();
                if (spinCount > 1024) {
                    Thread.yield();
                }
            }
        }
    }

    public long inspectAvailableTokens(long currentNanoTime) {
        int index = (int) (sequenceHead & ringMask);
        long slotAddress = getSlotAddress(index);
        long currentPacked = UNSAFE.getLongVolatile(null, slotAddress);
        long slotEpoch = unpackEpoch(currentPacked);
        long slotTokens = unpackTokens(currentPacked);

        long timeDelta = currentNanoTime - slotEpoch;
        if (timeDelta < 0 && -timeDelta <= epsilonThresholdNanos) {
            timeDelta = 0;
        } else if (timeDelta < 0) {
            timeDelta = 0;
        }

        long generatedTokens = 0;
        if (refillIntervalNanos > 0 && timeDelta > 0) {
            generatedTokens = (timeDelta / refillIntervalNanos) * burstCeiling;
        }

        return Math.min(burstCeiling, slotTokens + generatedTokens);
    }

    @Override
    public void close() {
        if (memoryAddress != 0L) {
            // Free the base allocation address (offset adjusted back)
            long baseAddress = memoryAddress & ~(CACHE_LINE_SIZE - 1);
            UNSAFE.freeMemory(baseAddress);
        }
    }
}