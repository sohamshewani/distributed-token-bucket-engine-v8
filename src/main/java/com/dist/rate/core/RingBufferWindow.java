package com.dist.rate.core;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RingBufferWindow implements AutoCloseable {

    private static final Unsafe UNSAFE;

    static {
        try {
            PrivilegedExceptionAction<Unsafe> action = () -> {
                Field f = Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                return (Unsafe) f.get(null);
            };
            UNSAFE = AccessController.doPrivileged(action);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize Unsafe for distributed-token-bucket-engine", e);
        }
    }

    private static final long CACHE_LINE_SIZE = 64;
    private static final long SLOT_SIZE_BYTES = 8; // Each slot is a 64-bit atomic long

    private final int capacity;
    private final int mask;
    private final long burstCeiling;
    private final long windowDurationMillis;
    private final long epsilonThresholdMillis;
    private final boolean failClosed;

    private final long memoryAddress;
    private final long totalAllocationSize;
    private final AtomicBoolean isClosed;

    // Cache-line padding fields to prevent false sharing on state tracking
    private long p0, p1, p2, p3, p4, p5, p6, p7;
    private volatile long headSequence;
    private long q0, q1, q2, q3, q4, q5, q6, q7;
    private volatile long tailSequence;
    private long r0, r1, r2, r3, r4, r5, r6, r7;

    private static final long HEAD_OFFSET;
    private static final long TAIL_OFFSET;

    static {
        try {
            HEAD_OFFSET = UNSAFE.objectFieldOffset(RingBufferWindow.class.getDeclaredField("headSequence"));
            TAIL_OFFSET = UNSAFE.objectFieldOffset(RingBufferWindow.class.getDeclaredField("tailSequence"));
        } catch (Exception ex) {
            throw new RuntimeException("Failed to resolve field offsets for RingBufferWindow", ex);
        }
    }

    public RingBufferWindow(int requestedCapacity, long burstCeiling, long windowDurationMillis, 
                            long epsilonThresholdMillis, boolean failClosed) {
        if (burstCeiling <= 0) {
            throw new IllegalArgumentException("Burst ceiling must be strictly positive");
        }
        if (windowDurationMillis <= 0) {
            throw new IllegalArgumentException("Window duration must be strictly positive");
        }
        if (requestedCapacity <= 0 || (requestedCapacity & (requestedCapacity - 1)) != 0) {
            throw new IllegalArgumentException("Capacity must be a power of two");
        }

        this.capacity = requestedCapacity;
        this.mask = requestedCapacity - 1;
        this.burstCeiling = burstCeiling;
        this.windowDurationMillis = windowDurationMillis;
        this.epsilonThresholdMillis = epsilonThresholdMillis;
        this.failClosed = failClosed;
        this.isClosed = new AtomicBoolean(false);

        // Allocate off-heap memory with 64-byte alignment plus padding overhead
        long rawSize = (long) this.capacity * SLOT_SIZE_BYTES;
        this.totalAllocationSize = rawSize + CACHE_LINE_SIZE;
        long allocated = UNSAFE.allocateMemory(this.totalAllocationSize);
        
        // Align address to 64-byte cache line boundary
        long remainder = allocated % CACHE_LINE_SIZE;
        if (remainder != 0) {
            this.memoryAddress = allocated + (CACHE_LINE_SIZE - remainder);
        } else {
            this.memoryAddress = allocated;
        }

        // Initialize all slots to zero
        for (int i = 0; i < this.capacity; i++) {
            UNSAFE.putLong(getSlotAddress(i), 0L);
        }

        this.headSequence = 0L;
        this.tailSequence = 0L;
    }

    private long getSlotAddress(int index) {
        return memoryAddress + ((long) index * SLOT_SIZE_BYTES);
    }

    public boolean tryConsume(long tokensRequested, long currentTimestampMillis) {
        if (isClosed.get()) {
            return !failClosed;
        }
        if (tokensRequested <= 0) {
            return true;
        }
        if (tokensRequested > burstCeiling) {
            return false;
        }

        while (true) {
            long currentHead = UNSAFE.getLongVolatile(this, HEAD_OFFSET);
            long currentTail = UNSAFE.getLongVolatile(this, TAIL_OFFSET);

            // Time monotonicity validation and NTP adjustment absorption
            long adjustedTimestamp = currentTimestampMillis;
            int slotIndex = (int) (currentHead & mask);
            long slotValue = UNSAFE.getLong(getSlotAddress(slotIndex));
            long slotEpoch = slotValue >>> 32;

            if (slotEpoch > 0) {
                long timeDelta = currentTimestampMillis - slotEpoch;
                if (timeDelta < 0) {
                    // Clock regression detected
                    if (Math.abs(timeDelta) <= epsilonThresholdMillis) {
                        // Absorb within epsilon threshold by pinning to previous valid epoch
                        adjustedTimestamp = slotEpoch;
                    } else {
                        // Exceeds epsilon threshold; trigger fail-safe policy
                        return !failClosed;
                    }
                }
            }

            // Pack new state: Upper 32 bits = timestamp epoch, Lower 32 bits = token count
            long currentPackedTokens = slotValue & 0xFFFFFFFFL;
            long newPackedTokens = currentPackedTokens + tokensRequested;

            if (newPackedTokens > burstCeiling) {
                return false; // Exceeds burst capacity invariant
            }

            long newSlotValue = (adjustedTimestamp << 32) | (newPackedTokens & 0xFFFFFFFFL);

            // Attempt CAS update on the ring buffer slot and sequence advancement
            if (UNSAFE.compareAndSwapLong(null, getSlotAddress(slotIndex), slotValue, newSlotValue)) {
                // Advance head atomically
                if (UNSAFE.compareAndSwapLong(this, HEAD_OFFSET, currentHead, currentHead + 1)) {
                    return true;
                }
            }

            Thread.onSpinWait();
        }
    }

    public void reconcileVectorClock(long remoteEpoch, long remoteTokens) {
        if (isClosed.get()) {
            return;
        }
        while (true) {
            long currentHead = UNSAFE.getLongVolatile(this, HEAD_OFFSET);
            int slotIndex = (int) (currentHead & mask);
            long slotValue = UNSAFE.getLong(getSlotAddress(slotIndex));
            long localEpoch = slotValue >>> 32;

            if (remoteEpoch >= localEpoch) {
                long reconciledTokens = Math.min(burstCeiling, remoteTokens);
                long newSlotValue = (remoteEpoch << 32) | (reconciledTokens & 0xFFFFFFFFL);

                if (UNSAFE.compareAndSwapLong(null, getSlotAddress(slotIndex), slotValue, newSlotValue)) {
                    return;
                }
            } else {
                return; // Local state is newer; reconciliation drops stale remote state
            }

            Thread.onSpinWait();
        }
    }

    public int getCapacity() {
        return capacity;
    }

    public long getBurstCeiling() {
        return burstCeiling;
    }

    public long getWindowDurationMillis() {
        return windowDurationMillis;
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            // Free the originally allocated memory block (using base memoryAddress offset logic or tracked allocation)
            long baseAllocation = memoryAddress;
            if (baseAllocation != 0) {
                UNSAFE.freeMemory(baseAllocation);
            }
        }
    }
}