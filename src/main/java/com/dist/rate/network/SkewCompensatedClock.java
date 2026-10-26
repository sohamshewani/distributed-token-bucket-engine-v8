package com.dist.rate.network;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public final class SkewCompensatedClock {

    private static final Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new AssertionError("Failed to initialize Unsafe", e);
        }
    }

    private static final long MAX_STEP_BACK_MICROS_DEFAULT = 10_000L; // 10 milliseconds epsilon

    // Cache-line padding offset fields for state variables
    private static final long PADDED_STATE_OFFSET;
    private static final int CACHE_LINE_SIZE = 64;

    private final long maxStepBackMicros;
    private final long memoryAddress;
    private final long allocationSize;

    // Layout inside off-heap block:
    // [0..7]: physical last observed time (micros)
    // [8..15]: logical skew-compensated time (micros)
    // [16..23]: accumulated offset/correction (micros)
    private static final long PHYSICAL_TIME_OFFSET = 0L;
    private static final long LOGICAL_TIME_OFFSET = 8L;
    private static final long CORRECTION_OFFSET = 16L;

    static {
        try {
            PADDED_STATE_OFFSET = CACHE_LINE_SIZE;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public SkewCompensatedClock() {
        this(MAX_STEP_BACK_MICROS_DEFAULT);
    }

    public SkewCompensatedClock(long maxStepBackMicros) {
        if (maxStepBackMicros < 0) {
            throw new IllegalArgumentException("maxStepBackMicros must be non-negative");
        }
        this.maxStepBackMicros = maxStepBackMicros;
        this.allocationSize = CACHE_LINE_SIZE * 4; // Ensure ample padding space
        this.memoryAddress = UNSAFE.allocateMemory(this.allocationSize);
        
        long initialTime = System.currentTimeMillis() * 1000L;
        UNSAFE.putLong(memoryAddress + PHYSICAL_TIME_OFFSET, initialTime);
        UNSAFE.putLong(memoryAddress + LOGICAL_TIME_OFFSET, initialTime);
        UNSAFE.putLong(memoryAddress + CORRECTION_OFFSET, 0L);
    }

    public long currentTimeMicros() {
        long currentPhysical = System.currentTimeMillis() * 1000L + (System.nanoTime() / 1000L) % 1000L;
        
        while (true) {
            long lastPhysical = UNSAFE.getLongVolatile(null, memoryAddress + PHYSICAL_TIME_OFFSET);
            long lastLogical = UNSAFE.getLong(memoryAddress + LOGICAL_TIME_OFFSET);
            long currentCorrection = UNSAFE.getLong(memoryAddress + CORRECTION_OFFSET);

            if (currentPhysical >= lastPhysical) {
                // Time is advancing or staying equal normally
                long delta = currentPhysical - lastPhysical;
                long newLogical = lastLogical + delta;
                
                if (UNSAFE.compareAndSwapLong(null, memoryAddress + PHYSICAL_TIME_OFFSET, lastPhysical, currentPhysical)) {
                    // Update logical time monotonically with the physical delta
                    updateLogicalTime(newLogical);
                    return newLogical;
                }
            } else {
                // Time regression detected (NTP adjustment backward)
                long stepBack = lastPhysical - currentPhysical;
                if (stepBack <= maxStepBackMicros) {
                    // Absorb regression deterministically: clamp logical time progression, enforce monotonicity
                    if (UNSAFE.compareAndSwapLong(null, memoryAddress + PHYSICAL_TIME_OFFSET, lastPhysical, currentPhysical)) {
                        // We do not decrease logical time; we hold it or advance it at a bounded rate
                        long newCorrection = currentCorrection - stepBack;
                        UNSAFE.putLong(memoryAddress + CORRECTION_OFFSET, newCorrection);
                        return lastLogical;
                    }
                } else {
                    // Regression exceeds epsilon threshold; perform hard synchronization step
                    if (UNSAFE.compareAndSwapLong(null, memoryAddress + PHYSICAL_TIME_OFFSET, lastPhysical, currentPhysical)) {
                        long newLogical = lastLogical - stepBack;
                        updateLogicalTime(newLogical);
                        return newLogical;
                    }
                }
            }
            Thread.onSpinWait();
        }
    }

    private void updateLogicalTime(long newLogical) {
        while (true) {
            long currentLogical = UNSAFE.getLongVolatile(null, memoryAddress + LOGICAL_TIME_OFFSET);
            if (newLogical <= currentLogical) {
                // Enforce strict monotonicity
                break;
            }
            if (UNSAFE.compareAndSwapLong(null, memoryAddress + LOGICAL_TIME_OFFSET, currentLogical, newLogical)) {
                break;
            }
            Thread.onSpinWait();
        }
    }

    public long getCorrectionOffset() {
        return UNSAFE.getLongVolatile(null, memoryAddress + CORRECTION_OFFSET);
    }

    public void release() {
        if (memoryAddress != 0L) {
            UNSAFE.freeMemory(memoryAddress);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            release();
        } finally {
            super.finalize();
        }
    }
}