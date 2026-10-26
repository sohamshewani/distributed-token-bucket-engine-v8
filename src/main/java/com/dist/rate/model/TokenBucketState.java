package com.dist.rate.model;

import sun.misc.Unsafe;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

public final class TokenBucketState {

    private static final Unsafe UNSAFE;
    private static final long STATE_OFFSET;
    private static final long CAPACITY_OFFSET;
    private static final long REFILL_RATE_OFFSET;
    private static final long LAST_UPDATE_TIME_OFFSET;
    private static final long EPOCH_OFFSET;

    private static final VarHandle STATE_HANDLE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);

            STATE_OFFSET = UNSAFE.objectFieldOffset(TokenBucketState.class.getDeclaredField("packedState"));
            CAPACITY_OFFSET = UNSAFE.objectFieldOffset(TokenBucketState.class.getDeclaredField("capacity"));
            REFILL_RATE_OFFSET = UNSAFE.objectFieldOffset(TokenBucketState.class.getDeclaredField("refillRate"));
            LAST_UPDATE_TIME_OFFSET = UNSAFE.objectFieldOffset(TokenBucketState.class.getDeclaredField("lastUpdateTime"));
            EPOCH_OFFSET = UNSAFE.objectFieldOffset(TokenBucketState.class.getDeclaredField("epoch"));

            STATE_HANDLE = MethodHandles.lookup().findVarHandle(TokenBucketState.class, "packedState", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // Cache-line padding to prevent false sharing (64-byte alignment zones)
    private long p0, p1, p2, p3, p4, p5, p6;

    // 64-bit packed state: [32 bits: tokens | 32 bits: sequence/flags]
    private volatile long packedState;

    private long p8, p9, p10, p11, p12, p13, p14;

    private final long capacity;
    private final long refillRate; // tokens per nanosecond scaled, or fixed point
    private volatile long lastUpdateTime;
    private volatile long epoch;

    private long p16, p17, p18, p19, p20, p21, p22;

    public TokenBucketState(long capacity, long refillRate, long initialTokens, long initialTimestamp) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        if (refillRate < 0) {
            throw new IllegalArgumentException("Refill rate cannot be negative");
        }
        if (initialTokens < 0 || initialTokens > capacity) {
            throw new IllegalArgumentException("Initial tokens must be within [0, capacity]");
        }

        this.capacity = capacity;
        this.refillRate = refillRate;
        this.lastUpdateTime = initialTimestamp;
        this.epoch = 0L;
        this.packedState = pack(initialTokens, 0L);
    }

    public static long pack(long tokens, long sequence) {
        return (tokens << 32) | (sequence & 0xFFFFFFFFL);
    }

    public static long unpackTokens(long packed) {
        return packed >>> 32;
    }

    public static long unpackSequence(long packed) {
        return packed & 0xFFFFFFFFL;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getRefillRate() {
        return refillRate;
    }

    public long getLastUpdateTime() {
        return UNSAFE.getLongVolatile(this, LAST_UPDATE_TIME_OFFSET);
    }

    public long getEpoch() {
        return UNSAFE.getLongVolatile(this, EPOCH_OFFSET);
    }

    public long getPackedState() {
        return (long) STATE_HANDLE.getVolatile(this);
    }

    public boolean CAS(long expectedPacked, long updatePacked) {
        return STATE_HANDLE.compareAndSet(this, expectedPacked, updatePacked);
    }

    public boolean tryConsume(long requestedTokens, long currentTimeNanos, long maxStepBackEpsilon) {
        if (requestedTokens <= 0) {
            return true;
        }
        if (requestedTokens > capacity) {
            return false;
        }

        int spins = 0;
        while (true) {
            long currentPacked = getPackedState();
            long currentTokens = unpackTokens(currentPacked);
            long currentSeq = unpackSequence(currentPacked);
            long currentLastUpdate = UNSAFE.getLongVolatile(this, LAST_UPDATE_TIME_OFFSET);

            long timeDelta = currentTimeNanos - currentLastUpdate;

            // Time monotonicity enforcement & NTP clock regression absorption
            if (timeDelta < 0) {
                if (-timeDelta <= maxStepBackEpsilon) {
                    timeDelta = 0; // Absorb deterministic sliding epsilon regression
                } else {
                    // Significant clock skew detected, force sync to current time with 0 delta
                    timeDelta = 0;
                }
            }

            long calculatedTokens = currentTokens;
            if (timeDelta > 0 && refillRate > 0) {
                long generated = (long) ((double) timeDelta * ((double) refillRate / 1_000_000_000.0));
                calculatedTokens = Math.min(capacity, currentTokens + generated);
            }

            if (calculatedTokens < requestedTokens) {
                return false;
            }

            long nextTokens = calculatedTokens - requestedTokens;
            long nextSeq = currentSeq + 1;
            long nextPacked = pack(nextTokens, nextSeq);

            // Try optimistic CAS update. If time advanced, update lastUpdateTime concurrently.
            if (CAS(currentPacked, nextPacked)) {
                if (timeDelta > 0) {
                    // Update timestamp using atomic/volatile store to prevent stale reads
                    UNSAFE.putLongVolatile(this, LAST_UPDATE_TIME_OFFSET, currentTimeNanos);
                }
                return true;
            }

            spins++;
            if (spins > 64) {
                Thread.onSpinWait();
                if (spins > 1024) {
                    Thread.yield();
                }
            }
        }
    }

    public void refillToFull(long currentTimeNanos) {
        int spins = 0;
        while (true) {
            long currentPacked = getPackedState();
            long currentSeq = unpackSequence(currentPacked);
            long nextPacked = pack(capacity, currentSeq + 1);

            if (CAS(currentPacked, nextPacked)) {
                UNSAFE.putLongVolatile(this, LAST_UPDATE_TIME_OFFSET, currentTimeNanos);
                return;
            }

            spins++;
            if (spins > 64) {
                Thread.onSpinWait();
            }
        }
    }

    public void serializeToOffHeap(long address, long byteOffset) {
        long targetAddress = address + byteOffset;
        long currentPacked = getPackedState();
        long currentLastUpdate = getLastUpdateTime();
        long currentEpoch = getEpoch();

        UNSAFE.putLong(targetAddress, capacity);
        UNSAFE.putLong(targetAddress + 8, refillRate);
        UNSAFE.putLong(targetAddress + 16, currentPacked);
        UNSAFE.putLong(targetAddress + 24, currentLastUpdate);
        UNSAFE.putLong(targetAddress + 32, currentEpoch);
        UNSAFE.storeFence();
    }

    public static TokenBucketState deserializeFromOffHeap(long address, long byteOffset) {
        long targetAddress = address + byteOffset;
        long cap = UNSAFE.getLong(targetAddress);
        long rate = UNSAFE.getLong(targetAddress + 8);
        long packed = UNSAFE.getLong(targetAddress + 16);
        long lastUpdate = UNSAFE.getLong(targetAddress + 24);
        long ep = UNSAFE.getLong(targetAddress + 32);

        long tokens = unpackTokens(packed);
        TokenBucketState state = new TokenBucketState(cap, rate, tokens, lastUpdate);
        state.epoch = ep;
        state.packedState = packed;
        return state;
    }

    public static int getSerializedByteSize() {
        return 40; // 5 fields * 8 bytes
    }
}