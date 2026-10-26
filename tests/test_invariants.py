package com.engine.ratelimit;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * High-performance, concurrent, and state-accurate token bucket rate limiter.
 * Designed to handle distributed state synchronization assumptions (via versioned state objects),
 * precise time progression, burst capacity, and massive multi-threaded contention.
 */
class TokenBucketEngine {

    public static class Configuration {
        private final long capacity;
        private final long refillRatePerSecond;
        private final String bucketId;

        public Configuration(String bucketId, long capacity, long refillRatePerSecond) {
            if (capacity <= 0) throw new IllegalArgumentException("Capacity must be positive");
            if (refillRatePerSecond <= 0) throw new IllegalArgumentException("Refill rate must be positive");
            if (bucketId == null || bucketId.isEmpty()) throw new IllegalArgumentException("Bucket ID cannot be null or empty");
            this.bucketId = bucketId;
            this.capacity = capacity;
            this.refillRatePerSecond = refillRatePerSecond;
        }

        public long getCapacity() { return capacity; }
        public long getRefillRatePerSecond() { return refillRatePerSecond; }
        public String getBucketId() { return bucketId; }
    }

    public static class BucketState {
        private final long tokens;
        private final long lastRefillTimestampMillis;
        private final long version;

        public BucketState(long tokens, long lastRefillTimestampMillis, long version) {
            this.tokens = tokens;
            this.lastRefillTimestampMillis = lastRefillTimestampMillis;
            this.version = version;
        }

        public long getTokens() { return tokens; }
        public long getLastRefillTimestampMillis() { return lastRefillTimestampMillis; }
        public long getVersion() { return version; }
    }

    public interface StateStore {
        BucketState get(String bucketId);
        boolean compareAndSet(String bucketId, BucketState expected, BucketState update);
    }

    public static class InMemoryStateStore implements StateStore {
        private final ConcurrentHashMap<String, BucketState> store = new ConcurrentHashMap<>();

        @Override
        public BucketState get(String bucketId) {
            return store.get(bucketId);
        }

        @Override
        public boolean compareAndSet(String bucketId, BucketState expected, BucketState update) {
            if (expected == null) {
                BucketState prev = store.putIfAbsent(bucketId, update);
                return prev == null;
            } else {
                return store.replace(bucketId, expected, update);
            }
        }
    }

    private final Configuration config;
    private final StateStore stateStore;
    private final TimeProvider timeProvider;

    public interface TimeProvider {
        long currentTimeMillis();
    }

    public static class SystemTimeProvider implements TimeProvider {
        @Override
        public long currentTimeMillis() {
            return Instant.now().toEpochMilli();
        }
    }

    public TokenBucketEngine(Configuration config, StateStore stateStore, TimeProvider timeProvider) {
        this.config = config;
        this.stateStore = stateStore;
        this.timeProvider = timeProvider;
    }

    /**
     * Attempts to consume the specified number of tokens from the bucket.
     * Retries via optimistic locking (CAS) until success or state validation failure.
     */
    public boolean tryConsume(long requestedTokens) {
        if (requestedTokens <= 0) {
            throw new IllegalArgumentException("Requested tokens must be positive");
        }
        if (requestedTokens > config.getCapacity()) {
            return false; // Cannot satisfy requests larger than maximum capacity
        }

        long maxRetries = 100;
        for (int i = 0; i < maxRetries; i++) {
            long now = timeProvider.currentTimeMillis();
            BucketState current = stateStore.get(config.getBucketId());

            BucketState calculated;
            if (current == null) {
                // Initialize bucket completely full, then deduct requested tokens
                long initialTokens = config.getCapacity() - requestedTokens;
                if (initialTokens < 0) {
                    return false;
                }
                calculated = new BucketState(initialTokens, now, 1L);
                if (stateStore.compareAndSet(config.getBucketId(), null, calculated)) {
                    return true;
                }
            } else {
                // Calculate token refill based on elapsed time
                long elapsedMillis = Math.max(0, now - current.getLastRefillTimestampMillis());
                double tokensToAdd = (elapsedMillis * (double) config.getRefillRatePerSecond()) / 1000.0;
                
                long newTokens = Math.min(config.getCapacity(), (long) (current.getTokens() + tokensToAdd));

                if (newTokens < requestedTokens) {
                    return false; // Insufficient tokens available
                }

                long remainingTokens = newTokens - requestedTokens;
                // If tokens didn't change and time didn't advance meaningfully, we still update timestamp to prevent drift stagnation
                long updatedTimestamp = (tokensToAdd >= 1.0 || elapsedMillis > 10) ? now : current.getLastRefillTimestampMillis();

                BucketState nextState = new BucketState(remainingTokens, updatedTimestamp, current.getVersion() + 1);
                if (stateStore.compareAndSet(config.getBucketId(), current, nextState)) {
                    return true;
                }
            }
            // Yield CPU lightly on CAS failure to reduce contention bus lock saturation
            Thread.onSpinWait();
        }
        throw new ConcurrentModificationException("Exceeded max CAS retries due to high contention on bucket: " + config.getBucketId());
    }

    public BucketState inspect() {
        return stateStore.get(config.getBucketId());
    }
}

/**
 * Comprehensive Test Suite for TokenBucketEngine verifying boundaries, invariants,
 * failure recovery, and extreme concurrent stress scenarios.
 */
class TokenBucketEngineTest {

    private static class MockTimeProvider implements TokenBucketEngine.TimeProvider {
        private final AtomicLong currentTime;

        public MockTimeProvider(long initialTimeMillis) {
            this.currentTime = new AtomicLong(initialTimeMillis);
        }

        @Override
        public long currentTimeMillis() {
            return currentTime.get();
        }

        public void advance(long millis) {
            currentTime.addAndGet(millis);
        }

        public void set(long millis) {
            currentTime.set(millis);
        }
    }

    @Test
    @DisplayName("Boundary: Zero or negative capacity or refill rate should be rejected")
    void testInvalidConfigurations() {
        assertThrows(IllegalArgumentException.class, () -> 
            new TokenBucketEngine.Configuration("b1", 0, 10));
        assertThrows(IllegalArgumentException.class, () -> 
            new TokenBucketEngine.Configuration("b1", -10, 10));
        assertThrows(IllegalArgumentException.class, () -> 
            new TokenBucketEngine.Configuration("b1", 10, 0));
        assertThrows(IllegalArgumentException.class, () -> 
            new TokenBucketEngine.Configuration("b1", 10, -5));
        assertThrows(IllegalArgumentException.class, () -> 
            new TokenBucketEngine.Configuration(null, 10, 10));
        assertThrows(IllegalArgumentException.class, () -> 
            new TokenBucketEngine.Configuration("", 10, 10));
    }

    @Test
    @DisplayName("Boundary: Negative or zero token consumption attempts are prohibited")
    void testInvalidConsumptionRequests() {
        TokenBucketEngine.Configuration config = new TokenBucketEngine.Configuration("b1", 10, 5);
        TokenBucketEngine engine = new TokenBucketEngine(config, new TokenBucketEngine.InMemoryStateStore(), new MockTimeProvider(0));

        assertThrows(IllegalArgumentException.class, () -> engine.tryConsume(0));
        assertThrows(IllegalArgumentException.class, () -> engine.tryConsume(-5));
    }

    @Test
    @DisplayName("Boundary: Requesting tokens exceeding absolute bucket capacity returns false immediately")
    void testRequestExceedsCapacity() {
        TokenBucketEngine.Configuration config = new TokenBucketEngine.Configuration("b1", 10, 5);
        TokenBucketEngine engine = new TokenBucketEngine(config, new TokenBucketEngine.InMemoryStateStore(), new MockTimeProvider(0));

        assertFalse(engine.tryConsume(11));
        assertFalse(engine.tryConsume(1000));
        // Verify state was not initialized incorrectly by failed oversized attempts
        assertNull(engine.inspect());
    }

    @Test
    @DisplayName("Invariant: Initial consumption fully utilizes capacity and subsequent calls fail until refill")
    void testInitialBurstAndDepletion() {
        TokenBucketEngine.Configuration config = new TokenBucketEngine.Configuration("b1", 10, 10);
        MockTimeProvider timeProvider = new MockTimeProvider(1000L);
        TokenBucketEngine engine = new TokenBucketEngine(config, new TokenBucketEngine.InMemoryStateStore(), timeProvider);

        // Consume all 10 tokens at once
        assertTrue(engine.tryConsume(10));
        // Next request should fail because bucket is empty
        assertFalse(engine.tryConsume(1));

        TokenBucketEngine.BucketState state = engine.inspect();
        assertEquals(0, state.getTokens());
        assertEquals(1000L, state.getLastRefillTimestampMillis());
    }

    @Test
    @DisplayName("Invariant: Tokens refill linearly over time up to maximum capacity constraint")
    void testRefillOverTimeAndCapacityCap() {
        // Capacity: 10, Refill rate: 5 tokens/sec (5 tokens per 1000ms -> 1 token per 200ms)
        TokenBucketEngine.Configuration config = new TokenBucketEngine.Configuration("b1", 10, 5);
        MockTimeProvider timeProvider = new MockTimeProvider(0L);
        TokenBucketEngine engine = new TokenBucketEngine(config, new TokenBucketEngine.InMemoryStateStore(), timeProvider);

        // Empty bucket
        assertTrue(engine.tryConsume(10));
        assertEquals(0, engine.inspect().getTokens());

        // Advance 400ms -> should yield 2 tokens (5 * 0.4)
        timeProvider.advance(400);
        assertTrue(engine.tryConsume(2));
        assertFalse(engine.tryConsume(1));

        // Advance 10 seconds (10000ms) -> should refill fully to 10 capacity, ignoring excess potential accumulation
        timeProvider.advance(10000);
        TokenBucketEngine.BucketState stateBeforeFullDrain = engine.inspect();
        // Even with long time passing, tokens should be capped strictly at capacity (10)
        assertTrue(engine.tryConsume(10));
        assertFalse(engine.tryConsume(1));
    }

    @Test
    @DisplayName("Concurrency Stress: Multi-threaded high contention race conditions and strict limit invariants")
    void testConcurrentHighContentionStress() throws InterruptedException {
        long capacity = 1000;
        long refillRate = 500; // 500 tokens per second
        TokenBucketEngine.Configuration config = new TokenBucketEngine.Configuration("stress-bucket", capacity, refillRate);
        TokenBucketEngine.StateStore store = new TokenBucketEngine.InMemoryStateStore();
        MockTimeProvider timeProvider = new MockTimeProvider(System.currentTimeMillis());
        TokenBucketEngine engine = new TokenBucketEngine(config, store, timeProvider);

        int threadCount = 64;
        int operationsPerThread = 200;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);

        AtomicInteger successfulConsumptions = new AtomicInteger(0);
        AtomicInteger failedConsumptions = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        // Request random tokens between 1 and 5
                        long requestSize = (j % 5) + 1;
                        if (engine.