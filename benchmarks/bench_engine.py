package com.example.tokenbucket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class TokenBucketMicrobenchmark {

    // Simulated Distributed Token Bucket Engine
    public static class TokenBucketEngine {
        private final long capacity;
        private final long refillRatePerNano;
        private double availableTokens;
        private long lastRefillNano;

        public TokenBucketEngine(long capacity, long refillRatePerSecond) {
            this.capacity = capacity;
            this.refillRatePerNano = refillRatePerSecond;
            this.availableTokens = capacity;
            this.lastRefillNano = System.nanoTime();
        }

        public synchronized boolean tryConsume(long tokens) {
            refill();
            if (availableTokens >= tokens) {
                availableTokens -= tokens;
                return true;
            }
            return false;
        }

        private void refill() {
            long now = System.nanoTime();
            long elapsedNano = now - lastRefillNano;
            if (elapsedNano > 0) {
                double tokensToAdd = (elapsedNano * refillRatePerNano) / 1_000_000_000.0;
                availableTokens = Math.min(capacity, availableTokens + tokensToAdd);
                lastRefillNano = now;
            }
        }
    }

    private static final int WARMUP_ITERATIONS = 500_000;
    private static final int MEASUREMENT_ITERATIONS = 2_000_000;
    private static final int CONCURRENCY = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) throws InterruptedException {
        // High capacity and refill rate to primarily measure processing engine throughput & latency
        TokenBucketEngine engine = new TokenBucketEngine(Long.MAX_VALUE / 2, Long.MAX_VALUE / 4);

        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);

        // --- WARMUP ---
        CountDownLatch warmupLatch = new CountDownLatch(CONCURRENCY);
        int warmupPerThread = WARMUP_ITERATIONS / CONCURRENCY;
        for (int i = 0; i < CONCURRENCY; i++) {
            executor.submit(() -> {
                for (int j = 0; j < warmupPerThread; j++) {
                    engine.tryConsume(1);
                }
                warmupLatch.countDown();
            });
        }
        warmupLatch.await();

        // --- MEASUREMENT ---
        CountDownLatch measureLatch = new CountDownLatch(CONCURRENCY);
        int measurePerThread = MEASUREMENT_ITERATIONS / CONCURRENCY;
        
        // Storage for latencies in nanoseconds
        List<long[]> threadLatencies = new ArrayList<>(CONCURRENCY);
        for (int i = 0; i < CONCURRENCY; i++) {
            threadLatencies.add(new long[measurePerThread]);
        }

        long startTime = System.nanoTime();

        for (int i = 0; i < CONCURRENCY; i++) {
            final int threadIndex = i;
            final long[] latencies = threadLatencies.get(i);
            executor.submit(() -> {
                for (int j = 0; j < measurePerThread; j++) {
                    long startOp = System.nanoTime();
                    engine.tryConsume(1);
                    long endOp = System.nanoTime();
                    latencies[j] = endOp - startOp;
                }
                measureLatch.countDown();
            });
        }

        measureLatch.await();
        long endTime = System.nanoTime();
        executor.shutdown();

        // --- AGGREGATE METRICS ---
        long totalDurationNano = endTime - startTime;
        long totalOps = (long) measurePerThread * CONCURRENCY;
        double opsPerSec = (totalOps * 1_000_000_000.0) / totalDurationNano;

        // Combine all latencies for percentile calculation
        long[] allLatencies = new long[totalOps];
        int destPos = 0;
        for (long[] latencies : threadLatencies) {
            System.arraycopy(latencies, 0, allLatencies, destPos, latencies.length);
            destPos += latencies.length;
        }

        java.util.Arrays.sort(allLatencies);

        double p50 = allLatencies[(int) (allLatencies.length * 0.50)] / 1000.0; // convert to microseconds
        double p90 = allLatencies[(int) (allLatencies.length * 0.90)] / 1000.0;
        double p99 = allLatencies[(int) (allLatencies.length * 0.99)] / 1000.0;
        double p999 = allLatencies[(int) (allLatencies.length * 0.999)] / 1000.0;
        double max = allLatencies[allLatencies.length - 1] / 1000.0;

        // --- OUTPUT FORMATTED TABLE ---
        System.out.println("=========================================================================");
        System.out.println("           Distributed Token Bucket Engine - Microbenchmark             ");
        System.out.println("=========================================================================");
        System.out.printf(" %-25s | %-15s | %-20s %n",psis("Metric"), psis("Value"), psis("Unit"));
        System.out.println("-------------------------------------------------------------------------");
        System.out.printf(" %-25s | %-15d | %-20s %n", "Concurrency Threads", CONCURRENCY, "threads");
        System.out.printf(" %-25s | %-15d | %-20s %n", "Total Operations", totalOps, "ops");
        System.out.printf(" %-25s | %-15.2f | %-20s %n", "Throughput", opsPerSec, "ops/sec");
        System.out.println("-------------------------------------------------------------------------");
        System.out.printf(" %-25s | %-15.3f | %-20s %n", "Latency p50", p50, "µs");
        System.out.printf(" %-25s | %-15.3f | %-20s %n", "Latency p90", p90, "µs");
        System.out.printf(" %-25s | %-15.3f | %-20s %n", "Latency p99", p99, "µs");
        System.out.printf(" %-25s | %-15.3f | %-20s %n", "Latency p99.9", p999, "µs");
        System.out.printf(" %-25s | %-15.3f | %-20s %n", "Latency Max", max, "µs");
        System.out.println("=========================================================================");
    }

    private static String psis(String val) {
        return val;
    }
}