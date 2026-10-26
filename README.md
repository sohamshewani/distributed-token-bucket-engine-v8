# distributed-token-bucket-engine
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/distributed-token-bucket-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/distributed-token-bucket-engine/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/distributed-token-bucket-engine) [![Stars](https://img.shields.io/github/stars/sohamshewani/distributed-token-bucket-engine?style=flat)](https://github.com/sohamshewani/distributed-token-bucket-engine)

## Executive Summary & Problem Space

`distributed-token-bucket-engine` is an ultra-low-latency, high-throughput distributed rate-limiting engine engineered for mission-critical infrastructure handling millions of operations per second. Traditional rate-limiting architectures suffer from severe bottlenecks under heavy concurrency: distributed coordination via consensus protocols (e.g., Raft, Paxos) introduces unacceptable network round-trip latencies, while centralized JVM-based datastores (e.g., Redis via Redisson) crash against JVM Garbage Collection (GC) pauses, heap allocations, and lock-contention ceilings (`synchronized` or standard `ReentrantLock` blocks).

This engine resolves these failure modes by decoupling rate-state management from the JVM heap entirely. By implementing zero-allocation off-heap memory management via direct `sun.misc.Unsafe` / `VarHandle` arenas, lock-free Compare-And-Swap (CAS) atomic primitive pipelines, hardware-aware lock-free ring buffers, and skew-compensated clock synchronization, `distributed-token-bucket-engine` achieves predictable sub-microsecond evaluation latencies with deterministic memory footprints immune to GC pressure.

---

## Structural ASCII Architecture / Dataflow Diagram

```
 +-----------------------------------------------------------------------------------+
 |                             CLIENT INVOCATION LAYER                               |
 |   (High-Throughput API / Network Ingress / Netty EventLoop Threads)               |
 +-----------------------------------------------------------------------------------+
                                          |
                                          v
 +-----------------------------------------------------------------------------------+
 |                        SkewCompensatedClock (Network Layer)                       |
 |   - NTP/PTP Monotonic Drift Correction & Hardware Timestamp Extraction              |
 +-----------------------------------------------------------------------------------+
                                          |
                                          v
 +-----------------------------------------------------------------------------------+
 |                       DistributedRateLimiter (Engine Core)                        |
 |   - Orchestrates Bucket Lookup, Token Replenishment, and Consumption Validation   |
 +-----------------------------------------------------------------------------------+
         |                                                                  |
         v                                                                  v
 +-------------------------------+                         +---------------------------------+
 |   CASAtomicEngine             |                         |      RingBufferWindow           |
 |   - Lock-Free State Updates   |                         |   - Zero-Allocation Sliding     |
 |   - VarHandle CAS Pipelines   |                         |     Window Sliding Aggregates   |
 +-------------------------------+                         +---------------------------------+
         |                                                                  |
         +---------------------------------+--------------------------------+
                                           |
                                           v
 +-----------------------------------------------------------------------------------+
 |                       OffHeapArenaAllocator (Memory Layer)                        |
 |   - Direct Memory Layout via sun.misc.Unsafe / VarHandle                          |
 |   - TokenBucketState Structs Packets (Zero-GC Heap Allocation)                    |
 +-----------------------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

The execution model relies strictly on lock-free concurrency and explicit memory ownership paradigms to guarantee thread safety and eliminate race conditions.

### Thread Safety & Lock Contention Prevention
- **Lock-Free Mutex Avoidance:** Zero `synchronized`, `ReentrantLock`, or monitor acquisitions exist on the hot evaluation path. All concurrent bucket mutations are orchestrated through lock-free Compare-And-Swap (`CAS`) primitives driven by `java.lang.invoke.VarHandle`.
- **ABA Problem Mitigations:** State transitions operate on packed 64-bit/128-bit primitives inside `TokenBucketState.java`, coupling a monotonic sequence counter with token counts to systematically eliminate ABA vulnerabilities during high-frequency concurrent refills.

### Memory Ownership & Off-Heap Mechanics
- **OffHeapArenaAllocator (`src/main/java/com/dist/rate/memory/OffHeapArenaAllocator.java`):** Allocations bypass the Java heap entirely by interacting directly with native memory blocks managed through arena chunks. This completely bypasses generational garbage collectors, eliminating Stop-The-World (STW) pauses.
- **Strict Lifetime Ownership:** Memory blocks adhere to explicit lifecycle boundaries. Once an arena is destroyed, dangling pointers are trapped through boundary-check guards built directly into memory read/write offset calculations.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Time Complexity | Space Complexity | P99 Latency (Est.) | Bottleneck Profile |
| :--- | :--- | :--- | :--- | :--- |
| **Token Consumption (Read/Write)** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ off-heap | $< 250\text{ns}$ | CPU Cache Line Coherency (MESI protocol) |
| **Ring Buffer Window Sliding** | $\mathcal{O}(1)$ amortized | $\mathcal{O}(W)$ fixed-size | $< 100\text{ns}$ | L1/L2 Cache Bandwidth |
| **Off-Heap Allocation / Deallocation** | $\mathcal{O}(1)$ arena block | $\mathcal{O}(N)$ bounded | $< 500\text{ns}$ | Native OS `mmap` / Arena Chunk Slicing |
| **Clock Skew Compensation** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | $< 50\text{ns}$ | CPU `RDTSC` Instruction Latency |

---

## Security Architecture & Threat Model

`distributed-token-bucket-engine` is engineered with an aggressive hardening posture to defend against malformed inputs, memory corruption attacks, and denial-of-service vectors.

### Input Boundaries & Sanitization
- **Strict Parameter Bounds:** All bucket capacities, refill rates, and requested token quantities undergo explicit boundary enforcement before reaching the execution pipeline. Negative values, overflow-inducing tokens, and null pointer inputs throw immediate, unrecoverable `IllegalArgumentException` or `NullPointerException` guards.

### Memory Corruption & Safety Protections
- **Bounds-Checked Unsafe Access:** Direct memory interactions within `OffHeapArenaAllocator` implement rigorous offset and size boundary verifications to prevent buffer over-reads and out-of-bounds writes that could lead to JVM segmentation faults or native memory corruption.
- **Immutable State Projections:** Public API surfaces expose defensive copies or read-only value types (`TokenBucketState`), preventing external actors from mutating internal off-heap bit-patterns or bypassing rate-limit policies.

---

## Build, Verification & Benchmarking Recipes

Ensure your environment satisfies the prerequisites before executing the lifecycle recipes:
- **Java Development Kit (JDK):** OpenJDK 17 or higher (configured with `--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED` if required by strict module policies).
- **Build System:** Apache Maven 3.8+.
- **Python Runtime:** Python 3.9+ (required for invariant testing and benchmarking suites).

### 1. Compile and Package the Engine
```bash
mvn clean package -DskipTests
```

### 2. Run Comprehensive Test Suites (Java Unit Tests & Python Invariant Verification)
Execute the Maven test suite to validate core concurrency invariants:
```bash
mvn test
```

Execute the Python-based invariant and integration harness (`tests/test_invariants.py`):
```bash
python3 -m unittest discover -s tests
```

### 3. Execute Performance & Latency Benchmarks
Run the high-concurrency micro-benchmark suite (`benchmarks/bench_engine.py`) to measure throughput and latency profiles:
```bash
python3 benchmarks/bench_engine.py --threads=16 --duration=30 --target-ops=1000000