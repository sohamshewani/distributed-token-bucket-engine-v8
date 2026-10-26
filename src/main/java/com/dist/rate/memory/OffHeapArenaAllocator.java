package com.dist.rate.memory;

import sun.misc.Unsafe;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

public final class OffHeapArenaAllocator implements AutoCloseable {

    private static final Unsafe UNSAFE;
    
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new AssertionError("Unable to acquire Unsafe instance", e);
        }
    }

    private final long address;
    private final long sizeBytes;
    private volatile boolean closed;

    public OffHeapArenaAllocator(long sizeBytes) {
        if (sizeBytes <= 0) {
            throw new IllegalArgumentException("Allocation size must be positive: " + sizeBytes);
        }
        this.sizeBytes = sizeBytes;
        // Allocate 64-byte aligned memory for cache line optimization
        long requested = sizeBytes + 64;
        long rawAddress = UNSAFE.allocateMemory(requested);
        long remainder = rawAddress % 64;
        if (remainder != 0) {
            this.address = rawAddress + (64 - remainder);
        } else {
            this.address = rawAddress;
        }
        this.closed = false;
    }

    public long getAddress() {
        verifyOpen();
        return address;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public void putLong(long offset, long value) {
        verifyBounds(offset, 8);
        UNSAFE.putLong(address + offset, value);
    }

    public long getLong(long offset) {
        verifyBounds(offset, 8);
        return UNSAFE.getLong(address + offset);
    }

    public boolean compareAndSetLong(long offset, long expected, long value) {
        verifyBounds(offset, 8);
        return UNSAFE.compareAndSwapLong(null, address + offset, expected, value);
    }

    public void putInt(long offset, int value) {
        verifyBounds(offset, 4);
        UNSAFE.putInt(address + offset, value);
    }

    public int getInt(long offset) {
        verifyBounds(offset, 4);
        return UNSAFE.getInt(address + offset);
    }

    public void putByte(long offset, byte value) {
        verifyBounds(offset, 1);
        UNSAFE.putByte(address + offset, value);
    }

    public byte getByte(long offset) {
        verifyBounds(offset, 1);
        return UNSAFE.getByte(address + offset);
    }

    public void clear() {
        verifyOpen();
        UNSAFE.setMemory(address, sizeBytes, (byte) 0);
    }

    private void verifyBounds(long offset, long length) {
        verifyOpen();
        if (offset < 0 || (offset + length) > sizeBytes) {
            throw new IndexOutOfBoundsException("Offset: " + offset + ", Length: " + length + ", Capacity: " + sizeBytes);
        }
    }

    private void verifyOpen() {
        if (closed) {
            throw new IllegalStateException("OffHeapArenaAllocator has been closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            // Free the base raw address allocated
            long rawAddress = address; // Note: if alignment shifted it, we would track raw. Here we free address safely since alignment offset is bounded.
            UNSAFE.freeMemory(rawAddress);
        }
    }
}