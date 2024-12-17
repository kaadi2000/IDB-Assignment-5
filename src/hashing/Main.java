package hashing;

import xxl.core.collections.containers.io.BlockFileContainer;
import xxl.core.io.Buffer;
import xxl.core.io.LRUBuffer;
import xxl.core.io.converters.LongConverter;

public class Main {
    public static void main(String[] args) {
        int numInserts = 1000;

        BlockFileContainer primary = new BlockFileContainer("main", ExternalLinearHashMap.BLOCK_SIZE);
        BlockFileContainer secondary = new BlockFileContainer("overflow", ExternalLinearHashMap.BLOCK_SIZE);
        Buffer<Object, Integer, HashBlock<Long, Long>> buffer = new LRUBuffer<>(512);

        primary.clear();
        secondary.clear();

        ExternalLinearHashMap<Long, Long> map = new ExternalLinearHashMap<Long, Long>(LongConverter.DEFAULT_INSTANCE, LongConverter.DEFAULT_INSTANCE, primary, secondary, buffer);
        for (long i = 0; i < numInserts; i++) {
            map.insert(i, i);

            for (long j = 0; j <= i; j++) {
                if (!map.contains(j)) {
                    throw new RuntimeException("Element not found: " + j);
                }
            }

            if (map.getSize() != i + 1) {
                throw new RuntimeException("wrong size");
            }
        }

        for (long i = 0; i < numInserts; i++) {
            if (!map.contains(i)) {
                throw new RuntimeException("Element not found: " + i);
            }
        }

        map.close();

        // Map should be persisted and work after re-creating
        map = new ExternalLinearHashMap<Long, Long>(LongConverter.DEFAULT_INSTANCE, LongConverter.DEFAULT_INSTANCE, primary, secondary, buffer);

        for (long i = 0; i < numInserts; i++) {
            if (!map.contains(i)) {
                throw new RuntimeException("Element not found: " + i);
            }
        }
    }
}