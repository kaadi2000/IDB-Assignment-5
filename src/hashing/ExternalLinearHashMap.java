package hashing;

import xxl.core.collections.MapEntry;
import xxl.core.collections.containers.Container;
import xxl.core.collections.containers.io.BufferedContainer;
import xxl.core.collections.containers.io.ConverterContainer;
import xxl.core.functions.Constant;
import xxl.core.io.Block;
import xxl.core.io.Buffer;
import xxl.core.io.converters.FixedSizeConverter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ExternalLinearHashMap<K, V> implements Iterable<MapEntry<K, V>> {
    /**
     * Number of initial blocks
     */
    static final int INITIAL_CAPACITY = 4;

    /**
     * Threshold for expansion steps
     */
    static final float MAX_LOAD_FACTOR = 0.80f;

    /**
     * Block size in bytes.
     */
    static final int BLOCK_SIZE = 64;

    /**
     * Size of the header in each block in bytes.
     */
    static final int BLOCK_HEADER_SIZE = 16;

    /**
     * Size of the metadata in the first block of the secondary file, in bytes.
     */
    static final int METADATA_SIZE = 32;

    /**
     * Number of elements stored in each block
     */
    private final int elementsPerBlock;

    private final ArrayList<HashBucket> buckets;
    /**
     * Number of buckets in use (== (DEFAULT_INITIAL_CAPACITY << level) + expansionPointer)
     */
    private int numBuckets;

    /**
     * Current expansion pointer position
     */
    private int expansionPointer;

    /**
     * Number of completed total expansions
     */
    private int level;

    /**
     * Number of elements in this hashmap.
     */
    private int size;

    private boolean isOpen;
    private final Container primary;
    private final Container secondary;
    private final Container rawSecondary;

    /**
     * Represents the probe result of a Key in a bucket. If the element is found, the key-value pair is set in the entry field.
     * The remaining fields are necessary to perform updates or inserts on the probed bucket.
     */
    private class ProbeResult {
        /**
         * K-V pair, if found.
         */
        final MapEntry<K, V> entry;

        /**
         * Block of the K-V pair, if found, otherwise the last block in the overflow list.
         */
        final HashBlock<K, V> block;

        /**
         * Container holding the block, i.e. primary, if it is in the main array, secondary if it is in the overflow list.
         */
        final Container container;

        /**
         * id of the block in the given container.
         */
        final long blockId;

        public ProbeResult(MapEntry<K, V> entry, HashBlock<K, V> block, Container container, long id) {
            this.entry = entry;
            this.block = block;
            this.container = container;
            this.blockId = id;
        }
    }

    /**
     * Represents a bucket of the hash map. The first block of each of the buckets lie consecutively in the primary file,
     * while overflow buckets lie in the secondary file. Blocks are linked via the overflow block identifier.
     */
    private class HashBucket implements Iterable<MapEntry<K, V>> {
        /**
         * id of the first block in the primary file
         */
        final long id;

        /**
         * Create a new bucket, reserving a new block in the primary file if necessary.
         * Otherwise, the existing block in the primary container is used.
         */
        HashBucket(int hashIndex) {
            this.id = (long) hashIndex * ExternalLinearHashMap.BLOCK_SIZE;

            if (!primary.contains(this.id)) {
                long id = (long) primary.reserve(new Constant(null));
                if (id != this.id)
                    throw new RuntimeException("Unexpected id: " + id);
                primary.update(this.id, new HashBlock<K, V>());
            }
        }

        /**
         * Create a new bucket in the primary file and fill it with the given elements.
         */
        HashBucket(int index, List<MapEntry<K, V>> elements) {
            this(index);
            this.setElements(elements);
        }

        /**
         * Probe the bucket for the given key by iterating over the primary block and all blocks in the overflow list.
         * <p>
         * Returns a ProbeResult that, if the key is found, contains the entry, the block containing the entry,
         * the container containing the block and the id of the block inside the container.
         * <p>
         * If the element is not found, the entry is set to null, and block/container/id refer to the last
         * block in the overflow list of this bucket.
         */
        ProbeResult probe(K key) {
            // TODO
            return new ProbeResult(null, null, null, 0);
        }

        /**
         * Insert a key-value pair into the bucket; returns the previous value if it already existed.
         */
        V insert(K key, V value) {
            ProbeResult res = probe(key);
            // TODO
            return null;
        }

        boolean contains(K key) {
            ProbeResult res = probe(key);
            return res.entry != null;
        }

        V get(K key) {
            ProbeResult res = probe(key);
            if (res.entry == null)
                return null;
            else
                return res.entry.getValue();
        }

        /**
         * Sets the elements of this bucket. Reuses existing overflow buckets and frees unused ones.
         */
        void setElements(List<MapEntry<K, V>> elems) {
            HashBlock<K, V> cur = getPrimaryBlock(id);
            cur.clearElements();

            boolean isPrimary = true;
            long id = this.id;
            for (MapEntry<K, V> e : elems) {
                if (cur.getSize() == elementsPerBlock) {
                    // Append an overflow block if it does not exist
                    if (!cur.hasOverflow())
                        cur.setOverflowId(newOverflowId());

                    if (isPrimary) {
                        primary.update(id, cur);
                        isPrimary = false;
                    } else {
                        secondary.update(id, cur);
                    }

                    id = cur.getOverflowId();
                    cur = getOverflowBlock(id);
                    cur.clearElements();
                }
                cur.add(e);
            }

            // Free any extra overflow pages
            if (cur.hasOverflow()) {
                freeOverflowList(cur.getOverflowId());
                cur.unsetOverflow();
            }

            if (isPrimary)
                primary.update(id, cur);
            else
                secondary.update(id, cur);
        }

        public Iterator<MapEntry<K, V>> iterator() {
            return new Iterator<>() {
                HashBlock<K, V> cur = getPrimaryBlock(id);
                Iterator<MapEntry<K, V>> it = cur.iterator();

                @Override
                public boolean hasNext() {
                    return it.hasNext() || cur.hasOverflow();
                }

                @Override
                public MapEntry<K, V> next() {
                    if (!it.hasNext()) {
                        cur = getOverflowBlock(cur.getOverflowId());
                        it = cur.iterator();
                    }
                    return it.next();
                }
            };
        }
    }

    public ExternalLinearHashMap(FixedSizeConverter<K> keyConverter, FixedSizeConverter<V> valueConverter,
                                 Container primary1, Container secondary1, Buffer<Object, Integer, HashBlock<K, V>> buffer) {

        FixedSizeConverter<HashBlock<K, V>> converter = new FixedSizeConverter<>(
                keyConverter.getSerializedSize() + valueConverter.getSerializedSize()
        ) {
            @Override
            public HashBlock<K, V> read(DataInput dataInput, HashBlock<K, V> block) throws IOException {
                long numElems = dataInput.readLong();
                long overflowPointer = dataInput.readLong();
                ArrayList<MapEntry<K, V>> list = new ArrayList<>();
                for (int i = 0; i < numElems; i++)
                    list.add(new MapEntry<>(keyConverter.read(dataInput), valueConverter.read(dataInput)));
                return new HashBlock<>(list, overflowPointer);
            }

            @Override
            public void write(DataOutput dataOutput, HashBlock<K, V> block) throws IOException {
                dataOutput.writeLong(block.getSize());
                dataOutput.writeLong(block.getOverflowId());
                for (MapEntry<K, V> elem : block) {
                    keyConverter.write(dataOutput, elem.getKey());
                    valueConverter.write(dataOutput, elem.getValue());
                }
            }
        };

        this.isOpen = true;
        this.buckets = new ArrayList<>();
        this.primary = new BufferedContainer(new ConverterContainer(primary1, converter), buffer);
        this.secondary = new BufferedContainer(new ConverterContainer(secondary1, converter), buffer);
        this.rawSecondary = secondary1;

        this.elementsPerBlock = (BLOCK_SIZE - BLOCK_HEADER_SIZE) / converter.getSerializedSize();
        if (elementsPerBlock == 0 || BLOCK_SIZE < METADATA_SIZE)
            throw new RuntimeException("Increase block size");

        if (primary.size() == 0) {
            // New container; initialize with defaults

            this.numBuckets = INITIAL_CAPACITY;
            this.expansionPointer = 0;
            this.level = 0;

            for (int i = 0; i < INITIAL_CAPACITY; i++)
                this.buckets.add(new HashBucket(i));

            // Reserve the first block in the secondary file for metadata
            this.rawSecondary.reserve(new Constant(null));
            this.rawSecondary.update(0, new Block(BLOCK_SIZE));
        } else {
            // Existing container; initialize from existing metadata
            readMetadata();
            for (int i = 0; i < numBuckets; i++)
                this.buckets.add(new HashBucket(i));
        }
    }

    /**
     * Write metadata to the secondary file.
     */
    private void writeMetadata() {
        Block o = (Block) rawSecondary.get(0);
        o.writeLong(0, size);
        o.writeLong(8, numBuckets);
        o.writeLong(16, expansionPointer);
        o.writeLong(24, level);
        rawSecondary.update(0, o);
    }

    /**
     * Read metadata from the secondary file.
     */
    private void readMetadata() {
        Block o = (Block) rawSecondary.get(0);
        this.size = (int) o.readLong(0);
        this.numBuckets = (int) o.readLong(8);
        this.expansionPointer = (int) o.readLong(16);
        this.level = (int) o.readLong(24);
    }

    /**
     * Close underlying containers and write metadata to disk. The map cannot be used after calling this method.
     */
    public void close() {
        if (isOpen) {
            writeMetadata();
            secondary.close();
            primary.close();
            isOpen = false;
        }
    }

    // Convenience methods to get blocks from the containers:

    /**
     * Request a new block id in the overflow container
     */
    private long newOverflowId() {
        long id = (long) secondary.reserve(new Constant(null));
        secondary.update(id, new HashBlock<K, V>());
        return id;
    }

    /**
     * Load a block from the primary file
     */
    private HashBlock<K, V> getPrimaryBlock(long id) {
        return (HashBlock<K, V>) primary.get(id);
    }

    /**
     * Load a block from the secondary (overflow) file
     */
    private HashBlock<K, V> getOverflowBlock(long id) {
        return (HashBlock<K, V>) secondary.get(id);
    }

    /**
     * Free a list of overflow blocks.
     */
    private void freeOverflowList(long beginId) {
        HashBlock<K, V> block = getOverflowBlock(beginId);
        secondary.remove(beginId);
        if (block.hasOverflow())
            freeOverflowList(block.getOverflowId());
    }

    /**
     * Get the number of elements contained in this map
     */
    public long getSize() {
        return size;
    }

    /**
     * Check if the size is over a threshold and a split should be performed
     */
    private boolean isOverflow() {
        return getLoadFactor() >= MAX_LOAD_FACTOR;
    }

    /**
     * Get the load factor of this hashmap
     */
    public double getLoadFactor() {
        return 1.0 * getSize() / (buckets.size() * elementsPerBlock);
    }

    /**
     * Expand the bucket currently pointed at by the expansion pointer.
     */
    private void performExpansion() {
        // TODO
    }

    /**
     * Calculates hash function for the given level.
     */
    private int hashIndex(K key, int level) {
        int hash = key.hashCode() % (INITIAL_CAPACITY << level);
        if (hash < 0)
            hash += (INITIAL_CAPACITY << level);
        return hash;
    }

    /**
     * Generates the actual bucket index for the given key.
     */
    private int realHashIndex(K key) {
        // TODO
        return 0;
    }

    /**
     * Inserts a key-value tuple into the map; returns the previous value if the key was already contained.
     * Performs an expansion if the current load factor is too high (i.e. isOverflow() returns true).
     */
    public V insert(K key, V value) {
        if (!isOpen)
            throw new IllegalStateException("Hashmap is closed.");

        int ind = realHashIndex(key);
        HashBucket bucket = buckets.get(ind);
        V res = bucket.insert(key, value);
        if (res == null)
            size++;
        if (isOverflow())
            performExpansion();
        return res;
    }

    /**
     * Get the value of a key-value pair from this map; null if it wasn't contained.
     */
    public V get(K key) {
        if (!isOpen)
            throw new IllegalStateException("Hashmap is closed.");

        int ind = realHashIndex(key);
        HashBucket bucket = buckets.get(ind);
        return bucket.get(key);
    }

    /**
     * Check if a key is contained in the hashmap.
     */
    public boolean contains(K key) {
        if (!isOpen)
            throw new IllegalStateException("Hashmap is closed.");

        int ind = realHashIndex(key);
        HashBucket bucket = buckets.get(ind);
        return bucket.contains(key);
    }

    /**
     * Print contents of the map to stdout.
     */
    public void display() {
        for (HashBucket bucket : buckets) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (MapEntry<K, V> entry : bucket) {
                if (sb.length() > 1)
                    sb.append(", ");
                sb.append("(")
                        .append(entry.getKey().toString())
                        .append(", ")
                        .append(entry.getValue().toString())
                        .append(")");
            }
            sb.append("]");
            System.out.println(sb);
        }
        System.out.println();
    }

    @Override
    public Iterator<MapEntry<K, V>> iterator() {
        if (!isOpen)
            throw new IllegalStateException("Hashmap is closed.");

        return new Iterator<>() {
            private Iterator<MapEntry<K, V>> it = buckets.getFirst().iterator();
            private int bucketIndex = 0;
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < size;
            }

            @Override
            public MapEntry<K, V> next() {
                if (!it.hasNext()) {
                    while (++bucketIndex < buckets.size()) {
                        it = buckets.get(bucketIndex).iterator();
                        if (it.hasNext())
                            break;
                    }
                }
                i++;
                return it.next();
            }
        };
    }
}