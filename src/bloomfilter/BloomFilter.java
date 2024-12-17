package bloomfilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;


public class BloomFilter<E> {
    protected static final int H_MUL = 31;

    private byte[] bits;
    private Function<Integer, Integer>[] hf;

    public BloomFilter(DataInput input) throws IOException {
        // TODO -Done

        int numByte = input.readInt();
        this.bits = new byte[numByte];
        
        input.readFully(this.bits);

        final int vectorSize = numByte * Byte.SIZE;
        int numHashes = (int) -(Math.log(0.01) / Math.log(2));
        this.hf = new Function[numHashes];
        for(int i = 0; i < numHashes; i++) {
            int j = i;
            this.hf[i] = k -> {
                int hash = (j * H_MUL * k) % vectorSize;
                if (hash < 0) {
                    hash += vectorSize;
                }
                return hash;
            };
        }

    }

    public BloomFilter(final int numBytes) {
        final int vecSize = numBytes * Byte.SIZE;
        this.bits = new byte[numBytes];
        // False positive rate of 1%:
        this.hf = new Function[(int) -(Math.log(0.01) / Math.log(2))];
        for (int i = 0; i < this.hf.length; i++) {
            final int j = i;
            this.hf[i] = k -> (((j * H_MUL * k) % vecSize) + vecSize) % vecSize;
        }
    }

    /**
     * Serialize this filter into the data output, packing 8 bytes from the bits vector into one output byte.
     */
    public void close(DataOutput output) throws IOException {
        // TODO - Done - Should work!!

        output.writeInt(this.bits.length);
        output.write(this.bits);
    }

    /**
     * Add an element to this filter.
     */
    public void add(E element) {
        // TODO - Done

        int codeHash = element.hashCode();
        for (Function<Integer, Integer> hashFunction : this.hf) {
            int i = hashFunction.apply(codeHash);
            int byteIndex = i / 8;
            int bitPos = i % 8;
            bits[byteIndex] |= (1 << bitPos);
        }
    }

    /**
     * Returns false, if the element was definitely not added to the filter, true otherwise.
     */
    public boolean containsMaybe(E element) {
        // TODO - Done

        int codeHash = element.hashCode();
        for(Function<Integer, Integer> hashFunction : this.hf) {
            int i = hashFunction.apply(codeHash);
            int byteIndex = i / 8;
            int bitPos = i % 8;
            boolean result = (bits[byteIndex] & (1 << bitPos)) != 0;
            if (!result) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reset the state of the bloom filter.
     */
    public void reset() {
        // TODO - Done
        Arrays.fill(bits, (byte) 0);
        return;
    }
}
