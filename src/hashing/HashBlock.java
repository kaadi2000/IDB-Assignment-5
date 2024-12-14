package hashing;

import xxl.core.collections.MapEntry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A class representing a hash block. Contains a list of K-V pairs, and possibly an identifier to an overflow block.
 * An overflow block id of 0 is null, i.e. there is no overflow.
 */
public class HashBlock<K, V> implements Iterable<MapEntry<K, V>> {

    private List<MapEntry<K, V>> elems;
    private long overflowId;

    public HashBlock() {
        elems = new ArrayList<>();
        overflowId = 0;
    }

    public HashBlock(List<MapEntry<K, V>> elems, long overflowId) {
        this.elems = elems;
        this.overflowId = overflowId;
    }

    public void clearElements() {
        this.elems = new ArrayList<>();
    }

    public void add(MapEntry<K, V> elem) {
        this.elems.add(elem);
    }

    public int getSize() {
        return elems.size();
    }

    public boolean hasOverflow() {
        return overflowId != 0;
    }

    public void setOverflowId(long id) {
        this.overflowId = id;
    }

    public long getOverflowId() {
        return overflowId;
    }

    public void unsetOverflow() {
        this.overflowId = 0;
    }

    @Override
    public Iterator<MapEntry<K, V>> iterator() {
        return elems.iterator();
    }
}