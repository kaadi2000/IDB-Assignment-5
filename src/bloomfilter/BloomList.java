package bloomfilter;

import java.util.Collection;
import java.util.LinkedList;

public class BloomList<E> extends LinkedList<E> {
    private final BloomFilter<E> bf;

    /**
     * Create a BloomList from a BloomFilter instance.
     */
    public BloomList(BloomFilter<E> bf) {
        this.bf = bf;
    }

    /**
     * Reset the bloom filter to bring it to the optimal state
     * (i.e. the state reached when inserting all current element into the filter).
     * Can be called after (many) deletions to reduce the number of false positives.
     */
    public void resetBloomFilter() {
        // TODO

        bf.reset();
        for(E e : this) {
            bf.add(e);
        }
    }

    @Override
    public boolean add(E e) {
        // TODO

        boolean added = super.add(e);

        if(added){
            bf.add(e);
        }
        return added;
    }

    @Override
    public void add(int index, E element) {
        // TODO
        super.add(index, element);
        bf.add(element);
    }

    @Override
    public E set(int i, E e) {
        // TODO
        E prev = super.set(i, e);

        resetBloomFilter();
        return prev;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        // TODO

        boolean added = super.addAll(index, c);
        if(added){
            for(E e : c){
                bf.add(e);
            }
        }
        return added;
    }

    @Override
    public boolean contains(Object e) {
        // TODO - Done

        if(!bf.containsMaybe((E) e)){
            return false;
        }
        return super.contains(e);
    }

    @Override
    public void clear() {
        // TODO - Done
        super.clear();
        bf.reset();
    }
}
