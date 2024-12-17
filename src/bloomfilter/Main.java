package bloomfilter;


import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class Main {
    public static void main(String[] args) throws IOException {
        int numInserts = 16;

        Random rng = new Random();

        BloomFilter<Integer> filter = new BloomFilter<>(64);

        BloomList<Integer> bloomList = new BloomList<>(filter);
        List<Integer> list = new LinkedList<>();

        for (int i = 0; i < numInserts; i++) {
            int n = rng.nextInt();
            bloomList.add(n);
            list.add(n);
        }

        // Test serialization

        DataOutputStream dos = new DataOutputStream(new FileOutputStream("bloom.dat"));
        filter.close(dos);
        dos.flush();
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream("bloom.dat"));
        filter = new BloomFilter<>(dis);
        for (Integer i : list) {
            if (!filter.containsMaybe(i)) {
                throw new RuntimeException("Not contained: " + i);
            }
        }
        dis.close();

        // Compare (mostly) unsuccessful search:
        long t0, t1;

        t0 = System.currentTimeMillis();
        for (int i = 0; i < numInserts; i++)
            list.contains(rng.nextInt());

        t1 = System.currentTimeMillis();
        System.out.println("list: " + (t1 - t0) + " ms");

        t0 = System.currentTimeMillis();
        for (int i = 0; i < numInserts; i++)
            bloomList.contains(rng.nextInt());

        t1 = System.currentTimeMillis();
        System.out.println("bloom list: " + (t1 - t0) + " ms");
    }
}