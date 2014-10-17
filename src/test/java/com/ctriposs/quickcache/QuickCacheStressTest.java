package com.ctriposs.quickcache;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.ctriposs.quickcache.util.TestUtil;

public class QuickCacheStressTest {

    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "stress/simplecache/";

    private static QuickCache<String> cache;

    public static void main(String[] args) throws IOException {
        int numKeyLimit = 1024 * 16;
        int valueLengthLimit = 1024 * 16;

        CacheConfig config = new CacheConfig();
        config.setStorageMode(CacheConfig.StorageMode.MapFile)
                .setExpireInterval(2 * 1000)
                .setMigrateInterval(2 * 1000)
                .setMaxOffHeapMemorySize(1 * 1024 * 1024 * 1024L);
        cache = new QuickCache<String>(TEST_DIR, config);
        Map<String, byte[]> bytesMap = new HashMap<String, byte[]>();

        String[] rndStrings = new String[] {
                TestUtil.randomString(valueLengthLimit/2),
                TestUtil.randomString(valueLengthLimit),
                TestUtil.randomString(valueLengthLimit + valueLengthLimit/2)
        };
        byte[] bytes = rndStrings[1].getBytes();

        Random random = new Random();

        System.out.println("Start from date " + new Date());
        long start = System.currentTimeMillis();
        for (long counter = 0;; counter++) {

            int rndKey = random.nextInt(numKeyLimit);

            boolean put = true;
            if (put) {
                bytes = rndStrings[random.nextInt(3)].getBytes();
                if (counter <= numKeyLimit) {
                    bytesMap.put(String.valueOf(counter), bytes);
                }
                //bytesMap.put(String.valueOf(counter), bytes);
                cache.put(String.valueOf(counter), bytes);
            } else {
            /*bytesMap.remove(String.valueOf(rndKey));
            byte[] oldV = cache.delete(String.valueOf(rndKey));
            byte[] v = cache.get(String.valueOf(rndKey));
            if (v != null) {
                System.out.println("should be null. key:" + String.valueOf(rndKey) + "    Value:" + new String(v));
                System.out.println("                key:" + String.valueOf(rndKey) + "    oldValue:" + (oldV == null ? null : new String(oldV)));
            }*/
            }

            cache.put(counter + "-ttl", bytes, (long) 10 * 1000);

            if (counter  % 1000000 == 0) {
                System.out.println("Current date:" + new Date());
                System.out.println("counter:     " + counter);
                System.out.println("purge       " + cache.getExpireCounter());
                System.out.println("move        " + cache.getMigrateCounter());

                System.out.println("size:        " + cache.getCount());            
                System.out.println("used:         " + cache.getUsedSize());

                System.out.println();

                System.out.println();
                System.out.println(TestUtil.getMemoryFootprint());
                long end = System.currentTimeMillis();
				System.out.println("timeSpent = " + (end - start));
				System.out.println("ttl count = " + (cache.getCount() - bytesMap.size()));
				System.out.println("used size = " + cache.getUsedSize());

                for (int i = 0; i < numKeyLimit; i++) {
                    String key = String.valueOf(i);
                    byte[] mapValue = bytesMap.get(key);
                    byte[] cacheValue = cache.get(key);

                    if (mapValue == null && cacheValue != null) {
                        System.out.println("Key:" + key);
                        System.out.println("Value:" + new String(cacheValue));
                        throw new RuntimeException("Validation exception, key exists in cache but not in map");
                    }
                    if (mapValue != null && cacheValue == null) {
                    	cacheValue = cache.get(key);
                    	System.out.println("Validation exception, key exists in map but not in cache");
                        System.out.println("Key:" + key);
                        System.out.println("Value:" + new String(cacheValue));
                        System.out.println("Value:" + new String(mapValue));
                        //throw new RuntimeException("Validation exception, key exists in map but not in cache");

                    }
                    if (cacheValue != null && mapValue != null) {
                        if (compare(mapValue, cacheValue) != 0) {
                        	System.out.println("Validation exception, value in map does not equal to cache");
                            System.out.println("Key:" + key);
                            System.out.println("Value:" + new String(cacheValue));
                            System.out.println("Value:" + new String(mapValue));
                            cacheValue = cache.get(key);
                         
                           // throw new RuntimeException("Validation exception, value in map does not equal to cache");
                        }
                        
                    }
                }

                start = System.currentTimeMillis();
            }
        }
    }

    public static int compare(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }

        return left.length - right.length;
    }
}
