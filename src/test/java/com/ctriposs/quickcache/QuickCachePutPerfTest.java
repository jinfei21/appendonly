package com.ctriposs.quickcache;

import com.ctriposs.quickcache.util.TestSample;
import com.ctriposs.quickcache.util.TestUtil;
import com.ctriposs.quickcache.utils.FileUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

@RunWith(Parameterized.class)
public class QuickCachePutPerfTest {

    private static final int INIT_COUNT = 200000;
    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "performance/put/";

    private static QuickCache<String> cache;

    @Parameterized.Parameter(value = 0)
    public CacheConfig.StorageMode storageMode;

    @Parameterized.Parameters
    public static Collection<CacheConfig.StorageMode[]> data() throws IOException {
        CacheConfig.StorageMode[][] data = {
                { CacheConfig.StorageMode.PureFile },
        };
        return Arrays.asList(data);
    }

    private QuickCache<String> cache() throws IOException {
        CacheConfig config = new CacheConfig();
        config.setStorageMode(storageMode)
                .setCapacityPerBlock(128 * 1024 * 1024)
                .setExpireInterval(2 * 1000)
                .setMigrateInterval(10);
        QuickCache<String> cache = new QuickCache<String>(TEST_DIR, config);

        TestSample sample = new TestSample();
        for (int i = 0; i < INIT_COUNT; i++) {
            sample.intA = i;
            sample.doubleA = i;
            sample.longA = i;
            cache.put(String.valueOf(i), sample.toBytes());
        }

        return cache;
    }

    @Test
    public void testSingleThreadPut() throws Exception {
        cache = cache();
        final TestSample sample = new TestSample();
        Random random = new Random();

        long start = System.nanoTime();

        for (int i = 0; i < 2 * INIT_COUNT; i++) {
            sample.intA = i;
            sample.doubleA = i;
            sample.longA = i;
            String key = String.valueOf(random.nextInt(INIT_COUNT));
            cache.put(key, sample.toBytes());
        }
        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second%n",
                (int) (INIT_COUNT * 2 * 1e6 / duration));
    }

    @After
    public void close() throws IOException {
        try {
            cache.close();
            FileUtil.deleteDirectory(new File(TEST_DIR));
        } catch (IOException e) {
            System.gc();
            try {
                FileUtil.deleteDirectory(new File(TEST_DIR));
            } catch (IllegalStateException e1) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e2) {
                }
                FileUtil.deleteDirectory(new File(TEST_DIR));
            }
        }
    }
}
