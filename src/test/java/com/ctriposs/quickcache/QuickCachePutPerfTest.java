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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RunWith(Parameterized.class)
public class QuickCachePutPerfTest {

    private static final int INIT_COUNT = 400000;
    private static final int THREAD_COUNT = 256;
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
            sample.longA = 10;
            sample.stringA = "a";
            String key = String.valueOf(random.nextInt(INIT_COUNT));
            cache.put(key, sample.toBytes());
        }
        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second single thread%n",
                (int) (INIT_COUNT * 2 * 1e6 / duration));
    }

    @Test
    public void testMultiThreadPut() throws Exception {
        final int count = 2*1000*1000;
        ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final Random random = new Random();

        cache = cache();

        List<Future<?>> futures = new ArrayList<Future<?>>();
        long start = System.nanoTime();
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int finalI = i;
            futures.add(service.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        final TestSample sample = new TestSample();
                        for (int j = finalI; j < count; j += THREAD_COUNT) {
                            sample.intA = j;
                            sample.doubleA = j;
                            sample.longA = j;
                            sample.stringA = "a";
                            cache.put(String.valueOf(random.nextInt(INIT_COUNT)), sample.toBytes());
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second multi thread%n",
                (int) (count * 1e6 / duration));
        service.shutdown();
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
