import com.hazelcast.config.Config;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.InstanceDestroyedException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: eugene
 * Date: 13/02/2013
 * Time: 21:06
 * To change this template use File | Settings | File Templates.
 */
public class HazelcastSemaphoreTest {
    private static final long REQUEST_DELAY = 12;
    private static final int MAX_REQUESTS = 100000;
    private ISemaphore semaphore;

    public static void main(String[] args) throws InstanceDestroyedException, InterruptedException {
        HazelcastSemaphoreTest test = new HazelcastSemaphoreTest();
        test.run();
    }

    private void run() throws InstanceDestroyedException, InterruptedException {
        initializeSemaphore();
        ExecutorService execService = Executors.newFixedThreadPool(20);
        for (int i = 0; i < MAX_REQUESTS; i++) {
            execService.execute(new ThrottledRequest());
        }
    }

    private void acquire(ISemaphore semaphore) {
        try {
            long start = now();
            semaphore.acquireAttach();
            long end = now();
            long duration = end - start;
            log("after_acquire. available_permits=[" + semaphore.availablePermits() + "], duration(ms)=[" + duration + "]");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void release(ISemaphore semaphore) {
        long start = System.currentTimeMillis();
        semaphore.releaseDetach();
        //assert(semaphore.availablePermits() > 0 && semaphore.availablePermits() < 10);
        long end = System.currentTimeMillis();
        long duration = end - start;
        log("after_release. available_permits=[" + semaphore.availablePermits() + "], duration(ms)=[" + duration + "]");
    }

    private static void log(String message) {
        System.out.println(message);
    }

    private void initializeSemaphore() {
        Config cfg = new Config();
        SemaphoreConfig s1Config = new SemaphoreConfig("s1", 10);
        cfg.addSemaphoreConfig(s1Config);
        semaphore = Hazelcast.newHazelcastInstance(cfg).getSemaphore("s1");
    }

    private class ThrottledRequest implements Runnable {
        @Override
        public void run() {
            long start = now();
            acquire(semaphore);
            release(semaphore);
            long end = now();
            long duration = end - start;
            log("request. duration(ms)=[" + duration + "]");
        }

        private void sleepQuietly(long timeInMillis) {
            try {
                Thread.sleep(timeInMillis);
            } catch (InterruptedException e) {

            }
        }
    }

    private long now() {
        return System.currentTimeMillis();
    }
}
