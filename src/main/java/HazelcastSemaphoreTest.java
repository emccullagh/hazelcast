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
    private static final int NUMBER_OF_REQUESTS = 40;
    private static final int REQUEST_TIME_IN_MILLIS = 500;
    private static final int MAX_THREADS = 20;
    private static final int MAX_SEMAPHORE_LEASES = 10;
    private static final int TIME_BETWEEN_THREADS_IN_MILLIS = 10;
    private ISemaphore semaphore;

    public static void main(String[] args) throws InstanceDestroyedException, InterruptedException {
        HazelcastSemaphoreTest test = new HazelcastSemaphoreTest();
        test.run();
    }

    private void run() throws InstanceDestroyedException, InterruptedException {
        initializeSemaphore();
        ExecutorService execService = Executors.newFixedThreadPool(MAX_THREADS);
        long start = now();

        for (int i = 0; i < NUMBER_OF_REQUESTS; i++) {
            execService.execute(new ThrottledRequest());
            sleepQuietly(TIME_BETWEEN_THREADS_IN_MILLIS);
        }

        execService.shutdown();
        waitForShutdown(execService);

        long duration = now() - start;
        log("= total_duration(ms)=[" + duration + "]");
    }

    private void waitForShutdown(ExecutorService execService) {
        while (!execService.isTerminated()) {}
    }

    private static void log(String message) {
        System.out.println(message);
    }

    private void initializeSemaphore() {
        Config cfg = new Config();
        SemaphoreConfig s1Config = new SemaphoreConfig("s1", MAX_SEMAPHORE_LEASES);
        cfg.addSemaphoreConfig(s1Config);
        semaphore = Hazelcast.newHazelcastInstance(cfg).getSemaphore("s1");
    }

    private class ThrottledRequest implements Runnable {
        @Override
        public void run() {
            long start = now();
            acquire(semaphore);
            sleepQuietly(REQUEST_TIME_IN_MILLIS);
            release(semaphore);
            long duration = now() - start;
            log("- request (duration(ms)=[" + duration + "])");
        }

        private void acquire(ISemaphore semaphore) {
            try {
                long start = now();
                semaphore.acquireAttach();
                long duration = now() - start;
                log("> acquire (available_permits=[" + semaphore.availablePermits() + "], duration(ms)=[" + duration + "])");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void release(ISemaphore semaphore) {
            long start = now();
            semaphore.releaseDetach();
            long duration = now() - start;
            log("< release (available_permits=[" + semaphore.availablePermits() + "], duration(ms)=[" + duration + "])");
        }
    }

    private void sleepQuietly(long timeInMillis) {
        try {
            Thread.sleep(timeInMillis);
        } catch (InterruptedException e) {
            log(e.getMessage());
        }
    }

    private long now() {
        return System.currentTimeMillis();
    }
}
