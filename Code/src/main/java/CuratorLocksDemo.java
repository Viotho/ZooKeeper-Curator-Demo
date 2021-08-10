import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import utils.FakeLimitedResource;

import java.util.concurrent.TimeUnit;

@Slf4j
public class CuratorLocksDemo {
    public static class ReentrantLockDemo{
        public static void main(String[] args) {
            CuratorFramework client = CuratorDemo.initWithBuilder();
            String lockPath = "/lock";
            InterProcessMutex lock = new InterProcessMutex(client, lockPath);
            for (int i = 0; i < 50; i++) {
                new Thread(new TestThread(i, lock)).start();
            }

        }
        public static class TestThread implements Runnable {
            private Integer threadFlag;
            private InterProcessMutex lock;

            public TestThread(Integer threadFlag, InterProcessMutex lock) {
                this.threadFlag = threadFlag;
                this.lock = lock;
            }

            @Override
            public void run() {
                try {
                    lock.acquire();
                } catch (Exception e) {
                    log.error(e.toString());
                } finally {
                    try {
                        lock.release();
                    } catch (Exception e) {
                        log.error(e.toString());
                    }
                }
            }
        }
    }

    public static class ReentrantReadWriteLockDemo {
        private InterProcessReadWriteLock lock;
        private InterProcessMutex readLock;
        private InterProcessMutex writeLock;
        private FakeLimitedResource resource;

        public ReentrantReadWriteLockDemo(CuratorFramework client, String lockPath, FakeLimitedResource resource) {
            this.lock = new InterProcessReadWriteLock(client, lockPath);
            this.readLock = lock.readLock();
            this.writeLock = lock.writeLock();
            this.resource = resource;
        }

        public void doWork(long time, TimeUnit timeUnit) throws Exception {
            // Acquiring writeLock before acquiring readLock.
            if (!writeLock.acquire(time, timeUnit)) {
                log.error("Can not acquire write lock");
            }
            if (!readLock.acquire(time, timeUnit)) {
                log.error("Can not acquire read lock");
            }
            log.debug("Read write lock acquired.");
            try {
                resource.use();
            } finally {
                readLock.release();
                writeLock.release();
            }
        }
    }
}
