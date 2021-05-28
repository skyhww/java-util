package hww.utils.redis;

import java.util.function.Supplier;

/**
 * redisLock提供资源保护功能，防止获取锁后忘记关闭。同时RedisLockAcquiredTimeOutException，提供了锁释放失败的提示
 *
 * @author hanweiwei
 * @see RedisLock#doWithLock(Supplier, Long)
 */
public interface RedisLock {
    Long defaultExpireMills = 5000L;

    String getKey();

    <T> ResultHolder<T> doWithLock(Supplier<T> supplier, Long expireMills) throws RedisLockAcquiredTimeOutException;

    /**
     * 业务端有必要处理该异常
     */
    class RedisLockAcquiredTimeOutException extends RuntimeException {
        private ResultHolder resultHolder;

        public RedisLockAcquiredTimeOutException(ResultHolder resultHolder) {
            this.resultHolder = resultHolder;
        }

        public ResultHolder getResultHolder() {
            return resultHolder;
        }

    }

    class ResultHolder<T> {
        private Throwable err;
        private boolean lockAcquired = false;
        private T result;

        public Throwable getErr() {
            return err;
        }

        public void setErr(Throwable err) {
            this.err = err;
        }

        public boolean isLockAcquired() {
            return lockAcquired;
        }

        public void setLockAcquired(boolean lockAcquired) {
            this.lockAcquired = lockAcquired;
        }

        public T getResult() {
            return result;
        }

        public void setResult(T result) {
            this.result = result;
        }
    }
}
