package hww.utils.redis.spring;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.function.Supplier;

/**
 * 提供了基于spring retry的重试功能
 *
 * @author hanweiwei
 */
public class StringRetryAbleRedisLock extends SpringRedisLock {

    private static final int retryTimes = 3;
    private static final SimpleRetryPolicy SIMPLE_RETRY_POLICY = new SimpleRetryPolicy(retryTimes);
    private static final FixedBackOffPolicy FIXED_BACK_OFF_POLICY;
    private static final RetryListener RETRY_LISTENER = new RetryListenerSupport() {
        @Override
        public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            if (throwable instanceof RedisLockAcquiredTimeOutException) {
                throw (RedisLockAcquiredTimeOutException) throwable;
            }
        }
    };

    static {
        FIXED_BACK_OFF_POLICY = new FixedBackOffPolicy();
        FIXED_BACK_OFF_POLICY.setBackOffPeriod(100);
    }

    public StringRetryAbleRedisLock(String key) {
        super(key);
    }

    public StringRetryAbleRedisLock(RedisTemplate redisTemplate, String key) {
        super(redisTemplate, key);
    }

    public <T> ResultHolder<T> doWithRetry(Supplier<T> supplier, Long expireMills, RetryPolicy retryPolicy, BackOffPolicy backOffPolicy) throws RedisLockAcquiredTimeOutException {
        RetryTemplate retryTemplate = new RetryTemplate();
        if (retryPolicy == null) {
            retryPolicy = SIMPLE_RETRY_POLICY;
        }
        if (backOffPolicy == null) {
            backOffPolicy = FIXED_BACK_OFF_POLICY;
        }

        retryTemplate.setBackOffPolicy(backOffPolicy);
        retryTemplate.setRetryPolicy(retryPolicy);
        ResultHolder<T> resultHolder = null;
        retryTemplate.setListeners(new RetryListener[]{RETRY_LISTENER});
        try {
            resultHolder = (ResultHolder<T>) retryTemplate.execute((RetryCallback<Object, Throwable>) retryContext -> doWithLock(supplier, expireMills));
        } catch (Throwable throwable) {
            resultHolder = new ResultHolder<>();
            resultHolder.setLockAcquired(false);
        }
        return resultHolder;
    }

}
