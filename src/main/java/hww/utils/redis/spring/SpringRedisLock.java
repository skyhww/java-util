package hww.utils.redis.spring;

import hww.utils.redis.RedisLock;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * 基于spring的redis处理
 *
 * @author hanweiwei
 */
public class SpringRedisLock implements RedisLock, ApplicationContextAware {
    private RedisTemplate redisTemplate;
    private ApplicationContext applicationContext;
    private String key;
    private List<String> keyList = new ArrayList<>(1);
    private static final RedisScript<String> SCRIPT_LOCK = new DefaultRedisScript<>("return redis.call('set',KEYS[1],ARGV[1],'NX','PX',ARGV[2])", String.class);
    private static final RedisScript<String> SCRIPT_UNLOCK = new DefaultRedisScript<>("if redis.call('get',KEYS[1]) == ARGV[1] then return tostring(redis.call('del',KEYS[1])==1) else return '0' end", String.class);
    private String id;

    public SpringRedisLock(RedisTemplate redisTemplate, String key) {
        this.redisTemplate = redisTemplate;
        this.key = key;
        keyList.add(key);
    }

    public SpringRedisLock(String key) {
        this.key = key;
        keyList.add(key);
    }

    @Override
    public String getKey() {
        return key;
    }


    @SuppressWarnings("unchecked")
    protected boolean tryLock(long expireMillis) {
        id = UUID.randomUUID().toString();
        String r = (String) redisTemplate.execute(SCRIPT_LOCK, redisTemplate.getStringSerializer(), redisTemplate.getStringSerializer(), keyList, id, String.valueOf(expireMillis));
        return "OK".equalsIgnoreCase(r);
    }

    @SuppressWarnings("unchecked")
    protected boolean release() {
        String result = (String) getRedisTemplate().execute(SCRIPT_UNLOCK, redisTemplate.getStringSerializer(), redisTemplate.getStringSerializer(), keyList, id);
        boolean released = "1".equals(result);
        id = null;
        return released;
    }

    @Override
    public <T> ResultHolder<T> doWithLock(Supplier<T> supplier, Long expireMills) throws RedisLockAcquiredTimeOutException {
        ResultHolder<T> resultHolder = new ResultHolder<>();
        if (tryLock(expireMills == null || expireMills <= 0 ? defaultExpireMills : expireMills)) {
            resultHolder.setLockAcquired(true);
            try {
                T r = supplier.get();
                resultHolder.setResult(r);
            } catch (Exception e) {
                resultHolder.setErr(e);
            }
            if (!release()) {
                throw new RedisLockAcquiredTimeOutException(resultHolder);
            }
        }
        return resultHolder;
    }

    protected RedisTemplate getRedisTemplate() {
        if (redisTemplate == null && applicationContext != null) {
            redisTemplate = applicationContext.getBean(RedisTemplate.class);
        }
        return redisTemplate;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
