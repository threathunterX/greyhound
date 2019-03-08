package com.threathunter.greyhound.server.query;

import com.threathunter.util.SystemClock;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Get top values of a variable, the key should meet the given condition.
 *
 * @author Wen Lu
 */
public class VariableKeyTopValueQuery {
    private static final Logger logger = LoggerFactory.getLogger(VariableKeyTopValueQuery.class);
    private static volatile AtomicLong incrementalID = new AtomicLong();
    private static final Cache<String, VariableKeyTopValueQuery> cache;

    static {
        cache = CacheBuilder.newBuilder().expireAfterWrite(500, TimeUnit.MILLISECONDS).build();
    }

    public static VariableKeyTopValueQuery createQuery(int resultCount, String key, String requestid) {
        if (requestid == null) {
            requestid = incrementalID.getAndIncrement() + "";
        }
        VariableKeyTopValueQuery query =  new VariableKeyTopValueQuery(requestid, resultCount, key);
        cache.put(requestid, query);
        return query;
    }

    public static VariableKeyTopValueQuery createQuery(int resultCount, String key) {
        return createQuery(resultCount, key, null);
    }

    public static VariableKeyTopValueQuery getQuery(String requestid) {
        return cache.getIfPresent(requestid);
    }

    private ConcurrentMap<String, Double> cachedResults = new ConcurrentHashMap<>();
    private final String requestid;
    private final long createTime;
    private final CountDownLatch latch;
    private final String key;

    public VariableKeyTopValueQuery(String requestid, int resultCount, String key) {
        this.requestid = requestid;
        this.createTime = SystemClock.getCurrentTimestamp();
        this.latch = new CountDownLatch(resultCount);
        this.key = key;
    }

    public String getRequestid() {
        return requestid;
    }

    public long getCreateTime() {
        return createTime;
    }

    public Map<String, Double> getCachedResults() {
        return cachedResults;
    }

    public Map<String, Double> waitForResults() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("esper:interrupted during waiting for the results on key " + key, e);
        }
        return cachedResults;
    }

    public Map<String, Double> waitForResults(long timeout, TimeUnit unit) {
        try {
            boolean finish = latch.await(timeout, unit);
            if (!finish) {
                logger.error("esper:we don't get result for key " + key);
            }
        } catch (InterruptedException e) {
            logger.error("esper:interrupted during waiting for the results on key " + key, e);
        }
        return cachedResults;
    }

    public void setResult(Map<String, Double> values) {
        cachedResults.putAll(values);
    }

    public void countdown() {
        latch.countDown();
    }
}
