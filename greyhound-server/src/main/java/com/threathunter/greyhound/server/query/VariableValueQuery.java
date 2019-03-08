package com.threathunter.greyhound.server.query;

import com.threathunter.util.SystemClock;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Query the variable name of a specific key.
 *
 * @author Wen Lu
 */
public class VariableValueQuery implements VariableQuery {
    private static final Logger logger = LoggerFactory.getLogger(VariableValueQuery.class);
    private static volatile AtomicLong incrementalID = new AtomicLong();
    private static final Cache<String, VariableValueQuery> cache;

    static {
        cache = CacheBuilder.newBuilder().expireAfterWrite(500, TimeUnit.MILLISECONDS).build();
    }

    public static VariableValueQuery createQuery(int total, String key, String requestid) {
        if (requestid == null) {
            requestid = incrementalID.getAndIncrement() + "";
        }

        VariableValueQuery query =  new VariableValueQuery(total, requestid, key);
        cache.put(query.getRequestid(), query);
        return query;
    }

    public static VariableValueQuery createQuery(int total, String key) {
        return createQuery(total, key, null);
    }

    public static VariableValueQuery getQuery(String requestid) {
        return cache.getIfPresent(requestid);
    }

    /**
     * the map is only added by a single thread, it's safe here.
     */
    private Map<String, Double> cachedResults = new HashMap<>();
    private final String requestid;
    private final int total;
    private final long createTime;
    private final CountDownLatch latch;
    private final String key;

    public VariableValueQuery(int total, String requestid, String key) {
        this.total = total;
        this.requestid = requestid;
        this.key = key;
        this.createTime = SystemClock.getCurrentTimestamp();
        this.latch = new CountDownLatch(total);
    }

    public int getTotal() {
        return total;
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
            logger.error("esper:interrupted during waiting for the results on key" + key, e);
        }
        return cachedResults;
    }

    public Map<String, Double> waitForResults(long timeout, TimeUnit unit) {
        try {
            boolean finish = latch.await(timeout, unit);
            if (!finish) {
                logger.error("esper:we only get partial results for key {}, need ({}), but only got({})", key, total, cachedResults.size());
                return cachedResults;
            }
        } catch (InterruptedException e) {
            logger.error("esper:interrupted during waiting for the results on key " + key, e);
        }
        return cachedResults;
    }

    public void addResult(String variableName, Double value) {
        if (!cachedResults.containsKey(variableName)) {
            cachedResults.put(variableName, value);
        } else {
            if (value != null) {
                Double old = cachedResults.get(variableName);
                if (old == null) {
                    old = 0.0;
                }
                cachedResults.put(variableName, old + value);
            }
        }
    }

    public void countdown() {
        latch.countDown();
    }
}
