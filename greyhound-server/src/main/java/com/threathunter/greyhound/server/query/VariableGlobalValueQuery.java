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
 * Query the variable values by broadcasting requests to all the instances.
 *
 * Some variable may exist in all the sharded instances, such as event count...
 *
 * @author Wen Lu
 */
public class VariableGlobalValueQuery implements VariableQuery {
    private static final Logger logger = LoggerFactory.getLogger(VariableGlobalValueQuery.class);
    private static volatile AtomicLong incrementalID = new AtomicLong();
    private static final Cache<String, VariableGlobalValueQuery> cache;

    static {
        cache = CacheBuilder.newBuilder().expireAfterWrite(500, TimeUnit.MILLISECONDS).build();
    }

    public static VariableGlobalValueQuery createQuery(int total, String key, String requestid) {
        if (requestid == null) {
            requestid = incrementalID.getAndIncrement() + "";
        }

        VariableGlobalValueQuery query =  new VariableGlobalValueQuery(total, requestid, key);
        cache.put(query.getRequestid(), query);
        return query;
    }

    public static VariableGlobalValueQuery createQuery(int total, String key) {
        return createQuery(total, key, null);
    }

    public static VariableGlobalValueQuery getQuery(String requestid) {
        return cache.getIfPresent(requestid);
    }

    /**
     * the map is only added by a single thread, it's safe here.
     */
    private ConcurrentMap<String, Double> cachedResults = new ConcurrentHashMap<>();
    private final String requestid;
    private final int total;
    private final long createTime;
    private final CountDownLatch latch;
    private final String key;

    public VariableGlobalValueQuery(int total, String requestid, String key) {
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
        if (value != null && cachedResults.putIfAbsent(variableName, value) != null) {
            // there are already value
            while(true) {
                Double old = cachedResults.get(variableName);
                Double newValue = null;
                if (old == null && value == null) {
                    newValue = null;
                } else if (old == null) {
                    // value != null
                    newValue = value;
                } else if (value == null) {
                    // old != null
                    newValue = old;
                } else {
                    // old != null && value != null
                    newValue = old + value;
                }
                if (cachedResults.replace(variableName, old, newValue)) {
                    break;
                }
            }

        }
    }

    public void countdown() {
        latch.countDown();
    }
}
