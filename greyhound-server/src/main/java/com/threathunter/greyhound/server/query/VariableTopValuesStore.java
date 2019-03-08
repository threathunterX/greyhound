package com.threathunter.greyhound.server.query;

import com.threathunter.util.SystemClock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.stream.Collectors.toList;

/**
 * @author Wen Lu
 */
public class VariableTopValuesStore {
    public static ConcurrentMap<Long, Map<String, Map<String, Double>>> store = new ConcurrentHashMap<>();
    private static volatile Map<String, Long> lastModifyTsMap = new ConcurrentHashMap<String, Long>();

    public static void setTopValuesInThisThread(String variable, Map<String, Double> topvalues) {
        Long id = Thread.currentThread().getId();
        Map<String, Map<String, Double>> m = store.get(id);
        if (m == null) {
            store.putIfAbsent(id, new HashMap<>());
            m = store.get(id);
        }

        m.put(variable, topvalues);
        lastModifyTsMap.put(variable, SystemClock.getCurrentTimestamp());
    }

    public static Map<String, Double> getTopValues(String variable) {
        long current = SystemClock.getCurrentTimestamp();
        if (current - lastModifyTsMap.getOrDefault(variable, 0L)> 300 * 1000) {
            // no update in recent 5 minutes
            return Collections.emptyMap();
        }

        Map<String, Double> result = new HashMap<>();
        for(Map<String, Map<String, Double>> m : store.values()) {
            Map<String, Double> threadData = m.get(variable);
            if (threadData != null) {
                result.putAll(threadData);
            }
        }

        Comparator<Map.Entry<String, Double>> c = Comparator.comparing(Map.Entry::getValue);
        c = c.reversed();
        List<Map.Entry<String, Double>> list = result.entrySet().stream().sorted(c).limit(10).collect(toList());

        result = new HashMap<>();
        for(Map.Entry<String, Double> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
