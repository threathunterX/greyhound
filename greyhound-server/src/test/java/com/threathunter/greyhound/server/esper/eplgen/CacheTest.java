package com.threathunter.greyhound.server.esper.eplgen;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 */
public class CacheTest {
    @Test
    public void test() throws ExecutionException, InterruptedException {
        Cache<String, AtomicInteger> cache = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.SECONDS).build();
        System.out.println(cache.get("1", () -> new AtomicInteger()).incrementAndGet());
        System.out.println(cache.get("1", () -> new AtomicInteger()).incrementAndGet());
        System.out.println(cache.get("2", () -> new AtomicInteger()).incrementAndGet());
        System.out.println(cache.get("1", () -> new AtomicInteger()).incrementAndGet());
        Thread.sleep(6000);
        System.out.println(cache.get("1", () -> new AtomicInteger()).incrementAndGet());
    }
}
