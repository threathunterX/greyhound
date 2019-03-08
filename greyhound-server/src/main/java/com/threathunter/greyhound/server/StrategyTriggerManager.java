package com.threathunter.greyhound.server;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.engine.TriggerEvent;
import com.threathunter.greyhound.server.esper.EsperContainer;
import com.threathunter.greyhound.server.utils.DaemonThread;
import com.threathunter.greyhound.server.utils.DelayEventManager;
import com.threathunter.greyhound.server.utils.StrategyInfoCache;
import com.threathunter.model.Event;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by daisy on 17-11-13
 */
public class StrategyTriggerManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(StrategyTriggerManager.class);
    private static final Logger ANALYSIS_LOGGER = LoggerFactory.getLogger("analytics");

    private final Cache<String, List<String>> dimensionTriggerStrategyCache;
    private final Cache<String, AtomicInteger> eventIdSignalCache;
    private final Cache<String, List<String>> eventIdNoticesCache;
    private final Set<String> enableDimension;

    private final BlockingQueue<TriggerEvent> checkQueue;
    private final Thread checkThread;
    private final NoticeSender noticeSender;
    private final boolean redisMode;
    private final boolean syncNoticeInfo;
    private final DelayEventManager delayEventManager;

    private final boolean debug;
    private final boolean debugAll;
    private final Set<String> debugStrategies;

    private volatile boolean running = false;

    public StrategyTriggerManager(EsperContainer container, Set<String> enableDimension, boolean redisMode, int syncExpireSec) {
        this.dimensionTriggerStrategyCache = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS).build();
        this.eventIdSignalCache = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS).build();
        this.enableDimension = enableDimension;
        this.checkQueue = new ArrayBlockingQueue<>(100000);
        this.checkThread = DaemonThread.INSTANCE.newThread(() -> {
            while (running) {
                try {
                    List<TriggerEvent> checks = new ArrayList<>();
                    this.checkQueue.drainTo(checks, 10000);
                    doStrategyCheck(checks);
                    Thread.sleep(500);
                } catch (Exception e) {
                    LOGGER.error("[greyhound:strategy check thread]", e);
                }
            }
        });
        this.noticeSender = new NoticeSender();
        this.redisMode = redisMode;
        if (syncExpireSec > 0) {
            this.syncNoticeInfo = true;
            this.eventIdNoticesCache = CacheBuilder.newBuilder().expireAfterWrite(syncExpireSec, TimeUnit.SECONDS).build();
        } else {
            this.syncNoticeInfo = false;
            this.eventIdNoticesCache = null;
        }
        this.delayEventManager = new DelayEventManager(container);
        this.debug = CommonDynamicConfig.getInstance().getBoolean("greyhound.server.strategy.debug", false);
        if (debug) {
            String[] debugNames = CommonDynamicConfig.getInstance().getStringArray("greyhound.server.strategy.debug.strategy.names");
            if (debugNames != null && debugNames.length > 0) {
                debugStrategies = new HashSet<>();
                for (String s : debugNames) {
                    debugStrategies.add(s);
                }
                debugAll = false;
            } else {
                debugAll = true;
                debugStrategies = null;
            }
        } else {
            debugAll = false;
            debugStrategies = null;
        }
    }

    public void addDimensionTrigger(String idWithDimension, List<String> strategies) {
        this.dimensionTriggerStrategyCache.put(idWithDimension, strategies);
    }

    public void addCheck(String id, Event event) {
        // first check if need add to queue
        // every id should have at least one signal, for computation in ip dimension
        // if uid is not empty, signal should +1
        // if did is not empty, signal should +1
        try {
            int signaledCount = this.eventIdSignalCache.get(id, () -> new AtomicInteger(0)).incrementAndGet();
            if (signaledCount >= getSignalCount(event)) {
                ANALYSIS_LOGGER.warn("add check 2, id:{}, event:{} ,time: {}",id,event,System.currentTimeMillis());
                this.checkQueue.add(new TriggerEvent(id, event));
            }
        } catch (Exception e) {
            LOGGER.error("add check error", e);
        }
    }

    public void start() {
        if (this.running) {
            LOGGER.warn("[greyhound:strategy check thread] already started");
            return;
        }
        this.running = true;
        this.noticeSender.start(redisMode);
        this.delayEventManager.start();
        this.checkThread.start();
    }

    public void stop() {
        if (!this.running) {
            LOGGER.warn("[greyhound:strategy check thread] already stopped");
            return;
        }
        this.running = false;
        try {
            this.checkThread.join(1000);
            this.noticeSender.stop();
            this.delayEventManager.stop();
        } catch (Exception e) {
        }
    }

    public List<String> getNoticeList(String eventId) {
        if (eventId == null) {
            return null;
        }
        List<String> ret = this.eventIdNoticesCache.getIfPresent(eventId);
        ANALYSIS_LOGGER.warn("getNoticeList, eventId: {} ret: {}",eventId, ret);
        return ret;
    }

    private void doStrategyCheck(List<TriggerEvent> checks) {
        checks.forEach(triggerEvent -> {
            ANALYSIS_LOGGER.warn("get check 1: id: {}",triggerEvent.getEvent().getId());
            Set<String> allDimensionTriggered = new HashSet<>();
            this.enableDimension.forEach(dimension -> {
                String id = String.format("%s_%s", dimension, triggerEvent.getIdentifier());
                List<String> list = this.dimensionTriggerStrategyCache.getIfPresent(id);
                if (list != null) {
                    allDimensionTriggered.addAll(list);
                }
                ANALYSIS_LOGGER.warn("get check 2: id: {} , dimension: {} ,dimensionTriggerStrategy: {}",
                        triggerEvent.getEvent().getId(),dimension, list);
            });

            if (allDimensionTriggered.size() > 0) {
                Set<String> checked = new HashSet<>();
                List<String> toNotice = new ArrayList<>();
                List<String> toProfile = new ArrayList<>();

                allDimensionTriggered.forEach(dimensionTrigger -> {
                    String[] splits = dimensionTrigger.split("@@");
                    String strategy = splits[0];
                    if (debug) {
                        logDebugInfo(strategy, splits[1]);
                    }
                    ANALYSIS_LOGGER.warn("get check 3: id: {} , strategy: {} , dimensionTrigger: {}",
                            triggerEvent.getEvent().getId(), strategy, dimensionTrigger);
                    if (!checked.contains(strategy)) {
                        StrategyInfoCache.StrategyInfo info = StrategyInfoCache.getInstance().getStrategyInfo(strategy);
                        if (info != null) {
                            boolean trigger = true;
                            boolean forDelay = splits.length > 2 && splits[2].equals("delay");
                            for (String exp : info.getAllDimensionedExpression(forDelay)) {
                                if (!allDimensionTriggered.contains(exp)) {
                                    trigger = false;
                                    break;
                                }
                            }
                            if (trigger) {
                                if (splits.length > 2 && splits[2].equals("delay")) {
                                    this.delayEventManager.addDelayEvent(triggerEvent.getEvent(), strategy);
                                } else {
                                    if (info.isProfileScope()) {
                                        toProfile.add(strategy);
                                    } else {
                                        toNotice.add(strategy);
                                    }
                                }
                            }
                            checked.add(strategy);
                        }
                    }
                });
                if (toNotice.size() > 0 || toProfile.size() > 0) {
                    this.noticeSender.sendNotice(triggerEvent, toNotice, toProfile);
                    if (this.syncNoticeInfo && toNotice.size() > 0) {
//                        triggerEvent.getEvent().getPropertyValues().put("noticelist", toNotice);
                        // for synchronize, put the notices into the cache
                        // and the persistent writer can then read the notices before writing events into the disk.
                        this.eventIdNoticesCache.put(triggerEvent.getEvent().getId(), toNotice);
                        ANALYSIS_LOGGER.warn("get check 4: id: {}, toNotice: {} , toProfile:{}",
                                triggerEvent.getEvent().getId(), toNotice, toProfile);
                    }
                }

            }

        });
    }

    private int getSignalCount(Event event) {
        int i = 1;
        if (!((String) event.getPropertyValues().getOrDefault("uid", "")).isEmpty()) {
            i++;
        }
        if (!((String) event.getPropertyValues().getOrDefault("did", "")).isEmpty()) {
            i++;
        }
        return i;
    }

    private void logDebugInfo(String strategy, String dimension) {
        if (!debugAll) {
            if (!debugStrategies.contains(strategy)) {
                return;
            }
        }
        ANALYSIS_LOGGER.info(String.format("check strategy: %s, dimension: %s", strategy, dimension));
    }
}
