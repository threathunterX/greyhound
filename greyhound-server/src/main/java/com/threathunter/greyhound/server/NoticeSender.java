package com.threathunter.greyhound.server;

import com.threathunter.babel.meta.ServiceMeta;
import com.threathunter.babel.meta.ServiceMetaUtil;
import com.threathunter.babel.rpc.impl.ServiceClientImpl;
import com.threathunter.greyhound.server.engine.TriggerEvent;
import com.threathunter.greyhound.server.utils.StrategyInfoCache;
import com.threathunter.model.Event;
import com.threathunter.util.MetricsHelper;
import com.threathunter.util.SystemClock;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by daisy on 17-11-14
 */
public class NoticeSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoticeSender.class);

    private final BlockingQueue<NoticeInfoHolder> cache = new LinkedBlockingDeque<>();
    private volatile boolean running = false;
    private final Worker worker;
    private ServiceClientImpl noticeNotifyClient;
    private ServiceClientImpl profileNotifyClient;
    private ServiceMeta noticeNotifyMeta;
    private ServiceMeta profileNotifyMeta;

    private final Cache<String, Long> noticesCache = CacheBuilder.newBuilder().maximumSize(3000).build();

    public NoticeSender() {
        running = true;
        worker = new Worker();
    }

    public void start(boolean redisMode) {
        if (redisMode) {
            noticeNotifyMeta = ServiceMetaUtil.getMetaFromResourceFile("NoticeNotify_redis.service");
            profileNotifyMeta = ServiceMetaUtil.getMetaFromResourceFile("ProfileNoticeChecker_redis.service");
        } else {
            noticeNotifyMeta = ServiceMetaUtil.getMetaFromResourceFile("NoticeNotify_rmq.service");
            profileNotifyMeta = ServiceMetaUtil.getMetaFromResourceFile("ProfileNoticeChecker_redis.service");
        }

        noticeNotifyClient = new ServiceClientImpl(noticeNotifyMeta);
        profileNotifyClient = new ServiceClientImpl(profileNotifyMeta);
        noticeNotifyClient.start();
        profileNotifyClient.start();
        worker.start();
    }

    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        try {
            noticeNotifyClient.stop();
            profileNotifyClient.stop();
            worker.interrupt();
            worker.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
        }
    }

    private boolean needNotice(String key, String strategy, long timestamp) {
        String cacheKey = String.format("%s_%s", strategy, key);
        Long oldTimestamp = noticesCache.getIfPresent(cacheKey);
        if (oldTimestamp != null && (timestamp - oldTimestamp) < 300 * 1000) {
            return false;
        }
        noticesCache.put(cacheKey, timestamp);
        return true;
    }


    public void sendNotice(TriggerEvent triggerEvent, List<String> strategies, List<String> toProfile) {
        this.cache.add(new NoticeInfoHolder(triggerEvent, strategies, toProfile));
    }

    private class Worker extends Thread {
        public Worker() {
            super("notice sender");
            this.setDaemon(true);

        }

        @Override
        public void run() {
            int idle = 0;
            while (running) {
                List<NoticeInfoHolder> toNotice = new ArrayList<>();
                cache.drainTo(toNotice, 1000);
                if (toNotice.isEmpty()) {
                    idle++;
                    if (idle >= 3) {
                        // sleep after 3 times that no event is coming
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    idle = 0;
                    try {
                        List<Event> noticeEvents = new ArrayList<>();
                        List<Event> profileEvents = new ArrayList<>();
                        for (NoticeInfoHolder holder : toNotice) {
                            if (holder.getToProfile().size() > 0) {
                                profileEvents.add(genProfileEvent(holder.getTriggerEvent().getEvent(), holder.getToProfile()));
                            }
                            List<String> notices = holder.getStrategies();
                            try {
                                for (String notice : notices) {
                                    Event event = genNoticeEvents(notice, holder.getTriggerEvent().getEvent());
                                    if (event != null) {
                                        noticeEvents.add(event);
                                    }
                                }
                            } catch (Exception e) {
                                LOGGER.error("[greyhound:notice sender]", e);
                            }
                        }
                        if (noticeEvents.size() == 1) {
                            noticeNotifyClient.notify(noticeEvents.get(0), noticeNotifyMeta.getName());
                        } else if (noticeEvents.size() > 0) {
                            noticeNotifyClient.notify(noticeEvents, noticeNotifyMeta.getName());
                        }
                        if (profileEvents.size() == 1) {
                            profileNotifyClient.notify(profileEvents.get(0), profileNotifyMeta.getName());
                        } else if (profileEvents.size() > 0) {
                            profileNotifyClient.notify(profileEvents, profileNotifyMeta.getName());
                        }
                        MetricsHelper.getInstance().addMetrics("nebula.notices", (double) noticeEvents.size());
                        MetricsHelper.getInstance().addMetrics("nebula.profile.notice", (double) profileEvents.size());
                    } catch (Exception ex) {
                        LOGGER.error("rpc:fatal:fail to send notice", ex);
                    }
                }
            }
        }
    }

    private Event genProfileEvent(Event event, List<String> toProfile) {
        Map<String, Object> properties = new HashMap<>();
        properties.putAll(event.getPropertyValues());
        properties.put("strategylist", toProfile);
        return new Event(event.getApp(), event.getName(), event.getKey(), event.getTimestamp(), event.value(), properties);
    }

    private Event genNoticeEvents(String strategy, Event triggerEvent) {
        StrategyInfoCache.StrategyInfo info = StrategyInfoCache.getInstance().getStrategyInfo(strategy);
        if (info == null) {
            return null;
        }
        String key = (String) triggerEvent.getPropertyValues().get(info.getCheckValue());
        if (!needNotice(key, strategy, triggerEvent.getTimestamp())) {
            return null;
        }

        Map<String, Object> propertyValues = new HashMap<>();
        propertyValues.put("riskScore", info.getScore());
        propertyValues.put("strategyName", strategy);
        propertyValues.put("checkpoints", info.getCheckPoints());
        propertyValues.put("checkType", info.getCheckType());
        propertyValues.put("sceneName", info.getCategory());
        propertyValues.put("decision", info.getDecision());
        propertyValues.put("expire", SystemClock.getCurrentTimestamp() + info.getTtl() * 1000);
        propertyValues.put("remark", info.getRemark());
        propertyValues.put("variableValues", "");
        propertyValues.put("test", info.isTest());
        Map<String, Object> triggerValues = triggerEvent.genAllData();
        propertyValues.put("triggerValues", triggerValues);
        propertyValues.put("geo_province", triggerValues.getOrDefault("geo_province", "unknown"));
        propertyValues.put("geo_city", triggerValues.getOrDefault("geo_city", "unknown"));
        return new Event("__all__", "notice", key, System.currentTimeMillis(), 1.0, propertyValues);
    }

    public class NoticeInfoHolder implements Serializable {
        private final TriggerEvent triggerEvent;
        private final List<String> strategies;
        private final List<String> toProfile;

        public NoticeInfoHolder(TriggerEvent triggerEvent, List<String> strategies, List<String> toProfile) {
            this.triggerEvent = triggerEvent;
            this.strategies = strategies;
            this.toProfile = toProfile;
        }

        public TriggerEvent getTriggerEvent() {
            return triggerEvent;
        }

        public List<String> getStrategies() {
            return strategies;
        }

        public List<String> getToProfile() {
            return toProfile;
        }

        @Override
        public String toString() {
            return String.format("{triggerevent=%s, strategy=%s, to_profile=%s}", triggerEvent.toString(), strategies.toString(), toProfile.toString());
        }
    }
}
