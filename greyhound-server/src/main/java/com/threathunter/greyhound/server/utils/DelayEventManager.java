package com.threathunter.greyhound.server.utils;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.esper.EsperContainer;
import com.threathunter.model.Event;
import com.threathunter.util.MetricsHelper;
import com.threathunter.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Handler the events that need to resend to online server because of delay strategies.
 * If a strategy contains sleep function, it will be separated to two parts in online.
 * First parts will compute normally, but when the collector update, it will be send to here,
 * and wait for a period until it is consider to send to online server.
 *
 * One thing we need to consider is the number of events that to be delay, if one event is consider to delay
 * 1 hour, meanwhile too many same events comes, memory will be a problem. Currently, we will do cache to prevent
 * same key-strategy pair redundancy, like notice. Notice, strategy should not depends on event's field after sleep.
 *
 * @author daisy
 */
public class DelayEventManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayEventManager.class);
    private static final long DUPLICATE_MILLIS = 1000 * 60 * 5;

    private final EsperContainer container;
    private final DelayEventWorker worker;
    // add a thread for the storage to check events that should be send
    private final HashSet<String> existKeyStrategyPairSet;
    public DelayEventManager(final EsperContainer container) {
        this.container = container;
        this.worker = new DelayEventWorker();
        this.existKeyStrategyPairSet = new HashSet<>();
    }

    public void addDelayEvent(final Event event, final String strategyName) {
        StrategyInfoCache.StrategyInfo info = StrategyInfoCache.getInstance().getStrategyInfo(strategyName);
        if (info == null) {
            return;
        }
        Long checkTimestamp = info.getDelayMillis() / DUPLICATE_MILLIS * DUPLICATE_MILLIS;
        String checkStr = String.format("%s@@%s@@%d", strategyName,
                event.getPropertyValues().getOrDefault(info.getCheckValue(), ""), checkTimestamp);
        if (existKeyStrategyPairSet.contains(checkStr)) {
            MetricsHelper.getInstance().addMetrics("greyhound.engine.delay.exist.drop.count", 1.0);
            return;
        }

        existKeyStrategyPairSet.add(checkStr);

        event.setTimestamp(event.getTimestamp() + info.getDelayMillis());
        worker.addDelayEvent(event, checkStr, strategyName);
        MetricsHelper.getInstance().addMetrics("greyhound.engine.delay.add.count", 1.0);
    }

    public void start() {
        this.worker.start();
    }

    public void stop() {
        this.worker.stopWork();
        try {
            this.worker.join(1000);
        } catch (Exception e) {
            LOGGER.error("error happened when stopping delay worker", e);
        }
    }

    static class DelayEvent implements Delayed {

        private final Event event;
        private final String checkString;

        public DelayEvent(final Event originEvent, final String checkString, final String strategyName) {
            this.event = new Event(originEvent.getApp(), originEvent.getName() + "_DELAY", originEvent.getKey(),
                    originEvent.getTimestamp(), originEvent.getId(), originEvent.getPid(),
                    originEvent.value(), new HashMap<>(originEvent.getPropertyValues()));
            this.event.getPropertyValues().put("delay_strategy", strategyName);
            this.checkString = checkString;
        }

        @Override
        public long getDelay(final TimeUnit unit) {
            return unit.convert(this.event.getTimestamp() - SystemClock.getCurrentTimestamp(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(final Delayed o) {
            return (int) (this.event.getTimestamp() - ((DelayEvent) o).event.getTimestamp());
        }
    }

    class DelayEventWorker extends Thread {
        private volatile boolean running = false;
        private final DelayQueue<DelayEvent> delayQueue;

        public DelayEventWorker() {
            super("delay events send worker");
            this.setDaemon(true);
            this.delayQueue = new DelayQueue<>();
        }

        public void addDelayEvent(final Event event, final String checkString, final String strategyName) {
            if (this.delayQueue.size() >= CommonDynamicConfig.getInstance().getInt(
                    "greyhound.engine.delay.max.count", 10000)) {
                MetricsHelper.getInstance().addMetrics("greyhound.engine.delay.drop.count", 1.0);
                return;
            }
            this.delayQueue.offer(new DelayEventManager.DelayEvent(event, checkString, strategyName));
            MetricsHelper.getInstance().addMetrics("greyhound.engine.delay.add.count", 1.0);
        }

        public void stopWork() {
            this.running = false;
            try {
                this.join(1000);
            } catch (Exception e) {
            }
        }

        @Override
        public void run() {
            if (running) {
                LOGGER.warn("already running");
                return;
            }
            running = true;
            while (running) {
                try {
                    DelayEvent delayEvent = this.delayQueue.take();
                    transportEvent(delayEvent.event);
                    existKeyStrategyPairSet.remove(delayEvent.checkString);
                    MetricsHelper.getInstance().addMetrics("nebula.online.delay.resend.count", 1.0);
                    count++;
                } catch (Exception e) {
                    LOGGER.error("taking expire events error", e);
                }
            }
        }

        int count = 0;

        long getCount() {
            return count;
        }

        void transportEvent(final Event event) {
            try {
                event.setTimestamp(System.currentTimeMillis());
                container.sendEvent(event);
            } catch (Exception e) {
                LOGGER.error("[nebula:online:delayer]runtime: error in sending delay event", e);
            }
        }
    }
}

