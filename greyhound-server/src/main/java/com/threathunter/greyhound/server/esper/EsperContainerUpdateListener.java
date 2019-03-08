package com.threathunter.greyhound.server.esper;

import com.threathunter.greyhound.server.esper.listeners.EsperEventListener;
import com.threathunter.util.MetricsHelper;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The entry point of the processor for esper generated events.
 *
 * @author Wen Lu
 */
public class EsperContainerUpdateListener implements UpdateListener {
    private static final Logger logger = LoggerFactory.getLogger(EsperContainerUpdateListener.class);

    private final List<EsperEventListener> onlineListeners = new ArrayList<>();
    private final List<EsperEventListener> offlineListeners = new ArrayList<>();

    private final Thread[] workers;
    private final BlockingQueue<EventBean[]> cache;
    private volatile boolean running = false;

    public EsperContainerUpdateListener(String name, List<EsperEventListener> listeners, int nThreads, int capacity) {
        String name1 = name;
        for (EsperEventListener l : listeners) {
            if (l.isOnlineListener()) {
                onlineListeners.add(l);
            } else {
                offlineListeners.add(l);
            }

        }

        workers = new Thread[nThreads];
        for (int i = 0; i < nThreads; i++) {
            workers[i] = new EsperOutputEventProcessor(name + ":" + "esperoutputworker" + i);
        }
        this.cache = new ArrayBlockingQueue<>(capacity);
    }

    public void start() {
        if (running) {
            logger.error("esper output worker has already started");
            return;
        }

        for (int i = 0; i < workers.length; i++) {
            workers[i].start();
        }

        running = true;
    }

    public void stop() {
        if (!running) {
            logger.error("esper output worker is not running now");
            return;
        }

        running = false;

        for (int i = 0; i < workers.length; i++) {
            try {
                workers[i].join(3000);
            } catch (InterruptedException ignore) {
                logger.error("esper output worker {} fail to close ", i);
            }
        }
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (!running) {
            return;
        }

        if (newEvents == null || newEvents.length == 0) {
            return;
        }

        for (EsperEventListener l : onlineListeners) {
            try {
                l.update(newEvents, oldEvents);
            } catch (Exception ex) {
                logger.error("fail to execute online update listener {}", l.getName());
                ex.printStackTrace();
            }
        }

        boolean success = cache.offer(newEvents);
        if (!success) {
            MetricsHelper.getInstance().addMetrics("nebula.esper.listener.dropevents", (double) newEvents.length);
        }
    }

    private class EsperOutputEventProcessor extends Thread {

        private final String name;

        public EsperOutputEventProcessor(String name) {
            super(name);
            this.name = name;
        }

        public void run() {
            while (running) {
                EventBean[] newEvents;
                try {
                    newEvents = cache.poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.error("esper output thread " + name + " meets error while polling update", e);
                    break;
                }

                if (newEvents == null || newEvents.length == 0) {
                    continue;
                }

                for (EsperEventListener l : offlineListeners) {
                    try {
                        l.update(newEvents, null);
                    } catch (Exception e) {
                        logger.error("esper output thread " + name + " meets error while processing update", e);
                        break;
                    }
                }
            }

            // deal with the remaining events
            List<EventBean[]> remainingEvents = new ArrayList<>();
            cache.drainTo(remainingEvents);

            for (EventBean[] r : remainingEvents) {
                for (EsperEventListener l : offlineListeners) {
                    try {
                        l.update(r, null);
                    } catch (Exception e) {
                        logger.error("esper output thread " + name + " meets error while processing remaining update", e);
                        break;
                    }
                }
            }
        }
    }
}
