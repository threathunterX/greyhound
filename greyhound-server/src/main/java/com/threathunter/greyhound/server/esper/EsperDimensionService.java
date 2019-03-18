package com.threathunter.greyhound.server.esper;

import com.threathunter.greyhound.server.StrategyTriggerManager;
import com.threathunter.greyhound.server.esper.listeners.*;
import com.threathunter.greyhound.server.query.VariableGlobalValueQuery;
import com.threathunter.greyhound.server.query.VariableKeyTopValueQuery;
import com.threathunter.greyhound.server.query.VariableValueQuery;
import com.threathunter.model.Event;
import com.threathunter.util.MetricsHelper;
import com.threathunter.util.SystemClock;
import com.threathunter.variable.DimensionType;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.time.CurrentTimeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 
 */
public class EsperDimensionService {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsperDimensionService.class);
    private static final Logger ANALYSIS_LOGGER = LoggerFactory.getLogger("analytics");
    private EsperEPLManager manager;

    private final String dimension;
    private final String dimensionCheckField;
    private final String name;

    private final int instanceCount;
    private final int listenerCount;
    private final int capacity;
    private List<EPServiceProvider> esperProviders;
    private List<EsperEventSenderTask> senders;

    private final StrategyTriggerManager strategyTriggerManager;

    private volatile boolean running = false;

    public ThreadLocal<List<String>> noticesInThread = ThreadLocal.withInitial(ArrayList::new);

    public ThreadLocal<Map<String, Double>> variablesInThread = ThreadLocal.withInitial(HashMap::new);

    public int getInstanceCount() {
        return instanceCount;
    }

    public String getDimension() {
        return this.dimension;
    }

    public EsperDimensionService(Configuration configuration, String dimension, StrategyTriggerManager manager) {
        this(configuration, dimension, 1, 1, 10000, manager);
    }

    public EsperDimensionService(Configuration configuration, String dimension, int shardCount, int listenerCount, int capacity, StrategyTriggerManager manager) {
        this.dimension = dimension;
        this.dimensionCheckField = DimensionType.getDimension(dimension).getFieldName();
        this.name = String.format("%s_%s", "greyhound", dimension);
        this.instanceCount = shardCount;
        this.listenerCount = listenerCount;
        this.capacity = capacity;
        this.esperProviders = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++) {
            EPServiceProvider provider = EPServiceProviderManager.getProvider(name + "-" + i, configuration);
            provider.initialize();
            esperProviders.add(provider);
        }
        this.strategyTriggerManager = manager;

        UpdateListener listener = buildListeners();
        this.manager = new EsperEPLManager(esperProviders, listener);
    }

    public void updateEpls(List<EsperEPL> epls) {
        this.manager.updateRules(epls);
    }

    public List<EsperEPL> getRunningEPL() {
        if (manager != null) {
            return manager.listRules();
        } else {
            return new ArrayList<>();
        }
    }

    public ThreadLocal<List<String>> getNoticesInThread() {
        return this.noticesInThread;
    }

    public ThreadLocal<Map<String, Double>> getVariablesInThread() {
        return this.variablesInThread;
    }

    public void start() {
        this.running = true;

        senders = new ArrayList<>();
        for (int i = 0; i < instanceCount; i++) {
            senders.add(new EsperEventSenderTask(String.format("espersender-%s-%s", name, i), esperProviders.get(i), capacity));
        }

        senders.forEach(Thread::start);
    }

    public void stop() {
        if (!running) {
            return;
        }
        this.running = false;

        esperProviders.forEach(provider -> {
            provider.getEPAdministrator().destroyAllStatements();
            provider.destroy();
        });
        esperProviders = new ArrayList<>();
        manager = null;

        senders.forEach(sender -> {
            try {
                sender.join(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        senders = null;
    }

    public EsperContainerUpdateListener buildListeners() {
        EsperEventListener valueListener = new VariableValueListener();
        EsperEventListener topListener = new TopValueListener();
        EsperEventListener keyTopListener = new VariableKeyTopValueListener();
        EsperEventListener alertListener = new StrategyDimensionTriggerListener(this);
        EsperEventListener statsListener = new StatsListerner();
        EsperContainerUpdateListener esperListener = new EsperContainerUpdateListener(name, Arrays.asList(valueListener, topListener,
                keyTopListener, alertListener, statsListener), this.listenerCount, this.capacity);
        LOGGER.warn("start:build esper listener on {}: {} threads and {} capacity", name, this.listenerCount, capacity);
        esperListener.start();
        return esperListener;
    }

    public boolean sendEvent(Event e, String key) {
        int bucket;
        if (key == null) {
            bucket = "".hashCode() % this.instanceCount;
        } else {
            bucket = key.hashCode() % this.instanceCount;
        }
        if (bucket < 0)
            bucket *= -1;

        LOGGER.debug("sending event {} to bucket {}", e.getName(), bucket);
        return this.senders.get(bucket).addEvent(e);
    }

    public void sendQueryEvent(Event e, String key) {
        int bucket;
        if (key == null) {
            bucket = "".hashCode() % this.instanceCount;
        } else {
            bucket = key.hashCode() % this.instanceCount;
        }
        if (bucket < 0)
            bucket *= -1;

        LOGGER.debug("sending event {} to bucket {}", e.getName(), bucket);
        this.senders.get(bucket).addQueryEvent(e);
    }


    public void broadcastEvent(final Event e) {
        for (EsperEventSenderTask sender : senders) {
            sender.addEvent(e);
        }
    }

    public boolean isIdle() {
        for (EsperEventSenderTask senderTask : this.senders) {
            if (!senderTask.isIdle()) {
                return false;
            }
        }
        return true;
    }

    private class EsperEventSenderTask extends Thread {
        private final String name;
        private final BlockingDeque<Event> queue;
        private final EPServiceProvider provider;
        private long lastSentTimestamp = 0;

        public EsperEventSenderTask(String name, final EPServiceProvider provider, int capacity) {
            super(name);
            this.name = name;
            this.queue = new LinkedBlockingDeque<>(capacity);
            this.provider = provider;
        }

        public boolean isIdle() {
            return this.queue.isEmpty();
        }

        @Override
        public void run() {
            int idle = 0;
            while (running) {
                List<Event> events = new ArrayList<>();
                this.queue.drainTo(events);
                if (events.isEmpty()) {
                    idle++;
                    if (idle >= 3) {
                        try {
                            Thread.sleep(100);
                        } catch (Exception e) {
                            LOGGER.error("error in waiting for esper events", e);
                        }
                    }
                } else {
                    idle = 0;
                    events.forEach(event -> {
                        try {
                            dealWithEvent(event);
                            MetricsHelper.getInstance().addMetrics("sliding.events.process.count", 1.0,
                                    "name", event.getName(), "shard", name, "dimension", dimension);
                        } catch (Exception e) {
                            LOGGER.error("process error", e);
                            MetricsHelper.getInstance().addMetrics("sliding.events.process.error.count", 1.0,
                                    "shard", name, "dimension", dimension);
                        }
                    });
                }
            }
        }

        public boolean addEvent(final Event event) {
            if (this.queue.offer(event)) {
                MetricsHelper.getInstance().addMetrics("sliding.events.offer.count", 1.0,
                        "name", event.getName(), "shard", name, "dimension", dimension);
                return true;
            } else {
                MetricsHelper.getInstance().addMetrics("sliding.events.drop.count", 1.0,
                        "name", event.getName(), "shard", name, "dimension", dimension);
                return false;
            }
        }

        public void addQueryEvent(final Event event) {
            this.queue.addFirst(event);
        }

        private void dealWithEvent(final Event e) {
            if (((String) e.getPropertyValues().getOrDefault(dimensionCheckField, "")).isEmpty()) {
                return;
            }
            Map<String, Object> eMap = e.genAllData();
            Map<String, Object> newMap = new HashMap<>();

            // build map object and reduce string memory usage
            for (Map.Entry<String, Object> entry : eMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                key = getSharedString(key);
                if (value instanceof String) {
                    value = getSharedString((String) value);
                }
                newMap.put(key, value);
            }

            long current = SystemClock.getCurrentTimestamp();
            if (current - lastSentTimestamp > 0.1) {
                provider.getEPRuntime().sendEvent(new CurrentTimeEvent(current));
                lastSentTimestamp = current;
            }

            // clean the notices generated in last call.
//            noticesInThread.get().clear();
            provider.getEPRuntime().sendEvent(newMap, e.getName());

            String eventName = e.getName();
            if (eventName.equals("_global__variablekeyvalue_request")) {
                if (e.getKey().equals("")) {
                    VariableGlobalValueQuery q = VariableGlobalValueQuery.getQuery((String) e.getPropertyValues().get("requestid"));
                    if (q != null) {
                        q.countdown();
                    }
                } else {
                    VariableValueQuery q = VariableValueQuery.getQuery((String) e.getPropertyValues().get("requestid"));
                    if (q != null) {
                        q.countdown();
                    }
                }
            } else if (eventName.equals("_global__variablekeytopvalue_request")) {
                VariableKeyTopValueQuery q = VariableKeyTopValueQuery.getQuery((String) e.getPropertyValues().get("requestid"));
                if (q != null) {
                    q.countdown();
                }
            }

            if (noticesInThread.get().size() > 0 && !e.getName().startsWith("_")) {
                List<String> noticesInThreads = new ArrayList<>(noticesInThread.get());
                ANALYSIS_LOGGER.warn("add check 1, id:{}, dimension:{}, noticesInThreads: {}, event:{} ",e.getId(),dimension,noticesInThreads,e);
                strategyTriggerManager.addDimensionTrigger(String.format("%s_%s", dimension, e.getId()), noticesInThreads);
                noticesInThread.get().clear();
            }
            strategyTriggerManager.addCheck(e.getId(), e);
        }

        private String getSharedString(String origin) {
            return origin.intern();
        }
    }
}
