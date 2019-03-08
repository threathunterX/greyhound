package com.threathunter.greyhound.server.batch;

import com.threathunter.bordercollie.slot.compute.SlotEngine;
import com.threathunter.bordercollie.slot.compute.SlotQuery;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.common.Identifier;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.greyhound.server.engine.EngineConfiguration;
import com.threathunter.greyhound.server.engine.RuleEngine;
import com.threathunter.greyhound.server.esper.EsperContainer;
import com.threathunter.model.Event;
import com.threathunter.model.VariableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by daisy on 17-10-29
 */
public class BatchModeContainer implements RuleEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger("analytics");

    private final int windowSize;
    private final Set<String> batchModeDimensions;
    private final Set<String> batchModeEventNames;
    private final SlotEngine slotEngine;
    private final SlotQuery slotQuery;
    private final Deque<Long> winIdQueue;

    private long currentWindow = -1L;

    private final TriggerHandler triggerHandler;
    private final BatchSlotRuleEngineProcessTask processTask;
    private final EsperContainer esperContainer;

    private final boolean debug;

    private volatile boolean running = false;

    public BatchModeContainer(final EngineConfiguration configuration, List<VariableMeta> metas) {
        int slotWidth = configuration.getSlotWidthInMin();
        this.batchModeDimensions = new HashSet<>();
        configuration.getBatchModeDimensions().forEach(d -> this.batchModeDimensions.add(d.toString()));
        this.batchModeEventNames = configuration.getBatchModeEventNames();

        this.windowSize = 5;
        this.winIdQueue = new LinkedList<>();

        List<VariableMetaWrapper> wrappers = VariableMetaWrapper.generateWrappers(metas, batchModeDimensions);
        this.slotEngine = createSlotEngine(slotWidth, TimeUnit.MINUTES, configuration, wrappers);
        this.slotQuery = new SlotQuery(this.slotEngine);

        this.processTask = new BatchSlotRuleEngineProcessTask("greyhound batch task", CommonDynamicConfig.getInstance().getInt("greyhound.server.rule.engine.batch.capacity", 100000));
        this.triggerHandler = createTriggerHandler(wrappers);
        this.esperContainer = createEsperContainer(configuration, wrappers);

        SlotContainerHolder.getInstance().setBatchModeContainer(this);
        this.debug = CommonDynamicConfig.getInstance().getBoolean("greyhound.server.strategy.debug", false);
    }

    private TriggerHandler createTriggerHandler(List<VariableMetaWrapper> wrappers) {
        List<VariableMeta> triggerCares = new ArrayList<>();
        wrappers.forEach(wrapper -> {
            VariableMeta meta = wrapper.getMeta();
            if (this.batchModeDimensions.contains(meta.getDimension())) {
                triggerCares.add(meta);
            }
        });

        TriggerHandler handler = new TriggerHandler(this, triggerCares);
        return handler;
    }

    private SlotEngine createSlotEngine(int slotWidth, TimeUnit unit, EngineConfiguration configuration, List<VariableMetaWrapper> wrappers) {
        List<VariableMeta> slotVariables = new ArrayList<>();
        wrappers.forEach(wrapper -> {
            VariableMeta meta = wrapper.getMeta();
            if (meta.getDimension().isEmpty()) {
                slotVariables.add(wrapper.getMeta());
            } else {
                if (this.batchModeDimensions.contains(meta.getDimension())) {
                    if (!meta.getName().contains("trigger") && !meta.getName().contains("collect")) {
                        slotVariables.add(wrapper.getMeta());
                    }
                }
            }
        });
        return new SlotEngine(slotWidth, unit, configuration.getEnableDimensions(), slotVariables, StorageType.BYTES_ARRAY);
    }

    private EsperContainer createEsperContainer(EngineConfiguration configuration, List<VariableMetaWrapper> wrappers) {
        List<VariableMetaWrapper> slidingVariables = new ArrayList<>();
        wrappers.forEach(wrapper -> {
            VariableMeta meta = wrapper.getMeta();
            if (this.batchModeDimensions.contains(meta.getDimension())) {
                if (meta.getName().contains("trigger") || meta.getName().contains("collect")) {
                    slidingVariables.add(wrapper);
                }
            } else {
                slidingVariables.add(wrapper);
            }
        });

        EsperContainer container = new EsperContainer(configuration, true);
        container.updateRules(slidingVariables);
        return container;
    }

    @Override
    public void sendEvent(Event event) {
        this.processTask.addEvent(event);
    }

    public void sendTriggerEvent(Event event) {
        this.esperContainer.sendEvent(event);
    }

    @Override
    public void updateRules(List<VariableMetaWrapper> wrappers) {
        LOGGER.warn("[greyhound:batch_slot]try to update rules");
        List<VariableMeta> slotVariables = new ArrayList<>();
        List<VariableMetaWrapper> slidingVariables = new ArrayList<>();
        List<VariableMeta> triggerCares = new ArrayList<>();
        wrappers.forEach(wrapper -> {
            VariableMeta meta = wrapper.getMeta();
            if (meta.getDimension().isEmpty()) {
                slidingVariables.add(wrapper);
                slotVariables.add(wrapper.getMeta());
            } else {
                if (this.batchModeDimensions.contains(meta.getDimension())) {
                    if (!meta.getName().contains("trigger") && !meta.getName().contains("collect")) {
                        slotVariables.add(wrapper.getMeta());
                    } else {
                        slidingVariables.add(wrapper);
                    }
                    triggerCares.add(meta);
                } else {
                    slidingVariables.add(wrapper);
                }
            }
        });

        this.slotEngine.update(slotVariables);
        this.triggerHandler.updateTriggerFields(triggerCares);

        this.esperContainer.updateRules(slidingVariables);
    }

    @Override
    public void start() {
        if (running) {
            LOGGER.warn("already running");
            return;
        }
        this.running = true;
        this.slotEngine.start();
        this.esperContainer.start();
        this.processTask.start();
    }

    @Override
    public void stop() {
        if (!running) {
            LOGGER.warn("already stopped");
        }
        this.running = false;
        this.slotEngine.stop();
        this.esperContainer.stop();
        try {
            this.processTask.join(1000);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public boolean isIdle() {
        return false;
    }

    public double querySlot(String variableName, String... keys) {
        if (debug) {
            LOGGER.info(String.format("come to get slot variable value, strategy: %s, keys: %s", variableName, (keys == null && keys.length <= 0)? "null" : keys[0]));
        }
        Object data = this.slotQuery.mergePrevious(Identifier.fromKeys("nebula", variableName), keys[0]);
        if (data == null) {
            return 0.0;
        }
        if (debug) {
            LOGGER.info("value: " + ((Number) data).doubleValue());
        }
        return ((Number)data).doubleValue();
    }

    private class BatchSlotRuleEngineProcessTask extends Thread {
        private final String name;
        private final BlockingDeque<Event> queue;

        public BatchSlotRuleEngineProcessTask(String name, int capacity) {
            super(name);
            this.name = name;
            this.queue = new LinkedBlockingDeque<>(capacity);
        }

        @Override
        public void run() {
            int idle = 0;
            while (running) {
                List<Event> events = new ArrayList<>();
                this.queue.drainTo(events, 1000);
                if (events.isEmpty()) {
                    idle++;
                    if (idle >= 3) {
                        try {
                            Thread.sleep(100);
                            Event event = new Event();
                            event.setTimestamp(System.currentTimeMillis());
                            processEvent(event);
                        } catch (Exception e) {
                            LOGGER.error("interrupt");
                        }
                    }
                } else {
                    idle = 0;
                    events.forEach(event -> processEvent(event));
                }
            }
        }

        public void addEvent(Event event) {
            this.queue.add(event);
        }

        private void processEvent(Event event) {
            event.setTimestamp(System.currentTimeMillis());
            boolean dummy = false;
            if (event.getName() == null || event.getName().isEmpty()) {
                // dummy for change time window
                dummy = true;
            }
            // for batch slot
            if (dummy || batchModeEventNames.contains(event.getName())) {
                long slotId = slotEngine.add(event);
                if (slotId != currentWindow) {
                    // addDimensionTrigger window length and remove when length is more than 5
                    winIdQueue.addLast(currentWindow);
                    if (winIdQueue.size() > windowSize) {
                        Long id = winIdQueue.removeFirst();
                        slotEngine.removeSlot(id);
                    }

                    // send triggers
                    System.out.println("send triggers: " + System.currentTimeMillis());
                    triggerHandler.sendTriggers();
                    currentWindow = slotId;
                }
                if (!dummy) {
                    triggerHandler.addTrigger(event);
                }
            } else {
                // for sliding
                esperContainer.sendEvent(event);
            }
        }
    }

    public static class SlotContainerHolder {
        private volatile BatchModeContainer batchModeContainer;

        private static final SlotContainerHolder INSTANCE = new SlotContainerHolder();

        public static SlotContainerHolder getInstance() {
            return INSTANCE;
        }

        public void setBatchModeContainer(BatchModeContainer batchModeContainer) {
            this.batchModeContainer = batchModeContainer;
        }

        public double querySlot(String variableName, String... keys) {
            return this.batchModeContainer.querySlot(variableName, keys);
        }
    }
}
