package com.threathunter.greyhound.server.esper;

import com.threathunter.greyhound.server.StrategyTriggerManager;
import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.greyhound.server.engine.EngineConfiguration;
import com.threathunter.greyhound.server.engine.RuleEngine;
import com.threathunter.greyhound.server.esper.eplgen.VariableEPLGenerator;
import com.threathunter.greyhound.server.esper.extension.EsperExtension;
import com.threathunter.model.Event;
import com.espertech.esper.client.Configuration;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.threathunter.common.Utility.scannerSubTypeFromPackage;

/**
 * Class for esper initialization and interaction.
 *
 * @author Wen Lu
 */
public class EsperContainer implements RuleEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsperContainer.class);

    /**
     * Esper configuration
     */
    private final boolean useInternalClock;
    private final boolean lossTolerant;
    private final Map<String, String> dimensionFields; // dimension and its related field name in events
    private final Map<String, EsperDimensionService> dimensionEsperServiceMap = new HashMap<>();

    /**
     * multiple instance
     */
    private final StrategyTriggerManager strategyTriggerManager;

    /**
     * Esper epl updater
     */
    private Thread ruleUpdater;

    /**
     * Container running state
     */
    private volatile boolean running = false;

    public EsperContainer(final EngineConfiguration configuration, final boolean batchModeEnable) {
        this.dimensionFields = new HashMap<>();
        configuration.getEnableDimensions().forEach(dimensionType -> this.dimensionFields.put(dimensionType.toString(), dimensionType.getFieldName()));
        this.lossTolerant = configuration.isLoseTolerant();
        this.useInternalClock = false;
        this.strategyTriggerManager = new StrategyTriggerManager(this, this.dimensionFields.keySet(), configuration.isRedisBabel(), batchModeEnable? -1 : configuration.getNoticeSyncExpireSeconds());

        Configuration conf = buildEsperConfiguration();
        this.dimensionFields.keySet().forEach(dimension -> dimensionEsperServiceMap.put(dimension,
                new EsperDimensionService(conf, dimension, configuration.getShardCount(), configuration.getThreadCount(), configuration.getCapacity(), this.strategyTriggerManager)));
    }

    public boolean isIdle() {
        for (EsperDimensionService service : this.dimensionEsperServiceMap.values()) {
            if (!service.isIdle()) {
                return false;
            }
        }
        return true;
    }

    public int getInstanceCount() {
        return this.dimensionEsperServiceMap.get("ip").getInstanceCount();
    }

    public void start() {
        running = true;

        this.dimensionEsperServiceMap.values().forEach(EsperDimensionService::start);
        this.strategyTriggerManager.start();
    }

    public List<String> getTriggerNoticeList(String eventId) {
        return this.strategyTriggerManager.getNoticeList(eventId);
    }

    private Configuration buildEsperConfiguration() {
        Configuration configuration = new Configuration();
        configuration.getEngineDefaults().getThreading().setInternalTimerEnabled(useInternalClock);
        configuration.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(true);

        // addDimensionTrigger our extension on esper
        scannerSubTypeFromPackage("com.threathunter", EsperExtension.class).forEach(configuration::addImport);

        configuration.getEngineDefaults().getExecution().setPrioritized(true);
        return configuration;
    }

    public void stop() {
        if (!running)
            return;
        running = false;
        this.dimensionEsperServiceMap.values().forEach(EsperDimensionService::stop);
    }

    public void updateRules(List<VariableMetaWrapper> wrappers) {
        LOGGER.warn("[greyhound:esper] try to update rules");
        Map<String, List<EsperEPL>> dimensionEplsMap = new HashMap<>();
        for (VariableMetaWrapper v : wrappers) {
            String dimension = v.getMeta().getDimension();
            if (dimension.isEmpty()) {
                /**
                 * 1. events like HTTP_DYNAMIC, it has no dimension
                 */
                for (String dim : this.dimensionFields.keySet()) {
                    dimensionEplsMap.computeIfAbsent(dim, d -> new ArrayList<>()).addAll(VariableEPLGenerator.getEPLs(v));
                }
            } else {
                /**
                 * 1. dimension variable
                 * 2. total variable meta will set dimension to global, and in esper it will go to ip dimension
                 */
                // rule engine may not need those global variables
                if (dimension.equals("global")) {
//                    dimensionEplsMap.computeIfAbsent("ip", d -> new ArrayList<>()).addAll(VariableEPLGenerator.genEPLFromVariable(v));
                } else {
                    dimensionEplsMap.computeIfAbsent(dimension, d -> new ArrayList<>()).addAll(VariableEPLGenerator.getEPLs(v));
                }
            }
        }

        MutableInt sum = new MutableInt(0);
        dimensionEplsMap.values().forEach(epls -> {
            Collections.sort(epls); sum.add(epls.size());
        });

        this.dimensionEsperServiceMap.forEach((dimension, service) -> service.updateEpls(dimensionEplsMap.get(dimension)));
    }

    public List<EsperEPL> getRunningEPL(String dimension) {
        return dimensionEsperServiceMap.get(dimension).getRunningEPL();
    }

    /**
     * Separate from query event, this events is for outside, processing real calculating,
     * may also sent to persistent except "HTTP_DYNAMIC_DELAY"
     * @param e
     */
    public void sendEvent(Event e) {
        /**
         * 1. For business events, need to send to all dimensions for calculating
         * 2. For rpc events, need to send to all dimensions to gather data
         *
         * Notice currently there is no consideration for query from esper.
         * We need to define new variable name pattern for query event in esper,
         * then we will create temp query var
         *
         * Notice, delay events like HTTP_DYNAMIC_DELAY will be sent to dimension
         * esper service
         */
        if (!lossTolerant) {
            this.dimensionFields.forEach((dimension, keyField) -> {
                String key = getShardingKey((String) e.getPropertyValues().get(keyField), dimension);
                if (key != null) {
                    EsperDimensionService esperDimensionService = this.dimensionEsperServiceMap.get(dimension);
                    while(!esperDimensionService.sendEvent(e, key)) {
                        try {
                            Thread.sleep(100);
                        } catch (Exception ex) {
                            LOGGER.error("interrupt waiting for event process", ex);
                            return;
                        }
                    }
                }
            });
        } else {
            this.dimensionFields.forEach((dimension, keyField) -> {
                String key = getShardingKey((String) e.getPropertyValues().get(keyField), dimension);
                if (key != null) {
                    this.dimensionEsperServiceMap.get(dimension).sendEvent(e, key);
                }
            });
        }
    }

    private String getShardingKey(final String rawKey, final String dimension) {
        if (rawKey == null) {
            return null;
        }
        String key = rawKey;
        if (dimension.equals("ip") && rawKey.contains(".")) {
            // use c_ipc as key
            key = rawKey.substring(0, rawKey.lastIndexOf('.'));
        }
        return key;
    }

    public void sendQuery(final Event event, final String dimension) {
        String shardingKey = event.getKey();
        String sendDimension = dimension;
        if (dimension.equals("global")) {
            sendDimension = "ip";
        }
        if (sendDimension.equals("ip") && shardingKey.contains(".")) {
            shardingKey = shardingKey.substring(0, shardingKey.lastIndexOf('.'));
        }
        this.dimensionEsperServiceMap.get(sendDimension).sendQueryEvent(event, shardingKey);
    }

    public void broadcastQueryEvent(final Event e, final String dimension) {
        // rpc events
        String sendDimension = dimension;
        if (dimension.equals("global")) {
            sendDimension = "ip";
        }
        this.dimensionEsperServiceMap.get(sendDimension).broadcastEvent(e);
    }
}
