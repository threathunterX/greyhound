package com.threathunter.greyhound.server.esper.listeners;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.esper.EsperDimensionService;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.event.map.MapEventBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * If a event trigger a strategy's condition meet in a dimension,
 * this listener will catch it and write the information in the thread local of {@code EsperDimensionService}
 * format is: strategyName@@dimension
 * @author daisy
 */
public class StrategyDimensionTriggerListener extends EsperEventListener {
    private static final Logger LOGGER = LoggerFactory.getLogger("analytics");
    private final EsperDimensionService esperDimensionService;
    private final boolean debug;
    private final boolean debugAll;
    private final boolean collectorOnly;
    private final Set<String> debugNamesSet;

    /**
     * This listener need to know its dimension service, for the schedule of events
     * and addDimensionTrigger notice to the local thread.
     * @param service the esper dimension service that this listener belongs to
     */
    public StrategyDimensionTriggerListener(EsperDimensionService service) {
        super("dimension collector");
        this.esperDimensionService = service;
        this.debug = CommonDynamicConfig.getInstance().getBoolean("greyhound.server.strategy.debug", false);
        this.collectorOnly = CommonDynamicConfig.getInstance().getBoolean("greyhound.server.strategy.debug.collector.only", false);
        if (debug) {
            String[] debugNames = CommonDynamicConfig.getInstance().getStringArray("greyhound.server.strategy.debug.variable.names");
            if (debugNames != null && debugNames.length > 0) {
                debugNamesSet = new HashSet<>();
                for (String s : debugNames) {
                    debugNamesSet.add(s);
                }
                debugAll = false;
            } else {
                debugAll = true;
                debugNamesSet = null;
            }
        } else {
            debugAll = false;
            debugNamesSet = null;
        }
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (debug) {
            if (newEvents != null && newEvents.length > 0) {
                for (EventBean bean : newEvents) {
                    logDebugInfo(bean);
                }
            }
        }

        if (newEvents != null && newEvents.length > 0
                && newEvents[0].getEventType().getName().contains("collect")) {
            for (EventBean bean : newEvents) {
                try {
                    String strategyName = (String) bean.get("strategyName");
                    String triggerId;
                    if (bean.getEventType().getName().contains("delayer")) {
                        triggerId = String.format("%s@@%s@@delay", strategyName, esperDimensionService.getDimension());
                    } else {
                        triggerId = String.format("%s@@%s", strategyName, esperDimensionService.getDimension());
                    }
                    esperDimensionService.getNoticesInThread().get().add(triggerId);
                } catch (Exception ex) {
                    processError("fail to process alert " + bean, ex);
                }
            }
        }
    }

    private void logDebugInfo(EventBean eventBean) {
        String name = eventBean.getEventType().getName();
        if (collectorOnly) {
            if (name.contains("collect")) {
                Map<String, Object> properties = ((MapEventBean) eventBean).getProperties();
                LOGGER.info(String.format("strategy name: %s, dimension: %s", properties.get("strategyName"), esperDimensionService.getDimension()));
            }
            return;
        }
        if (!debugAll) {
            if (!debugNamesSet.contains(name)) {
                return;
            }
        }
        Map<String, Object> properties = ((MapEventBean) eventBean).getProperties();
        LOGGER.info(String.format("name: %s, key: %s, value: %s",
                    name, properties.get("key") == null? "null" : properties.get("key").toString(),
                    properties.get("value") == null? "null" : properties.get("value").toString()));
    }

    @Override
    public boolean isOnlineListener() {
        return true;
    }
}
