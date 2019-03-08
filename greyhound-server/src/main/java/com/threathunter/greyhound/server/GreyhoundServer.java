package com.threathunter.greyhound.server;

import com.threathunter.greyhound.server.batch.BatchModeContainer;
import com.threathunter.greyhound.server.engine.EngineConfiguration;
import com.threathunter.greyhound.server.engine.RuleEngine;
import com.threathunter.greyhound.server.esper.EsperContainer;
import com.threathunter.greyhound.server.query.VariableQuery;
import com.threathunter.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by daisy on 17/6/26.
 */
public class GreyhoundServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(GreyhoundServer.class);
    private final RuleEngine ruleEngine;

    private volatile boolean running = false;
    private final Set<String> batchSlotDimension;

    public GreyhoundServer(final EngineConfiguration configuration, List<VariableMeta> sortedRuleMetas) {
//        this.initialVariableMetaType();
        if (configuration.getBatchModeDimensions() != null && !configuration.getBatchModeDimensions().isEmpty()) {
            this.batchSlotDimension = new HashSet<>();
            if (configuration.getBatchModeDimensions() != null) {
                configuration.getBatchModeDimensions().forEach(d -> this.batchSlotDimension.add(d.toString()));
            }
            this.ruleEngine = new BatchModeContainer(configuration, sortedRuleMetas);
        } else {
            this.batchSlotDimension = null;
            this.ruleEngine = new EsperContainer(configuration, false);
            this.ruleEngine.updateRules(VariableMetaWrapper.generateWrappers(sortedRuleMetas, null));
        }
    }

    public void start() {
        if (this.running) {
            LOGGER.warn("[greyhound:server] already running");
            return;
        }
        this.running = true;
        this.ruleEngine.start();
    }

    public void stop() {
        this.ruleEngine.stop();
    }

    public void addEvent(final Event event) {
        this.ruleEngine.sendEvent(event);
    }

    public List<String> getTriggerNoticeList(final String eventId) {
        if (batchSlotDimension != null) {
            return null;
        }
        return ((EsperContainer) this.ruleEngine).getTriggerNoticeList(eventId);
    }

    public Object query(String app, String variable, String key) {
        VariableMeta meta = VariableMetaRegistry.getInstance().getVariableMeta(app, variable);
        if (meta == null) {
            LOGGER.error(String.format("variable meta not found, app: %s, variable: %s", app, variable));
            return null;
        }

        String dimension = meta.getDimension();
        boolean keyTopValue = false;

        VariableQuery query;
        if (keyTopValue) {
//            query = VariableQueryUtil.sendKeyTopQuery(engine, variable, dimension, key, 5);
        } else {
//            query = VariableQueryUtil.sendKeyQuery(engine, variable, dimension, key);
        }
//        return query.waitForResults(500, TimeUnit.MILLISECONDS);
        return null;
    }

    public void updateRules(List<VariableMeta> metas) {
        this.ruleEngine.updateRules(VariableMetaWrapper.generateWrappers(metas, this.batchSlotDimension));
    }

    public boolean isIdle() {
        return this.ruleEngine.isIdle();
    }

    private void initialVariableMetaType() {
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();
    }
}
