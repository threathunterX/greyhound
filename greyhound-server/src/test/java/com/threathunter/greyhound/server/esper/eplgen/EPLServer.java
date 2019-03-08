package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.common.Utility;
import com.threathunter.greyhound.server.esper.extension.EsperExtension;
import com.threathunter.model.Event;
import com.espertech.esper.client.*;
import com.espertech.esper.client.time.CurrentTimeEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by daisy on 17-10-13
 */
public class EPLServer {
    private EPServiceProvider epServiceProvider;
    private Consumer<Map<String, Object>> updateConsumer;
    private UpdateListener listener;
    private long lastSentTimestamp = -1;

    public EPLServer(String providerURI, Consumer<Map<String, Object>> updateEventConsumer, List<String> epls) {
        this.epServiceProvider = EPServiceProviderManager.getProvider(providerURI, buildEsperConfiguration());
        this.updateConsumer = updateEventConsumer;
        this.listener = ((newEvents, oldEvents) -> {
            if (newEvents != null && newEvents.length > 0) {
                this.updateConsumer.accept(new HashMap<>((Map<String, Object>) newEvents[0].getUnderlying()));
            }
        });

        EPAdministrator admin = epServiceProvider.getEPAdministrator();
        epls.forEach(epl -> {
            EPStatement statement = admin.createEPL(epl);
            statement.addListener(listener);
        });
    }

    public void addEvent(Event event) {
        long current = System.currentTimeMillis();
            if (current - lastSentTimestamp > 0.1) {
                epServiceProvider.getEPRuntime().sendEvent(new CurrentTimeEvent(current));
                lastSentTimestamp = current;
            }
        epServiceProvider.getEPRuntime().sendEvent(event.genAllData(), event.getName());
    }

    private static Configuration buildEsperConfiguration() {
        Configuration configuration = new Configuration();
        configuration.getEngineDefaults().getThreading().setInternalTimerEnabled(true);
        configuration.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(true);

        configuration.getEngineDefaults().getExecution().setDisableLocking(true);
        configuration.getEngineDefaults().getThreading().setInternalTimerEnabled(false);
        configuration.getEngineDefaults().getThreading().setThreadPoolInbound(false);
        configuration.getEngineDefaults().getThreading().setThreadPoolOutbound(false);
        configuration.getEngineDefaults().getThreading().setThreadPoolRouteExec(false);

        // addDimensionTrigger our extension on esper
        Utility.scannerSubTypeFromPackage("com.threathunter", EsperExtension.class).forEach(configuration::addImport);

        configuration.getEngineDefaults().getExecution().setPrioritized(true);
        return configuration;
    }
}
