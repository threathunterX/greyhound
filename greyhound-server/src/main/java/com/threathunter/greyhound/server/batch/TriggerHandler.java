package com.threathunter.greyhound.server.batch;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.utils.DaemonThread;
import com.threathunter.model.Event;
import com.threathunter.model.Property;
import com.threathunter.model.VariableMeta;
import com.threathunter.util.MetricsHelper;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by daisy on 17-11-10
 */
public class TriggerHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerHandler.class);
    private static final Logger ANALYSIS_LOGGER = LoggerFactory.getLogger("analytics");
    private volatile Map<String, Trigger> triggers;
    private volatile String[] triggerFields;

    private final BatchModeContainer container;
    private final ThreadPoolExecutor executor;

    private final boolean debug;
    private final Gson gson;

    public TriggerHandler(BatchModeContainer container, List<VariableMeta> metas) {
        this.container = container;
        this.executor = new ThreadPoolExecutor(1, 3, 1, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), DaemonThread.INSTANCE);
        this.updateTriggerFields(metas);
        this.triggers = new HashMap<>();

        this.debug = CommonDynamicConfig.getInstance().getBoolean("greyhound.server.strategy.debug", false);
        this.gson = new Gson();
    }

    public void updateTriggerFields(List<VariableMeta> metas) {
//        Set<String> fields = new HashSet<>();
//        metas.forEach(meta -> {
//            List<Property> groupBys = meta.getGroupKeys();
//            if (groupBys != null && groupBys.size() > 0) {
//                groupBys.forEach(p -> fields.add(p.getName()));
//            }
//        });
//
//        String[] tFields = new String[fields.size()];
//        int ind = 0;
//        for (String f : fields) {
//            tFields[ind] = f;
//            ind++;
//        }

        String[] tFields = new String[1];
        tFields[0] = "c_ip";
        this.triggerFields = tFields;
    }

    public void addTrigger(Event event) {
        String triggerCode = Trigger.getTriggerCode(this.triggerFields, event);
        if (!this.triggers.containsKey(triggerCode)) {
            this.triggers.put(triggerCode, new Trigger(event.getApp(), event.getName(), event.getKey(), event.getId(), triggerCode));
            MetricsHelper.getInstance().addMetrics("sliding.handler.events.add.count", 1.0, "name", event.getName());
            if (debug) {
                LOGGER.warn("add future trigger, code: " + triggerCode);
            }
        }
    }

    public void sendTriggers(){
        Map<String, Trigger> next = this.triggers;
        LOGGER.warn("ready to send triggers, size: " + next.size());
        this.triggers = new HashMap<>();
        this.executor.execute(() ->
            next.values().forEach(trigger -> {
                Event event = trigger.genEvent();
                this.container.sendTriggerEvent(event);
                MetricsHelper.getInstance().addMetrics("sliding.handler.events.send.count", 1.0, "name", event.getName());
                if (debug) {
                    ANALYSIS_LOGGER.info(gson.toJson(event));
                }
            }));
    }

    static class Trigger {
        private final String triggerCode;
        private final String app;
        private final String name;
        private final String id;
        private final String key;

        public Trigger(String app, String name, String key, String id, String triggerCode) {
            this.triggerCode = triggerCode;
            this.app = app;
            this.name = name;
            this.id = id;
            this.key = key;
        }

        public String getTriggerCode() {
            return triggerCode;
        }

        public Event genEvent() {
            return new Event(this.app, this.name, this.key, System.currentTimeMillis(), this.id,
                    null, 1.0, parseProperties(this.triggerCode));
        }

        private Map<String, Object> parseProperties(String triggerCode) {
            Map<String, Object> map = new HashMap<>();
            for (String ff : triggerCode.split(";;;")) {
                String[] fv = ff.split("@@@");
                map.put(fv[0], fv[1]);
            }

            return map;
        }

        static String getTriggerCode(String[] triggerFields, Event event) {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (String f : triggerFields) {
                if (first) {
                    sb.append(f).append("@@@").append(event.getPropertyValues().get(f)).append(";;;");
                    first = false;
                } else {
                    sb.append(f).append("@@@").append(event.getPropertyValues().get(f));
                }
            }

            return sb.toString();
        }
    }
}
