package com.threathunter.greyhound.server.esper.eplgen.strategy;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.GreyhoundServer;
import com.threathunter.greyhound.server.engine.EngineConfiguration;
import com.threathunter.greyhound.tool.data.HttpDynamicEventMaker;
import com.threathunter.greyhound.tool.data.babel.service.BabelServiceReceiverHelper;
import com.threathunter.greyhound.tool.data.babel.service.NotifyReceiver;
import com.threathunter.model.*;
import com.threathunter.variable.DimensionType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static com.threathunter.greyhound.server.esper.eplgen.GreyhoundMetaHelper.*;

/**
 * 
 */
public class DelayStrategyTest {
    private static NotifyReceiver notifyReceiver;

    private GreyhoundServer greyhoundServer;
    private HttpDynamicEventMaker eventMaker;

    @BeforeClass
    public static void globalSetup() throws IOException {
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();
        CommonDynamicConfig.getInstance().addOverrideProperty("babel_server", "redis");
        CommonDynamicConfig.getInstance().addOverrideProperty("greyhound.server.strategy.debug", true);

        VariableMetaRegistry.getInstance().updateVariableMetas(new ArrayList<>());
        loadEvents("events.json");
        loadVariables("delay_variables.json");
        loadStrategies("delay_strategy.json");

        notifyReceiver = BabelServiceReceiverHelper.getInstance().createSimpleGetEventReceiver("NoticeNotify_redis.service");
        BabelServiceReceiverHelper.getInstance().start();
    }

    private void setupRealMode() {
        EngineConfiguration configuration = new EngineConfiguration();
        configuration.setBatchModeDimensions(null);
        configuration.setEnableDimensions(new HashSet<>(Arrays.asList(DimensionType.IP, DimensionType.UID, DimensionType.DID)));
        configuration.setRedisBabel(true);
        configuration.setShardCount(6);
        configuration.setThreadCount(5);

        eventMaker = new HttpDynamicEventMaker(10);

        List<VariableMeta> metas = VariableMetaRegistry.getInstance().getAllVariableMetas();
        greyhoundServer = new GreyhoundServer(configuration, metas);
        greyhoundServer.start();
    }

    @Test
    public void testTriggerDelayStrategy() throws InterruptedException {
        setupRealMode();
        String testUid = "user_1";
        // uid trigger: uri_stem contains A
        // sleep 60s
        // uid counter1: uri_stem contains B
        // uid collector: counter1 < 1

        Event event = eventMaker.nextEvent();
        event.getPropertyValues().put("uri_stem", "A");
        event.getPropertyValues().put("uid", testUid);
        greyhoundServer.addEvent(event);

        Thread.sleep(70 * 1000);

        Event notice = notifyReceiver.fetchNextEvent();
        System.out.println("ready to check notice");
        boolean trigger = false;
        while (notice != null) {
            if (notice.getPropertyValues().get("strategyName").equals("visit_page_A_no_B_user")) {
                Map<String, Object> triggerMap = (Map) notice.getPropertyValues().get("triggerValues");
                if (triggerMap.get("uid").equals(testUid)) {
                    trigger = true;
                }
            }
            notice = notifyReceiver.fetchNextEvent();
        }

        Assert.assertTrue(trigger);
        greyhoundServer.stop();
    }

    @Test
    public void testNotTriggerDelayStrategy() throws InterruptedException {
        setupRealMode();
        String testUid = "user_1";
        // uid trigger: uri_stem contains A
        // sleep 60s
        // uid counter1: uri_stem contains B
        // uid collector: counter1 < 1

        Event event = eventMaker.nextEvent();
        event.getPropertyValues().put("uri_stem", "A");
        event.getPropertyValues().put("uid", testUid);
        greyhoundServer.addEvent(event);
        Thread.sleep(10 * 1000);

        Event event1 = eventMaker.nextEvent();
        event1.getPropertyValues().put("uri_stem", "B");
        event1.getPropertyValues().put("uid", testUid);
        greyhoundServer.addEvent(event1);

        Thread.sleep(60 * 1000);

        Event notice = notifyReceiver.fetchNextEvent();
        boolean trigger = false;
        while (notice != null) {
            if (notice.getPropertyValues().get("strategyName").equals("visit_page_A_no_B_user")) {
                Map<String, Object> triggerMap = (Map) notice.getPropertyValues().get("triggerValues");
                if (triggerMap.get("uid").equals(testUid)) {
                    trigger = true;
                }
            }
            notice = notifyReceiver.fetchNextEvent();
        }

        Assert.assertFalse(trigger);
        greyhoundServer.stop();    }
}
