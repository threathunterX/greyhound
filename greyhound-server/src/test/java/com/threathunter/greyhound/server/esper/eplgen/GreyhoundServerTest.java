package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.babel.rpc.RemoteException;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.GreyhoundServer;
import com.threathunter.greyhound.server.batch.BatchModeContainer;
import com.threathunter.greyhound.server.engine.EngineConfiguration;
import com.threathunter.greyhound.tool.data.HttpDynamicEventMaker;
import com.threathunter.greyhound.tool.data.babel.service.BabelServiceReceiverHelper;
import com.threathunter.greyhound.tool.data.babel.service.NotifyReceiver;
import com.threathunter.model.*;
import com.threathunter.variable.DimensionType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.*;

import static com.threathunter.greyhound.server.esper.eplgen.GreyhoundMetaHelper.*;

/**
 * Created by daisy on 17-11-15
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BatchModeContainer.SlotContainerHolder.class)
@PowerMockIgnore("javax.management.*")
public class GreyhoundServerTest {

    private GreyhoundServer greyhoundServer;
    private HttpDynamicEventMaker maker;
    private NotifyReceiver notice;

    @BeforeClass
    public static void globalSetup() throws IOException {
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();
        CommonDynamicConfig.getInstance().addOverrideProperty("babel_server", "redis");
        CommonDynamicConfig.getInstance().addOverrideProperty("greyhound.server.strategy.debug", true);

        // variable meta registry update is one by one update, first clear old ones
        VariableMetaRegistry.getInstance().updateVariableMetas(new ArrayList<>());
        loadEvents("events.json");
        loadVariables("variables.json");
        loadStrategies("strategy.json");
    }

    private void setupRealMode() {
        EngineConfiguration configuration = new EngineConfiguration();
        configuration.setBatchModeDimensions(null);
        configuration.setEnableDimensions(new HashSet<>(Arrays.asList(DimensionType.IP, DimensionType.UID, DimensionType.DID)));
        configuration.setRedisBabel(true);

        maker = new HttpDynamicEventMaker(10);
        notice = BabelServiceReceiverHelper.getInstance().createSimpleGetEventReceiver("NoticeNotify_redis.service");
        BabelServiceReceiverHelper.getInstance().start();

        List<VariableMeta> metas = VariableMetaRegistry.getInstance().getAllVariableMetas();
        greyhoundServer = new GreyhoundServer(configuration, metas);
        greyhoundServer.start();
    }

    private void setupBatchMode() {
        EngineConfiguration configuration = new EngineConfiguration();
        configuration.setBatchModeDimensions(new HashSet<>(Arrays.asList(DimensionType.IP)));
        configuration.setEnableDimensions(new HashSet<>(Arrays.asList(DimensionType.IP, DimensionType.UID, DimensionType.DID)));
        configuration.setBatchModeEventNames(new HashSet<>(Arrays.asList("HTTP_DYNAMIC")));
        configuration.setRedisBabel(true);

        maker = new HttpDynamicEventMaker(10);
        notice = BabelServiceReceiverHelper.getInstance().createSimpleGetEventReceiver("NoticeNotify_redis.service");
        BabelServiceReceiverHelper.getInstance().start();

        List<VariableMeta> metas = VariableMetaRegistry.getInstance().getAllVariableMetas();
        greyhoundServer = new GreyhoundServer(configuration, metas);
        greyhoundServer.start();
    }

    @Test
    public void testGreyhoundSlotBatchMode() throws InterruptedException {
        setupBatchMode();

        String testIP1 = "1.1.1.1";
        String testIP2 = "2.2.2.2";

//        BatchModeContainer.SlotContainerHolder holder = PowerMockito.mock(BatchModeContainer.SlotContainerHolder.class);
//        Whitebox.setInternalState(BatchModeContainer.SlotContainerHolder.class, "INSTANCE", holder);

//        PowerMockito.when(holder.querySlot("ip__dynamic_count", testIP1)).thenReturn(6);
//        PowerMockito.when(holder.querySlot("ip__dynamic_count", testIP2)).thenReturn(5);
        for (int i = 0; i < 5; i++) {
            Event event1 = maker.nextEvent();
            Event event2 = maker.nextEvent();
            if (i <= 2) {
                event1.getPropertyValues().put("method", "GET");
                event2.getPropertyValues().put("method", "GET");
            }
            event1.getPropertyValues().put("c_ip", testIP1);
            event1.getPropertyValues().put("page", event1.getId());
            event2.getPropertyValues().put("c_ip", testIP2);
            event2.getPropertyValues().put("page", event2.getId());
            greyhoundServer.addEvent(event1);
            greyhoundServer.addEvent(event2);
        }
        Event event1 = maker.nextEvent();
        event1.getPropertyValues().put("c_ip", testIP1);
        event1.getPropertyValues().put("page", event1.getId());
        greyhoundServer.addEvent(event1);

        Thread.sleep(1000 * 60);

//        for (int i = 0; i < 2; i++) {
//            Event event11 = maker.nextEvent();
//            Event event22 = maker.nextEvent();
//            if (i <= 2) {
//                event11.getPropertyValues().put("method", "GET");
//                event22.getPropertyValues().put("method", "GET");
//            }
//            event11.getPropertyValues().put("c_ip", testIP1);
//            event11.getPropertyValues().put("page", event11.getId());
//            event22.getPropertyValues().put("c_ip", testIP2);
//            event22.getPropertyValues().put("page", event22.getId());
//            greyhoundServer.addEvent(event11);
//            greyhoundServer.addEvent(event22);
//        }
//
//        Thread.sleep(1000 * 60);

        greyhoundServer.addEvent(maker.nextEvent());
        Thread.sleep(1000 * 20);

        Event event = notice.fetchNextEvent();
        boolean trigger1 = false;
        boolean trigger2 = false;
        while (event != null) {
            if (event.getPropertyValues().get("strategyName").equals("highvisit_strategy")) {
                if (((Map) event.getPropertyValues().get("triggerValues")).get("c_ip").equals(testIP1)) {
                    trigger1 = true;
                }
                if (((Map) event.getPropertyValues().get("triggerValues")).get("c_ip").equals(testIP2)) {
                    trigger2 = true;
                }
            }
            event = notice.fetchNextEvent();
        }

        Assert.assertTrue(trigger1);
//        Assert.assertFalse(trigger1);
        Assert.assertFalse(trigger2);

        greyhoundServer.stop();
    }

    @Test
    public void testGreyhoundRealtimeMode() throws RemoteException, InterruptedException {
        setupRealMode();

        String testIP1 = "1.1.1.1";
        String testIP2 = "2.2.2.2";

        for (int i = 0; i < 5; i++) {
            Event event1 = maker.nextEvent();
            Event event2 = maker.nextEvent();
            event1.getPropertyValues().put("c_ip", testIP1);
            event1.getPropertyValues().put("page", event1.getId());
            event2.getPropertyValues().put("c_ip", testIP2);
            event2.getPropertyValues().put("page", event2.getId());
            greyhoundServer.addEvent(event1);
            greyhoundServer.addEvent(event2);
        }

        Event event1 = maker.nextEvent();
        event1.getPropertyValues().put("c_ip", testIP1);
        event1.getPropertyValues().put("page", event1.getId());

        greyhoundServer.addEvent(event1);
        Thread.sleep(1000);

        Event event = notice.fetchNextEvent();
        boolean trigger = false;
        while (event != null) {
            if (event.getPropertyValues().get("strategyName").equals("highvisit_strategy")) {
                if (((Map) event.getPropertyValues().get("triggerValues")).get("c_ip").equals(testIP1)) {
                    trigger = true;
                }
            }
            event = notice.fetchNextEvent();
        }

        Assert.assertTrue(trigger);

        greyhoundServer.stop();
    }
}
