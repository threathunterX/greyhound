package com.threathunter.greyhound.server.esper.eplgen.strategy;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.GreyhoundServer;
import com.threathunter.greyhound.server.engine.EngineConfiguration;
import com.threathunter.greyhound.tool.data.AccountLoginEventMaker;
import com.threathunter.greyhound.tool.data.AccountRegistrationEventMaker;
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
public class MultiDimensionStrategyTest {
    private static NotifyReceiver notifyReceiver;
    private static NotifyReceiver profileReceiver;

    private GreyhoundServer greyhoundServer;
    private HttpDynamicEventMaker dynamicEventMaker;
    private AccountRegistrationEventMaker registrationEventMaker;
    private AccountLoginEventMaker loginEventMaker;

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
        loadVariables("mdimension_variables.json");
        loadStrategies("mdimension_strategy.json");

        notifyReceiver = BabelServiceReceiverHelper.getInstance().createSimpleGetEventReceiver("NoticeNotify_redis.service");
        profileReceiver = BabelServiceReceiverHelper.getInstance().createSimpleGetEventReceiver("ProfileNoticeChecker_redis.service");
        BabelServiceReceiverHelper.getInstance().start();
    }

    private void setupRealMode() {
        EngineConfiguration configuration = new EngineConfiguration();
        configuration.setBatchModeDimensions(null);
        configuration.setEnableDimensions(new HashSet<>(Arrays.asList(DimensionType.IP, DimensionType.UID, DimensionType.DID)));
        configuration.setRedisBabel(true);
        configuration.setShardCount(6);
        configuration.setThreadCount(5);

        dynamicEventMaker = new HttpDynamicEventMaker(10);
        registrationEventMaker = new AccountRegistrationEventMaker(10);
        loginEventMaker = new AccountLoginEventMaker(10);

        List<VariableMeta> metas = VariableMetaRegistry.getInstance().getAllVariableMetas();
        greyhoundServer = new GreyhoundServer(configuration, metas);
        greyhoundServer.start();
    }

    @Test
    public void test404HVisit() throws InterruptedException {
        setupRealMode();
        String testIP = "1.1.1.1";
        String testDID = "did1";
        // ip trigger: status 404 httpdynamic
        // ip counter1: status 404 httpdynamic > 10
        // did trigger: status 404 httpdynamic
        // did did__account_regist_count__5m__rt > 10

        for (int i = 0; i < 11; i++) {
            Event event = registrationEventMaker.nextEvent();
            event.getPropertyValues().put("did", testDID);
            event.getPropertyValues().put("uid", "");
            greyhoundServer.addEvent(event);
        }

        for (int i = 0; i < 10; i++) {
            Event event = dynamicEventMaker.nextEvent();
            event.getPropertyValues().put("c_ip", testIP);
            event.getPropertyValues().put("status", new Long(404));
            event.getPropertyValues().put("uid", "");
            greyhoundServer.addEvent(event);
        }

        // trigger
        Event event = dynamicEventMaker.nextEvent();
        event.getPropertyValues().put("c_ip", testIP);
        event.getPropertyValues().put("did", testDID);
        event.getPropertyValues().put("status", new Long(404));
        event.getPropertyValues().put("uid", "");
        greyhoundServer.addEvent(event);

        Thread.sleep(1000);

        Event notice = notifyReceiver.fetchNextEvent();
        boolean trigger = false;
        while (notice != null) {
            if (notice.getPropertyValues().get("strategyName").equals("visit_404_H_regist_H_count_did")) {
                Map<String, Object> triggerMap = (Map) notice.getPropertyValues().get("triggerValues");
                if (triggerMap.get("c_ip").equals(testIP) && triggerMap.get("did").equals(testDID)) {
                    trigger = true;
                }
            }
            notice = notifyReceiver.fetchNextEvent();
        }

        Assert.assertTrue(trigger);
        greyhoundServer.stop();
    }

    @Test
    public void testCeLve() throws InterruptedException {
        setupRealMode();

        String testIP = "172.16.1.1";
        String testDID = "did1";
        // ip trigger: c_ip contains 172.16; useragent not match iphone|ipod|android|ios|phone|ipad; account_login
        // ip counter4: platform == 9 or 10 > 1; account_login
        // ip ip__account_login_count__5m__rt > 1; ip__visit_dynamic_count__5m__rt > 2
        // did trigger： did != ""; account_login
        // did did__visit_dynamic_count__5m__rt > 2
        for (int i = 0; i < 3; i++) {
            Event event = dynamicEventMaker.nextEvent();
            event.getPropertyValues().put("c_ip", testIP);
            event.getPropertyValues().put("did", testDID);
            greyhoundServer.addEvent(event);
        }

        for (int i = 0; i < 2; i++) {
            Event event = loginEventMaker.nextEvent();
            event.getPropertyValues().put("c_ip", testIP);
            event.getPropertyValues().put("platform", "9");
            event.getPropertyValues().put("did", testDID);
            greyhoundServer.addEvent(event);
        }

        Thread.sleep(1000);

        Event notice = profileReceiver.fetchNextEvent();
        boolean trigger = false;
        while (notice != null) {
            if (((List)notice.getPropertyValues().get("strategylist")).contains("策略")) {
                Map<String, Object> triggerMap = notice.getPropertyValues();
                if (triggerMap.get("c_ip").equals(testIP) && triggerMap.get("did").equals(testDID)) {
                    trigger = true;
                }
            }
            notice = profileReceiver.fetchNextEvent();
        }

        Assert.assertTrue(trigger);
        greyhoundServer.stop();
    }
}
