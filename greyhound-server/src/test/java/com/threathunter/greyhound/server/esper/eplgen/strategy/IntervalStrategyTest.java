package com.threathunter.greyhound.server.esper.eplgen.strategy;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.GreyhoundServer;
import com.threathunter.greyhound.server.engine.EngineConfiguration;
import com.threathunter.greyhound.server.esper.EsperEPL;
import com.threathunter.greyhound.server.esper.eplgen.VariableEPLGenerator;
import com.threathunter.greyhound.server.esper.eplgen.VariableMetaTestHelper;
import com.threathunter.greyhound.tool.data.AccountLoginEventMaker;
import com.threathunter.greyhound.tool.data.EventMaker;
import com.threathunter.greyhound.tool.data.HttpDynamicEventMaker;
import com.threathunter.greyhound.tool.data.babel.service.BabelServiceReceiverHelper;
import com.threathunter.greyhound.tool.data.babel.service.NotifyReceiver;
import com.threathunter.model.*;
import com.threathunter.variable.DimensionType;
import org.junit.*;

import java.io.IOException;
import java.util.*;

import static com.threathunter.greyhound.server.esper.eplgen.GreyhoundMetaHelper.loadEvents;
import static com.threathunter.greyhound.server.esper.eplgen.GreyhoundMetaHelper.loadStrategies;
import static com.threathunter.greyhound.server.esper.eplgen.GreyhoundMetaHelper.loadVariables;

/**
 * Created by daisy on 17-12-27
 */
public class IntervalStrategyTest {
    private static NotifyReceiver notifyReceiver;

    private EventMaker httpDynamicMaker;
    private EventMaker accountLoginMaker;

    private GreyhoundServer greyhoundServer;

    @BeforeClass
    public static void setupGlobal() throws IOException {
        CommonDynamicConfig.getInstance().addOverrideProperty("greyhound.server.strategy.debug", true);
        CommonDynamicConfig.getInstance().addOverrideProperty("babel_server", "redis");
        CommonDynamicConfig.getInstance().addOverrideProperty("redis_port", 6379);

        VariableMetaRegistry.getInstance().updateVariableMetas(new ArrayList<>());
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();

        loadEvents("events.json");
        loadVariables("interval_variables.json");
        loadStrategies("interval_strategy.json");

        notifyReceiver = BabelServiceReceiverHelper.getInstance().createSimpleGetEventReceiver("NoticeNotify_redis.service");
        BabelServiceReceiverHelper.getInstance().start();
    }

    @AfterClass
    public static void tearDownGlobal() {
        BabelServiceReceiverHelper.getInstance().stop();
    }

    @After
    public void tearDown() {
        greyhoundServer.stop();
    }

    @Before
    public void setup() {
        this.httpDynamicMaker = new HttpDynamicEventMaker(1);
        this.accountLoginMaker = new AccountLoginEventMaker(1);

        EngineConfiguration configuration = new EngineConfiguration();
        configuration.setBatchModeDimensions(null);
        configuration.setEnableDimensions(new HashSet<>(Arrays.asList(DimensionType.IP, DimensionType.UID, DimensionType.DID)));
        configuration.setRedisBabel(true);
        configuration.setShardCount(6);
        configuration.setThreadCount(5);

        List<VariableMeta> metas = VariableMetaRegistry.getInstance().getAllVariableMetas();
        greyhoundServer = new GreyhoundServer(configuration, metas);
        greyhoundServer.start();
    }

    @Test
    public void testInterval() throws InterruptedException {
        // ip trigger: contains .  ACCOUNT_LOGIN
        // did trigger: did != ""  ACCOUNT_LOGIN
        // did counter_2_1(last timestamp): contains captcha  HTTP_DYNAMIC
        // did counter_2_2(last timestamp)  did trigger
        // did counter_2_3(dual - timestamp)  counter_2_2  counter_2_1
        // did collector: counter_2_3.value < 2000  (did)trigger (did)counter_2_3
        String testDid = "mydid";

        Event httpEvent = this.httpDynamicMaker.nextEvent();
        httpEvent.getPropertyValues().put("did", testDid);
        httpEvent.getPropertyValues().put("page", "captcha");
        greyhoundServer.addEvent(httpEvent);

        Thread.sleep(500);

        Event loginEvent = this.accountLoginMaker.nextEvent();
        loginEvent.getPropertyValues().put("did", testDid);
        greyhoundServer.addEvent(loginEvent);

        Event notice = notifyReceiver.fetchNextEvent();
        boolean trigger = false;
        while (notice != null) {
            if (notice.getPropertyValues().get("strategyName").equals("interval_测试1")) {
                Map<String, Object> triggerMap = (Map) notice.getPropertyValues().get("triggerValues");
                if (triggerMap.get("did").equals(testDid)) {
                    trigger = true;
                }
            }
            notice = notifyReceiver.fetchNextEvent();
        }

        Assert.assertTrue(trigger);
    }
}
