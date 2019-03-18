package com.threathunter.greyhound.server.esper.eplgen.strategy;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.esper.EsperEPL;
import com.threathunter.greyhound.server.esper.eplgen.EPLServer;
import com.threathunter.greyhound.server.esper.eplgen.VariableEPLGenerator;
import com.threathunter.greyhound.server.esper.eplgen.VariableMetaTestHelper;
import com.threathunter.greyhound.tool.data.HttpDynamicEventMaker;
import com.threathunter.model.*;
import com.espertech.esper.event.map.MapEventBean;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 
 */
public class EPLStrategyTest {
    EPLServer server;
    HttpDynamicEventMaker eventMaker;

    @BeforeClass
    public static void globalSetup() {
        VariableMetaRegistry.getInstance().updateVariableMetas(new ArrayList<>());
        CommonDynamicConfig.getInstance().addOverrideProperty("greyhound.variable.meta.file", "StrategyVariables.json");
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();
    }

    @Before
    public void setup() {
        eventMaker = new HttpDynamicEventMaker(10);
    }

    @Test
    public void testHighVisit() throws InterruptedException {
        MutableBoolean assertFlag = new MutableBoolean(false);
        String testIP = "1.1.1.1";

        Consumer<Map<String, Object>> assertConsumer = (map) -> {
            if (map.containsKey("name") && ((String) map.get("name")).contains("collect")) {
                String triggerPage = (String) ((MapEventBean) map.get("triggerevent")).getProperties().get("uid");
                System.out.println(triggerPage + map.get("name").toString());
            }
            if (map.containsKey("name") && map.get("name").equals("__highvisit_collector__")) {
                Assert.assertEquals(testIP, map.get("key"));
                assertFlag.setTrue();
            }
        };
        server = new EPLServer("highvisit", assertConsumer, getEPLs());
        for (int i = 0; i < 5; i++) {
            Event event = eventMaker.nextEvent();
            event.getPropertyValues().put("c_ip", testIP);
            event.getPropertyValues().put("uid", event.getId());
            Thread.sleep(100);
            server.addEvent(event);
        }

        Assert.assertFalse(assertFlag.getValue());

        Event event = eventMaker.nextEvent();
        event.getPropertyValues().put("c_ip", testIP);
        event.getPropertyValues().put("uid", event.getId());
        server.addEvent(event);

        Thread.sleep(100);
        Assert.assertTrue(assertFlag.getValue());
    }

    @Test
    public void testClickTooFast() throws InterruptedException {
        MutableBoolean assertFlag = new MutableBoolean(false);
        String testIP = "1.1.1.1";

        server = new EPLServer("clicktoofast", (map) -> {
            if (map.get("name").equals("ip_click_diff")){
                System.out.println(map);
            }
            if (map.containsKey("name") && map.get("name").equals("__clicktoofast_collector__")) {
                Assert.assertEquals(testIP, map.get("key"));
                assertFlag.setTrue();
            }
        }, getEPLs());
        for (int i = 0; i < 3; i++) {
            if (i == 1) {
                Thread.sleep(1200);
            }
            if (i == 2) {
                Assert.assertFalse(assertFlag.getValue());
                Thread.sleep(100);
            }
            System.out.println("send: " + (i+1));
            Event event = eventMaker.nextEvent();
            System.out.println("send time: " + event.getTimestamp());
            event.getPropertyValues().put("c_ip", testIP);
            event.getPropertyValues().put("method", "POST");
            server.addEvent(event);
        }

        Thread.sleep(500);
        Assert.assertTrue(assertFlag.getValue());
    }

    @Test
    public void testVisitPostHighRatio() throws InterruptedException {
        MutableBoolean assertFlag = new MutableBoolean(false);
        String testIP = "1.1.1.1";
        server = new EPLServer("visitposthighratio", (map) -> {
//            if (map.get("name").equals("ip_get_ratio")){
//                System.out.println("key, " + map.get("key") + " : " + map.get("value"));
//            }
            System.out.println(map);
            if (map.containsKey("name") && map.get("name").equals("__visitposthighratio_collector__")) {
                Assert.assertEquals(testIP, map.get("key"));
                assertFlag.setTrue();
            }
        }, getEPLs());

        for (int i = 0; i < 3; i++) {
            Event event = eventMaker.nextEvent();
            if (i == 0) {
                event.getPropertyValues().put("method", "GET");
            } else {
                event.getPropertyValues().put("method", "POST");
            }
            event.getPropertyValues().put("c_ip", testIP);
            server.addEvent(event);
            Thread.sleep(1000);
            if (i < 2) {
                Assert.assertFalse(assertFlag.getValue());
            } else {
                Assert.assertTrue(assertFlag.getValue());
            }
        }
    }

    private List<String> getEPLs() {
        List<EsperEPL> epls = VariableEPLGenerator.getEPLs(VariableMetaTestHelper.getAllWrapper());
        List<String> result = new ArrayList<>();
        epls.forEach(epl -> result.add(epl.getStatement()));
        result.forEach(System.out::println);
        return result;
    }
}
