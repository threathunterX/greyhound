package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.greyhound.tool.data.EventMaker;
import com.threathunter.greyhound.tool.data.HttpDynamicEventMaker;
import com.threathunter.model.Event;
import com.espertech.esper.client.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by daisy on 17/5/23.
 */
public class EPLEsperTest {
    public static void main(String[] args) throws InterruptedException {
        String epl1 = "create schema HTTP_DYNAMIC as (`app` string, `uid` string, `method` string, `s_type` string, `name` string, `s_bytes` long, `c_bytes` long, `c_ip` string, `value` double, `did` string, `key` string, `timestamp` long)";
        String epl2 = "@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(999) insert into `ip__dynamic_count` select HTTP_DYNAMIC.c_ip as c_ip, count(HTTP_DYNAMIC.c_ip) as value, HTTP_DYNAMIC.c_ip as key from `HTTP_DYNAMIC`.win:time(10 seconds) group by HTTP_DYNAMIC.c_ip";
        String epl3 = "@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(999) insert into `HTTP_CLICK` select HTTP_DYNAMIC.c_ip as c_ip, HTTP_DYNAMIC.uid as uid, HTTP_DYNAMIC.did as did, HTTP_DYNAMIC.method as method, HTTP_DYNAMIC.s_type as s_type, HTTP_DYNAMIC.c_bytes as c_bytes, HTTP_DYNAMIC.s_bytes as s_bytes, coalesce(HTTP_DYNAMIC.value, 0) as value, '' as key, current_timestamp as timestamp from `HTTP_DYNAMIC`  where ((HTTP_DYNAMIC.method='POST') or ((HTTP_DYNAMIC.method='GET') and (HTTP_DYNAMIC.s_type.contains('text/html')) and (HTTP_DYNAMIC.s_bytes>1000)))";
        String epl4 = "@Hint('reclaim_group_aged=10,reclaim_group_freq=5') @Priority(998) insert into `ip_click_diff` select HTTP_CLICK.c_ip as c_ip, (last(HTTP_CLICK.timestamp, 0) - last(HTTP_CLICK.timestamp, 1)) as value, last(HTTP_CLICK.timestamp, 0) as firstvalue, last(HTTP_CLICK.timestamp, 1) as secondvalue, HTTP_CLICK.c_ip as key, current_timestamp as timestamp from `HTTP_CLICK`.std:groupwin(c_ip).win:length(2) as HTTP_CLICK where HTTP_CLICK.method='POST' group by HTTP_CLICK.c_ip having (last(HTTP_CLICK.timestamp, 1) > 0)";
        String epl5 = "@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(999) insert into `ip__dynamic_count_update` select 'update' as name, ip_click_diff.c_ip as c_ip,ip_click_diff.value as value, ip_click_diff.key as key from `ip_click_diff`.std:lastevent() group by ip_click_diff.c_ip";
        List<String> epls = new ArrayList<>();
        epls.add(epl1);
        epls.add(epl2);
        epls.add(epl3);
        epls.add(epl4);
        epls.add(epl5);

        EPServiceProvider epService = EPServiceProviderManager.getProvider("provider", buildEsperConfiguration());
        EPAdministrator admin = epService.getEPAdministrator();

        UpdateListener listener = (newEvents, oldEvents) -> {
            if (newEvents != null && newEvents.length > 0) {
                if (newEvents[0].getEventType().getName().equals("ip_click_diff")) {
                    System.out.println("come in");
                }
                Map<String, Object> o = (Map<String, Object>) newEvents[0].getUnderlying();
                if (o.containsKey("name") && ((String) o.get("name")).contains("update")) {
                    System.out.println(o.get("c_ip") + ": " + o.get("value"));
                }
//                System.out.println(newEvents[0].getEventType().getName());
            }
            if (oldEvents != null && oldEvents.length > 0) {
                System.out.println(oldEvents[0].getEventType().getName());
            }
        };
        for (int i = 0; i < 5; i++) {
            EPStatement statement = admin.createEPL(epls.get(i));
            statement.addListener(listener);
        }

        EventMaker eventMaker = new HttpDynamicEventMaker(1);
        Event httpEvent = eventMaker.nextEvent();
        httpEvent.getPropertyValues().put("c_ip", "1.1.1.1");
        httpEvent.getPropertyValues().put("method", "GET");
        httpEvent.getPropertyValues().put("s_bytes", 1100);
        epService.getEPRuntime().sendEvent(httpEvent.genAllData(), httpEvent.getName());
        Thread.sleep(4000);
        epService.getEPRuntime().sendEvent(httpEvent.genAllData(), httpEvent.getName());

        Event http2 = eventMaker.nextEvent();
        http2.getPropertyValues().put("c_ip", "2.2.2.2");
        http2.getPropertyValues().put("method", "POST");
        epService.getEPRuntime().sendEvent(http2.genAllData(), httpEvent.getName());
        System.out.println("sleep for 10 sec");
        Thread.sleep(11000);

        Event http3 = eventMaker.nextEvent();
        http3.getPropertyValues().put("c_ip", "2.2.2.2");
        http3.getPropertyValues().put("method", "POST");
        epService.getEPRuntime().sendEvent(http3.genAllData(), httpEvent.getName());
        epService.getEPRuntime().sendEvent(httpEvent.genAllData(), httpEvent.getName());
//        for (int i = 0; i < 3; i++) {
//            Event event = eventMaker.nextEvent();
//            event.setValue(300.0);
//            if (i > 0) {
////                event.setName("HTTP_STATIC");
//                event.setValue(1.0);
//            }
//            epService.getEPRuntime().sendEvent(event.genAllData(), event.getName());
//        }
//        Thread.sleep(1000);
    }

    private static Configuration buildEsperConfiguration() {
        Configuration configuration = new Configuration();
        configuration.getEngineDefaults().getThreading().setInternalTimerEnabled(true);
        configuration.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder(true);


        configuration.getEngineDefaults().getExecution().setPrioritized(true);
        return configuration;
    }
}
