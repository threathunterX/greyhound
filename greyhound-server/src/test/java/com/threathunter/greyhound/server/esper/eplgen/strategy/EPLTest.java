package com.threathunter.greyhound.server.esper.eplgen.strategy;

import com.threathunter.babel.rpc.RemoteException;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.esper.eplgen.EPLServer;
import com.threathunter.greyhound.tool.data.AccountLoginEventMaker;
import com.threathunter.greyhound.tool.data.HttpDynamicEventMaker;
import com.threathunter.greyhound.tool.data.OrderSubmitEventMaker;
import com.threathunter.greyhound.tool.data.babel.client.HttpLogSender;
import com.threathunter.model.Event;
import com.espertech.esper.event.map.MapEventBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class EPLTest {
    public static void main(String[] args) throws InterruptedException, RemoteException {
//        EPLTest.runRatio();
        EPLTest.runInterval();
    }

    public static void testEPLs() throws InterruptedException {
        List<String> epls = new ArrayList<>();
        epls.add("create schema HTTP_DYNAMIC as (`app` string, `uid` string, `method` string, `s_type` string, `name` string, `s_bytes` long, `c_bytes` long, `c_ip` string, `value` double, `did` string, `key` string, `timestamp` long)");
        epls.add("@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(999) insert into `HTTP_CLICK` select 'nebula' as app, 'HTTP_CLICK' as name, current_timestamp as timestamp, HTTP_DYNAMIC.c_ip as c_ip, HTTP_DYNAMIC.uid as uid, HTTP_DYNAMIC.did as did, HTTP_DYNAMIC.method as method, HTTP_DYNAMIC.s_type as s_type, HTTP_DYNAMIC.c_bytes as c_bytes, HTTP_DYNAMIC.s_bytes as s_bytes, coalesce(HTTP_DYNAMIC.value, 0) as value, '' as key from `HTTP_DYNAMIC`  where ((HTTP_DYNAMIC.method='POST') or ((HTTP_DYNAMIC.method='GET') and (HTTP_DYNAMIC.s_type.contains('text/html')) and (HTTP_DYNAMIC.s_bytes>1000)))");
        epls.add("@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(999) insert into `__highvisit_trigger__` select 'nebula' as app, '__highvisit_trigger__' as name, current_timestamp as timestamp, HTTP_DYNAMIC.c_ip as c_ip, HTTP_DYNAMIC.uid as uid, HTTP_DYNAMIC.did as did, HTTP_DYNAMIC.method as method, HTTP_DYNAMIC.s_type as s_type, HTTP_DYNAMIC.c_bytes as c_bytes, HTTP_DYNAMIC.s_bytes as s_bytes, coalesce(HTTP_DYNAMIC.value, 0) as value, '' as key from `HTTP_DYNAMIC`  where (HTTP_DYNAMIC.c_ip.contains('.'))");
        epls.add("@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(999) insert into `__visitposthighratio_trigger__` select 'nebula' as app, '__visitposthighratio_trigger__' as name, current_timestamp as timestamp, HTTP_DYNAMIC.c_ip as c_ip, HTTP_DYNAMIC.uid as uid, HTTP_DYNAMIC.did as did, HTTP_DYNAMIC.method as method, HTTP_DYNAMIC.s_type as s_type, HTTP_DYNAMIC.c_bytes as c_bytes, HTTP_DYNAMIC.s_bytes as s_bytes, coalesce(HTTP_DYNAMIC.value, 0) as value, '' as key from `HTTP_DYNAMIC`  where (HTTP_DYNAMIC.c_ip.contains('.'))");
        epls.add("@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(999) insert into `ip__dynamic_count` select 'nebula' as app, 'ip__dynamic_count' as name, current_timestamp as timestamp, HTTP_DYNAMIC.c_ip as c_ip, count(HTTP_DYNAMIC.c_ip) as value, HTTP_DYNAMIC.c_ip as key from `HTTP_DYNAMIC`.win:time(300 seconds) group by HTTP_DYNAMIC.c_ip");
        epls.add("@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(999) insert into `__clicktoofast_trigger__` select 'nebula' as app, '__clicktoofast_trigger__' as name, current_timestamp as timestamp, HTTP_DYNAMIC.c_ip as c_ip, HTTP_DYNAMIC.uid as uid, HTTP_DYNAMIC.did as did, HTTP_DYNAMIC.method as method, HTTP_DYNAMIC.s_type as s_type, HTTP_DYNAMIC.c_bytes as c_bytes, HTTP_DYNAMIC.s_bytes as s_bytes, coalesce(HTTP_DYNAMIC.value, 0) as value, '' as key from `HTTP_DYNAMIC`  where (HTTP_DYNAMIC.c_ip.contains('.'))");
        epls.add("@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(999) insert into `ip__dynamic_get_count` select 'nebula' as app, 'ip__dynamic_get_count' as name, current_timestamp as timestamp, HTTP_DYNAMIC.c_ip as c_ip, count(HTTP_DYNAMIC.c_ip) as value, HTTP_DYNAMIC.c_ip as key from `HTTP_DYNAMIC`.win:time(300 seconds) where (HTTP_DYNAMIC.method='GET')  group by HTTP_DYNAMIC.c_ip");
        epls.add("@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(998) insert into `__highvisit_collector__` select 'nebula' as app, '__highvisit_collector__' as name, current_timestamp as timestamp, 'highvisit_strategy' as strategyName, __highvisit_trigger__.value as __highvisit_trigger__, ip__dynamic_count.value as ip__dynamic_count, __highvisit_trigger__.* as triggerevent, __highvisit_trigger__.c_ip as key from __highvisit_trigger__ as __highvisit_trigger__ unidirectional left outer join ip__dynamic_count.std:lastevent() as ip__dynamic_count on __highvisit_trigger__.c_ip=ip__dynamic_count.key where (coalesce(ip__dynamic_count.value, 0)>5)");
        epls.add("@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(998) insert into `ip_get_ratio` select 'nebula' as app, 'ip_get_ratio' as name, current_timestamp as timestamp, ip__dynamic_get_count.c_ip as c_ip, ip__dynamic_get_count.c_ip as key, (OperationExt.doubleDivide(coalesce(ip__dynamic_get_count.value, 0), coalesce(ip__dynamic_count.value, 0))) as value from `ip__dynamic_get_count`.std:lastevent() as ip__dynamic_get_count full outer join `ip__dynamic_count`.std:lastevent() as ip__dynamic_count on ip__dynamic_get_count.key = ip__dynamic_count.key");
        epls.add("@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(998) insert into `ip_click_diff` select 'nebula' as app, 'ip_click_diff' as name, current_timestamp as timestamp, HTTP_CLICK.c_ip as c_ip, (last(HTTP_CLICK.timestamp, 0) - last(HTTP_CLICK.timestamp, 1)) as value, last(HTTP_CLICK.timestamp, 0) as firstvalue, last(HTTP_CLICK.timestamp, 1) as secondvalue, HTTP_CLICK.c_ip as key from `HTTP_CLICK`.std:groupwin(c_ip).win:length(2) as HTTP_CLICK  where (HTTP_CLICK.method='POST') group by HTTP_CLICK.c_ip having ((last(HTTP_CLICK.timestamp, 0)) > 0 and (last(HTTP_CLICK.timestamp, 1)) > 0)");
        epls.add("@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(997) insert into `__clicktoofast_collector__` select 'nebula' as app, '__clicktoofast_collector__' as name, current_timestamp as timestamp, 'clicktoofast_strategy' as strategyName, __clicktoofast_trigger__.value as __clicktoofast_trigger__, ip_click_diff.value as ip_click_diff, __clicktoofast_trigger__.* as triggerevent, __clicktoofast_trigger__.c_ip as key from __clicktoofast_trigger__ as __clicktoofast_trigger__ unidirectional left outer join ip_click_diff.std:lastevent() as ip_click_diff on __clicktoofast_trigger__.c_ip=ip_click_diff.key where (coalesce(ip_click_diff.value, 0)<1000.0)");
        epls.add("@Hint('reclaim_group_aged=300,reclaim_group_freq=150') @Priority(997) insert into `__visitposthighratio_collector__` select 'nebula' as app, '__visitposthighratio_collector__' as name, current_timestamp as timestamp, 'visitposthighratio_strategy' as strategyName, __visitposthighratio_trigger__.value as __visitposthighratio_trigger__, ip_get_ratio.value as ip_get_ratio, __visitposthighratio_trigger__.* as triggerevent, __visitposthighratio_trigger__.c_ip as key from __visitposthighratio_trigger__.std:lastevent() as __visitposthighratio_trigger__ full outer join ip_get_ratio.std:lastevent() as ip_get_ratio on __visitposthighratio_trigger__.c_ip=ip_get_ratio.key where ((coalesce(ip_get_ratio.value, 0)<0.4) and (coalesce(ip_get_ratio.value, 0)>0.0))");

        EPLServer server = new EPLServer("test", (Map<String, Object> map) -> {
            if (map.containsKey("name") && ((String) map.get("name")).contains("_collector__")) {
                String triggerPage = (String) ((MapEventBean) map.get("triggerevent")).getProperties().get("uid");
                System.out.println(triggerPage + map.get("name").toString() + ", ip visit count: " + map.get("ip__dynamic_count"));
            }
        }, epls);
        HttpDynamicEventMaker maker = new HttpDynamicEventMaker(10);

        String testIP = "1.1.1.1";
        for (int i = 0; i < 5; i++) {
            Event event = maker.nextEvent();
            event.getPropertyValues().put("c_ip", testIP);
            event.getPropertyValues().put("uid", event.getId());
            Thread.sleep(100);
            server.addEvent(event);
        }

        System.out.println("send the trigger event for strategy highvisit");

        Event event = maker.nextEvent();
        event.getPropertyValues().put("c_ip", testIP);
        event.getPropertyValues().put("uid", event.getId());
        server.addEvent(event);

        Thread.sleep(100);
    }

    public static void runInterval() throws InterruptedException {
        List<String> epls = new ArrayList<>();
        epls.add("create schema ACCOUNT_LOGIN as (`referer` string, `c_port` long, `request_type` string" +
                ", `s_ip` string, `s_body` string, `useragent` string, `geo_province` string, `c_bytes` long, `platform` string, `sid` string, `result` string, `uid` string, `password` string, `request_" +
                "time` long, `captcha` string, `host` string, `geo_city` string, `remember_me` string, `value` double, `login_channel` string, `key` string, `timestamp` long, `app` string, `s_port` long" +
                ", `method` string, `cookie` string, `c_body` string, `s_type` string, `login_verification_type` string, `c_ip` string, `notices` string, `uri_stem` string, `uri_query` string, `c_type`" +
                "string, `name` string, `s_bytes` long, `page` string, `xforward` string, `did` string, `referer_hit` string, `status` long)");
        epls.add("create schema HTTP_DYNAMIC as (`referer` string, `c_port` long, `request_type` string, `s_ip` string, `s_body` string, `useragent` string, `geo_province` string, `c_bytes` long, " +
                "`platform` string, `sid` string, `uid` string, `request_time` long, `host` string, `geo_city` string, `value` double, `key` string, `timestamp` long, `app` string, `s_port` long, `method` string, " +
                "`cookie` string, `c_body` string, `s_type` string, `c_ip` string, `notices` string, `uri_stem` string, `uri_query` string, `c_type` string, `name` string, `s_bytes` long, `page` string, `xforward` string, " +
                "`did` string, `referer_hit` string, `status` long)");
        epls.add("insert into `_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__trigger__rt` select 'nebula' as app, '_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__trigger__rt' as name, current_timestamp as timestamp, " +
                "ACCOUNT_LOGIN.c_ip as c_ip, ACCOUNT_LOGIN.sid as sid, ACCOUNT_LOGIN.did as did, ACCOUNT_LOGIN.platform as platform, ACCOUNT_LOGIN.page as page, ACCOUNT_LOGIN.c_port as c_port, ACCOUNT_LOGIN.c_bytes as c_bytes, ACCOUNT_LOGIN.c_body as c_body, " +
                "ACCOUNT_LOGIN.c_type as c_type, ACCOUNT_LOGIN.s_ip as s_ip, ACCOUNT_LOGIN.s_port as s_port, ACCOUNT_LOGIN.s_bytes as s_bytes, ACCOUNT_LOGIN.s_body as s_body, ACCOUNT_LOGIN.s_type as s_type, ACCOUNT_LOGIN.host as host, ACCOUNT_LOGIN.uri_stem as uri_stem, " +
                "ACCOUNT_LOGIN.uri_query as uri_query, ACCOUNT_LOGIN.referer as referer, ACCOUNT_LOGIN.method as method, ACCOUNT_LOGIN.status as status, ACCOUNT_LOGIN.cookie as cookie, ACCOUNT_LOGIN.useragent as useragent, ACCOUNT_LOGIN.xforward as xforward, " +
                "ACCOUNT_LOGIN.request_time as request_time, ACCOUNT_LOGIN.request_type as request_type, ACCOUNT_LOGIN.referer_hit as referer_hit, ACCOUNT_LOGIN.notices as notices, ACCOUNT_LOGIN.geo_city as geo_city, ACCOUNT_LOGIN.geo_province as geo_province, ACCOUNT_LOGIN.uid as uid, " +
                "ACCOUNT_LOGIN.login_verification_type as login_verification_type, ACCOUNT_LOGIN.password as password, ACCOUNT_LOGIN.captcha as captcha, ACCOUNT_LOGIN.result as result, ACCOUNT_LOGIN.remember_me as remember_me, ACCOUNT_LOGIN.login_channel as login_channel, " +
                "coalesce(ACCOUNT_LOGIN.value, 0) as value, '' as key from `ACCOUNT_LOGIN`  where (ACCOUNT_LOGIN.did!='')");
        epls.add("insert into `_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_1___rt` select 'nebula' as app, '_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_1___rt' as name, current_timestamp as timestamp, HTTP_DYNAMIC.did as did, " +
                "last(HTTP_DYNAMIC.timestamp) as value, HTTP_DYNAMIC.did as key from `HTTP_DYNAMIC`.win:time(300 seconds) where ((HTTP_DYNAMIC.page.contains('captcha')))  group by HTTP_DYNAMIC.did");
        epls.add("insert into `_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_2___rt` select 'nebula' as app, '_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_2___rt' as name, current_timestamp as timestamp, " +
                "_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__trigger__rt.did as did, last(_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__trigger__rt.timestamp) as value, _did__strategy__None__696E74657276616C5FE6B58BE8AF9531__trigger__rt.did as key " +
                "from `_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__trigger__rt`.win:time(300 seconds) group by _did__strategy__None__696E74657276616C5FE6B58BE8AF9531__trigger__rt.did");
        epls.add("insert into `_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_3___rt` select 'nebula' as app, '_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_3___rt' as name, current_timestamp as timestamp, " +
                "_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_2___rt.did as did, coalesce(_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_2___rt.did, _did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_1___rt.did) as key, " +
                "(coalesce(_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_2___rt.value, 0) - coalesce(_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_1___rt.value, 0)) as value " +
                "from `_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_2___rt` as _did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_2___rt unidirectional " +
                "join `_did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_1___rt`.std:lastevent() as _did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_1___rt " +
                " on _did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_2___rt.key = _did__strategy__None__696E74657276616C5FE6B58BE8AF9531__counter__2_1___rt.key");

        EPLServer server = new EPLServer("test", (Map<String, Object> map) -> {
            System.out.println(map);
        }, epls);

        HttpDynamicEventMaker dynamicEventMaker = new HttpDynamicEventMaker(1);
        AccountLoginEventMaker loginEventMaker = new AccountLoginEventMaker(1);
        String testDid = "test_did";
        Event captchaEvent = dynamicEventMaker.nextEvent();
        captchaEvent.getPropertyValues().put("page", "captcha");
        captchaEvent.getPropertyValues().put("did", testDid);
        server.addEvent(captchaEvent);

        Thread.sleep(500);

        Event loginEvent = loginEventMaker.nextEvent();
        loginEvent.getPropertyValues().put("did", testDid);
        server.addEvent(loginEvent);

        Thread.sleep(1000);
    }

    public static void runRatio() throws InterruptedException {
        List<String> epls = new ArrayList<>();
        epls.add("create schema ACCOUNT_LOGIN as (`referer` string, `c_port` long, `request_type` string" +
                ", `s_ip` string, `s_body` string, `useragent` string, `geo_province` string, `c_bytes` long, `platform` string, `sid` string, `result` string, `uid` string, `password` string, `request_" +
                "time` long, `captcha` string, `host` string, `geo_city` string, `remember_me` string, `value` double, `login_channel` string, `key` string, `timestamp` long, `app` string, `s_port` long" +
                ", `method` string, `cookie` string, `c_body` string, `s_type` string, `login_verification_type` string, `c_ip` string, `notices` string, `uri_stem` string, `uri_query` string, `c_type`" +
                "string, `name` string, `s_bytes` long, `page` string, `xforward` string, `did` string, `referer_hit` string, `status` long)");
        epls.add("insert into `ip__account_login_count__5m__rt` select 'nebula' as app, 'ip__account_login_count__5m__rt' as name, current_timestamp as timestamp, ACCOUNT_LOGIN.c_ip as c_ip, count(coalesce(ACCOUNT_LOGIN.value, 0)) as value, " +
                "ACCOUNT_LOGIN.c_ip as key from `ACCOUNT_LOGIN`.win:time(300 seconds) group by ACCOUNT_LOGIN.c_ip");
        epls.add("insert into `ip__account_login_count_fail__5m__rt` select 'nebula' as app, 'ip__account_login_count_fail__5m__rt' as name, current_timestamp as timestamp, ACCOUNT_LOGIN.c_ip as c_ip, count(coalesce(ACC" +
                "OUNT_LOGIN.value, 0)) as value, ACCOUNT_LOGIN.c_ip as key from `ACCOUNT_LOGIN`.win:time(300 seconds) where (ACCOUNT_LOGIN.result='F')  group by ACCOUNT_LOGIN.c_ip");
//        epls.add("insert into `ip__account_login_fail_ratio__5m__rt` select 'nebula' as app, 'ip__account_login_fail_ratio__5m__rt' as name, current_timestamp as timestamp, ip__account_login_count_fail__5m__rt.c_ip as c" +
        epls.add("insert into `ip__account_login_fail_ratio__5m__rt` select 'nebula' as app, 'ip__account_login_fail_ratio__5m__rt' as name, current_timestamp as timestamp, coalesce(ip__account_login_count_fail__5m__rt.c_ip, ip__account_login_count__5m__rt.c_ip) as c" +
                "_ip, coalesce(ip__account_login_count_fail__5m__rt.c_ip, ip__account_login_count__5m__rt.c_ip) as key, (OperationExt.doubleDivide(coalesce(ip__account_login_count_fail__5m__rt.value, 0)" +
                ", coalesce(ip__account_login_count__5m__rt.value, 0))) as value from `ip__account_login_count_fail__5m__rt` as ip__account_login_count_fail__5m__rt unidirectional join `ip__" +
                "account_login_count__5m__rt`.std:lastevent() as ip__account_login_count__5m__rt on ip__account_login_count_fail__5m__rt.key = ip__account_login_count__5m__rt.key");

        EPLServer server = new EPLServer("test", (Map<String, Object> map) -> {
            System.out.println(map);
        }, epls);
        AccountLoginEventMaker maker = new AccountLoginEventMaker(1);
        String testIP = "1.1.1.1";
        Event first = maker.nextEvent();
        first.getPropertyValues().put("c_ip", testIP);
        first.getPropertyValues().put("result", "T");
        System.out.println("send first");
        server.addEvent(first);

        for (int i = 0; i < 2; i++) {
            Event second = maker.nextEvent();
            second.getPropertyValues().put("c_ip", testIP);
            second.getPropertyValues().put("result", "F");
            System.out.println("send " + i);
            server.addEvent(second);
        }

        Thread.sleep(1000);
    }

    public static void testOrder() throws RemoteException {
        CommonDynamicConfig.getInstance().addOverrideProperty("babel_server", "redis");
        CommonDynamicConfig.getInstance().addOverrideProperty("redis_port", 6379);
        OrderSubmitEventMaker maker = new OrderSubmitEventMaker(1);

        String testUid = "user_test_1";
        HttpLogSender sender = new HttpLogSender();
        sender.start();

        Event first = maker.nextEvent();
        first.getPropertyValues().put("uid", testUid);
        first.getPropertyValues().put("receiver_address_city", "上海市");
        sender.notify(first);

        Event second = maker.nextEvent();
        second.getPropertyValues().put("uid", testUid);
        second.getPropertyValues().put("receiver_address_city", "苏州市");
        sender.notify(second);
        sender.notify(second);

        Event third = maker.nextEvent();
        third.getPropertyValues().put("uid", testUid);
        third.getPropertyValues().put("receiver_address_city", "太仓市");
        sender.notify(third);
        sender.notify(third);

        Event fourth = maker.nextEvent();
        fourth.getPropertyValues().put("uid", testUid);
        fourth.getPropertyValues().put("receiver_address_city", "常州市");
        sender.notify(fourth);

        sender.stop();
        System.exit(0);
    }
}
