package com.threathunter.greyhound.server.esper.extension;

import com.threathunter.util.BloomFilterInWindow;

import static com.threathunter.common.Utility.isEmptyStr;

/**
 * Created by lw on 2015/4/24.
 */
public class BloomFilterHelper implements EsperExtension {

    private static BloomFilterInWindow w =
            new BloomFilterInWindow(60 * 1000, 1, 0.001, 10000);

    public static void addIPURL(String ip, String url) {
        if (isEmptyStr(ip))
            return;

        if (isEmptyStr(url))
            return;

        if (url.contains("?")) {
            url = url.split("\\?")[0];
        }

        String ipurl = ip + "@@" + url;
        w.add(ipurl.getBytes());
    }

    public static boolean isIPRefererVisted(String ip, String referer) {
        if (isEmptyStr(ip))
            return false;

        if (isEmptyStr(referer))
            return false;

        if (referer.contains("?")) {
            referer = referer.split("\\?")[0];
        }

        String ipurl = ip + "@@" + referer;
        return w.contains(ipurl.getBytes());
    }
}
