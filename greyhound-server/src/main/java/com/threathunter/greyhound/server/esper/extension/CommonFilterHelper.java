package com.threathunter.greyhound.server.esper.extension;


import com.threathunter.util.LocationHelper;

/**
 * 
 */
public class CommonFilterHelper implements EsperExtension {
    public static boolean ipEqual(String leftip, String type, String param) {
        String location = LocationHelper.getLocation(leftip, type);
        if (location == null || location.isEmpty()) {
            return false;
        }
        if (isContainsChinese(param)) {
            if (location.equals(param)) {
                return true;
            }
        } else {
            if (location.equals(LocationHelper.getLocation(param, type))) {
                return true;
            }
        }
        return false;
    }

    public static boolean ipContains(String leftip, String type, String p1) {
        return _ipContains(leftip, type, p1);
    }

    public static boolean ipContains(String leftip, String type, String p1, String p2) {
        return _ipContains(leftip, type, p1, p2);
    }

    public static boolean ipContains(String leftip, String type, String p1, String p2, String p3) {
        return _ipContains(leftip, type, p1, p2, p3);
    }

    public static boolean ipContains(String leftip, String type, String p1, String p2, String p3, String p4) {
        return _ipContains(leftip, type, p1, p2, p3, p4);
    }

    public static boolean ipContains(String leftip, String type, String p1, String p2, String p3, String p4, String p5) {
        return _ipContains(leftip, type, p1, p2, p3, p4, p5);
    }

    public static boolean _ipContains(String leftip, String type, String... params) {
        String location = LocationHelper.getLocation(leftip, type);
        if (location == null || location.isEmpty()) {
            return false;
        }
        for (String p : params) {
            if (isContainsChinese(p)) {
                if (location.equals(p)) {
                    return true;
                }
            } else {
                if (location.equals(LocationHelper.getLocation(p, type))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isContainsChinese(String param) {
        for (int i = 0; i < param.length(); ) {
            int codePoint = param.codePointAt(i);
            i += Character.charCount(codePoint);
            if (Character.UnicodeScript.of(codePoint) == Character.UnicodeScript.HAN) {
                return true;
            }
        }
        return false;
    }

}
