package com.threathunter.greyhound.server.esper.extension;

import java.util.HashMap;
import java.util.Map;

/**
 * Functions for operation extension that can be used in epl.
 *
 * In order to enhance the esper functionality, we can define several static
 * functions and import them into esper.
 *
 * User: wenlu
 * Date: 13-12-10
 */
public class OperationExt implements EsperExtension {
    public static Double NEGATIVE_MAX = Double.MAX_VALUE * -1;

    public static Double doubleDivide(double numerator, double denominator) {
        Double result;
        if (denominator == 0) {
            result = null;
        } else {
            result = numerator / denominator;
            if (Double.isNaN(result)) {
                result = null;
            }
        }

        return result;
    }

    private static String _concatAttributes(Object... attributes) {
        if (attributes == null || attributes.length == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for(Object a : attributes) {
            sb.append(a);
            sb.append("@@");
        }
        // remove the last "@@"
        sb.deleteCharAt(sb.length()-1);
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }

    private static Boolean isStringIn(String part, String whole) {
        if (whole == null) {
            return false;
        }

        return whole.contains(part);
    }

    public static String concatAttributes() {
        return "";
    }

    public static String concatAttributes(Object a1) {
        return _concatAttributes(a1);
    }

    public static String concatAttributes(Object a1, Object a2) {
        return _concatAttributes(a1, a2);
    }

    public static String concatAttributes(Object a1, Object a2, Object a3) {
        return _concatAttributes(a1, a2, a3);
    }

    public static String concatAttributes(Object a1, Object a2, Object a3, Object a4) {
        return _concatAttributes(a1, a2, a3, a4);
    }

    public static String concatAttributes(Object a1, Object a2, Object a3, Object a4, Object a5) {
        return _concatAttributes(a1, a2, a3, a4, a5);
    }

    public static String concatAttributes(Object a1, Object a2, Object a3, Object a4, Object a5, Object a6) {
        return _concatAttributes(a1, a2, a3, a4, a5, a6);
    }

    public static String concatAttributes(Object a1, Object a2, Object a3, Object a4, Object a5, Object a6, Object a7) {
        return _concatAttributes(a1, a2, a3, a4, a5, a6, a7);
    }

    public static Map<String, Double> buildMap(Object[] args) {
        Map<String, Double> result = new HashMap<>();
        if (args != null) {
            for (int i = 0; i < args.length-1; i += 2) {
                String key = (String) args[i];
                Double value = (Double) args[i+1];
                if (key != null) {
                    result.put(key, value);
                }
            }
        }
        return result;
    }
}
