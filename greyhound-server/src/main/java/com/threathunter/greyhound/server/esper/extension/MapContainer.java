package com.threathunter.greyhound.server.esper.extension;

import java.util.HashMap;

/**
 * Store multiple kv entires into one map.
 *
 * <p>Provide esper friendly contructors and support at most 7 kv entries.
 *
 * @author Wen Lu
 */
public class MapContainer extends HashMap<Object, Object> implements EsperExtension {
    public MapContainer() {}

    private void init(Object... data) {
        if (data == null || data.length == 0) {
            return;
        }

        for(int i = 0; i < data.length/2*2; i+=2) {
            Object key = data[i];
            Object value = data[i+1];
            if (key == null || value == null)
                continue;

            this.put(key, value);
        }
    }

    public MapContainer(Object o1, Object o2) {
        init(o1, o2);
    }

    public MapContainer(Object o1, Object o2, Object o3, Object o4) {
        init(o1, o2, o3, o4);
    }

    public MapContainer(Object o1, Object o2, Object o3, Object o4, Object o5, Object o6) {
        init(o1, o2, o3, o4, o5, o6);
    }

    public MapContainer(Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8) {
        init(o1, o2, o3, o4, o5, o6, o7, o8);
    }

    public MapContainer(Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8,
                        Object o9, Object o10) {
        init(o1, o2, o3, o4, o5, o6, o7, o8, o9, o10);
    }

    public MapContainer(Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8,
                        Object o9, Object o10, Object o11, Object o12) {
        init(o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12);
    }

    public MapContainer(Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8,
                        Object o9, Object o10, Object o11, Object o12, Object o13, Object o14) {
        init(o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, o13, o14);
    }
}
