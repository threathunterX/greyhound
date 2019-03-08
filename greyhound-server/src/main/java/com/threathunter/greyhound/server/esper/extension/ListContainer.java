package com.threathunter.greyhound.server.esper.extension;

import java.util.ArrayList;

/**
 * Store multiple values into one list.
 *
 * <p>Provide esper friendly constructors and support at most 7 objects.
 *
 * @author Wen Lu
 */
public class ListContainer extends ArrayList<Object> implements EsperExtension {
    private Object[] data;

    private void init(Object... data) {
        for(Object o : data) {
            this.add(o);
        }
    }

    public ListContainer() {
    }

    public ListContainer(Object O1) {
        init(O1);
    }

    public ListContainer(Object O1, Object O2) {
        init(O1, O2);
    }

    public ListContainer(Object O1, Object O2, Object O3) {
        init(O1, O2, O3);
    }

    public ListContainer(Object O1, Object O2, Object O3, Object O4) {
        init(O1, O2, O3, O4);
    }

    public ListContainer(Object O1, Object O2, Object O3, Object O4, Object O5) {
        init(O1, O2, O3, O4, O5);
    }

    public ListContainer(Object O1, Object O2, Object O3, Object O4, Object O5,
                         Object O6) {
        init(O1, O2, O3, O4, O5, O6);
    }

    public ListContainer(Object O1, Object O2, Object O3, Object O4, Object O5,
                         Object O6, Object O7) {
        init(O1, O2, O3, O4, O5, O6, O7);
    }

    @Override
    public Object get(int index) {
        if (data == null || index >= data.length) {
            return null;
        }

        return super.get(index);
    }
}
