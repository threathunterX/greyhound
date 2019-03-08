package com.threathunter.greyhound.server.esper.listeners;

import com.threathunter.greyhound.server.query.VariableTopValuesStore;
import com.espertech.esper.client.EventBean;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Wen Lu
 */
public class TopValueListener extends EsperEventListener {

    public TopValueListener() {
        super("variabletop");
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents != null && newEvents.length > 0
                && "__vartop".equals(newEvents[0].getEventType().getName())) {
            Map<String, Double> values = new HashMap<>();
            String varName = (String)newEvents[0].get("variablename");
            try {
                for(EventBean bean : newEvents) {
                    String key = (String)bean.get("key");
                    Double value = (Double)bean.get("value");
                    if (value == null) continue;

                    values.put(key, value);
                }
                VariableTopValuesStore.setTopValuesInThisThread(varName, values);
            } catch (Exception ex) {
                processError("fail to process top value", ex);
            }
        }
    }

    @Override
    public boolean isOnlineListener() {
        return false;
    }
}
