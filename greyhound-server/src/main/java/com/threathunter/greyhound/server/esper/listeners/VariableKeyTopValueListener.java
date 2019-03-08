package com.threathunter.greyhound.server.esper.listeners;

import com.threathunter.greyhound.server.query.VariableKeyTopValueQuery;
import com.espertech.esper.client.EventBean;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Wen Lu
 */
public class VariableKeyTopValueListener extends EsperEventListener {

    public VariableKeyTopValueListener() {
        super("variablekeytopvalue");
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents != null && newEvents.length > 0
                && "__varkeytopvalue".equals(newEvents[0].getEventType().getName())) {
            try {
                String requestid = (String)newEvents[0].get("requestid");
                Map<String, Double> values = new HashMap<>();
                for(EventBean bean : newEvents) {
                    String key = (String)bean.get("key");
                    Double value = (Double)bean.get("value");
                    if (value == null) continue;

                    values.put(key, value);
                }
                VariableKeyTopValueQuery query = VariableKeyTopValueQuery.getQuery(requestid);
                if (query != null) {
                    query.setResult(values);
                }
            } catch (Exception error) {
                processError("fail to process the key top value", error);
            }
        }
    }

    @Override
    public boolean isOnlineListener() {
        return true;
    }
}
