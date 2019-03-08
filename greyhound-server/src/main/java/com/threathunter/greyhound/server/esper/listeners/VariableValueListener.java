package com.threathunter.greyhound.server.esper.listeners;

import com.threathunter.greyhound.server.query.VariableGlobalValueQuery;
import com.threathunter.greyhound.server.query.VariableValueQuery;
import com.espertech.esper.client.EventBean;

/**
 * @author Wen Lu
 */
public class VariableValueListener extends EsperEventListener {

    public VariableValueListener() {
        super("variablevalue");
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents != null) {
            for(EventBean bean : newEvents) {
                try {
                    if ("__varvalue".equals(bean.getEventType().getName())) {
                        String variablename = (String)bean.get("variablename");
                        Double value = (Double)bean.get("value");
                        String requestid = (String)bean.get("requestid");

                        if ("".equals(bean.get("key"))) {
                            VariableGlobalValueQuery q = VariableGlobalValueQuery.getQuery(requestid);
                            if (q != null) {
                                q.addResult(variablename, value);
                            }
                        } else {
                            VariableValueQuery q = VariableValueQuery.getQuery(requestid);
                            if (q != null) {
                                q.addResult(variablename, value);
                            }
                        }
                    }
                } catch(Exception ex) {
                    processError("fail to process value", ex);
                }
            }
        }

    }

    @Override
    public boolean isOnlineListener() {
        return true;
    }
}
