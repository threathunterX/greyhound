package com.threathunter.greyhound.server.esper.listeners;

import com.threathunter.util.MetricsHelper;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for esper update listeners which addDimensionTrigger error statistics.
 *
 * @author Wen Lu
 */
public abstract class EsperEventListener implements UpdateListener {

    private static final Logger logger = LoggerFactory.getLogger(EsperEventListener.class);

    private String name;
    private String metricName;

    EsperEventListener(String name) {
        this.name = name;

        String metricName = "esper-listener-" + name;
    }

    @Override
    public abstract void update(EventBean[] newEvents, EventBean[] oldEvents);

    public abstract boolean isOnlineListener();

    protected void processError(String msg, Throwable th) {
        MetricsHelper.getInstance().addMetrics(metricName, 1.0);
        String errMsg = "esper:listener " + name + " meets error:" + msg;
        if (th == null) {
            logger.error(errMsg);
        } else {
            logger.error(errMsg, th);
        }
    }

    protected void processError(String msg) {
        processError(msg, null);
    }

    public String getName() {
        return this.name;
    }
}
