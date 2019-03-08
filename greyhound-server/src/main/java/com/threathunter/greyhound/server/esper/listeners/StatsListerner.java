package com.threathunter.greyhound.server.esper.listeners;

import com.espertech.esper.client.EventBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Wen Lu
 */
public class StatsListerner extends EsperEventListener {
    private static final Logger logger = LoggerFactory.getLogger(StatsListerner.class);

    private volatile ConcurrentMap<String, AtomicLong> variableStatsMap = new ConcurrentHashMap<>();

    public StatsListerner() {
        super("stats");
//        holder = new VariableCounterMetrics("variablestats", 60, 300, true);
//        totalCalculate = new MetricsHolder("totalcalculate", 60, 300, true);
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents != null && newEvents.length > 0) {
            try {
                for (EventBean bean : newEvents) {
                    String variableName = bean.getEventType().getName();
                    AtomicLong counter = variableStatsMap.get(variableName);
                    if (counter == null) {
                        counter = new AtomicLong();
                        AtomicLong older = variableStatsMap.putIfAbsent(variableName, counter);
                        if (older != null) {
                            counter = older;
                        }
                    }
                    counter.incrementAndGet();
                }
            } catch (Exception error) {
                processError("fail to process the key top value", error);
            }
        }
//        if (newEvents != null && newEvents.length > 0
//                && "__varstats".equals(newEvents[0].getEventType().getName())) {
//            try {
//                for(EventBean bean : newEvents) {
//                    String variableName = (String) bean.get("variablename");
//                    Double countValue = (Double) bean.get("countvalue");
//                    holder.addValue(variableName, countValue);
//
//                    for(VariableMeta v : VariableMetaRegistry.getInstance().getAllVariableMetas()) {
//                        List<Identifier> srcids = v.getSrcVariablesID();
//                        if (srcids == null || srcids.isEmpty())
//                            continue;
//
//                        if (variableName.equals(srcids.get(0).getKeys().get(1))) {
//                            // found one son variable
//                            totalCalculate.addValue(countValue);
//                        }
//
//                    }
//                }
//            } catch (Exception error) {
//                processError("fail to process the key top value", error);
//            }
//        }
    }

    @Override
    public boolean isOnlineListener() {
        return false;
    }

//    public class VariableCounterMetrics extends MetricsForFamilyHolder {
//
//        public VariableCounterMetrics(String baseName, long periodInSeconds, long copies, boolean accumulative) {
//            super(baseName, periodInSeconds, copies, accumulative);
//        }
//
//        @Override
//        public void collectValue() {
//            ConcurrentMap<String, AtomicLong> mapInLastRound = variableStatsMap;
//            variableStatsMap = new ConcurrentHashMap<>();
//
//            logger.debug("collecting stats metrics for {} variables", mapInLastRound.size());
//            for (ConcurrentMap.Entry<String, AtomicLong> entry : mapInLastRound.entrySet()) {
//                this.addValue(entry.getKey(), entry.getValue().longValue());
//            }
//        }
//
//    }
}
