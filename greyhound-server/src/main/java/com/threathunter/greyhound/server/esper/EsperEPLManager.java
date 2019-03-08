package com.threathunter.greyhound.server.esper;

import com.threathunter.greyhound.server.esper.exception.EsperException;
import com.espertech.esper.client.*;
import com.espertech.esper.client.annotation.Priority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * EsperEPLManager is a helper class for managing rules on one Esper batch.
 * 
 * @author Wen Lu
 */
public class EsperEPLManager {
	private static final Logger logger = LoggerFactory.getLogger(EsperEPLManager.class);

	private final List<EPServiceProvider> esperProviders;
	private final UpdateListener listener;

	public EsperEPLManager(List<EPServiceProvider> esperProviders, UpdateListener listener) {
		this.esperProviders	= esperProviders;
		this.listener = listener;
	}

	public EsperEPLManager(EPServiceProvider esperProvider, UpdateListener listener) {
		this.esperProviders	= Arrays.asList(esperProvider);
		this.listener = listener;
	}

    /**
     * Get the current rules from esper batch, the ones with highest priority comes first.
     *
     * The rules are sorted, so the ones with lower priority will be in the front of
     * ths list.
     */
	public List<EsperEPL> listRules() {
		 List<EsperEPL> result = new ArrayList<EsperEPL>();
		 if (esperProviders != null && esperProviders.size() > 0) {
			 EPAdministrator esperAdmin = esperProviders.get(0).getEPAdministrator();
			 String[] names = esperAdmin.getStatementNames();
			 for (String name : names) {
				 EPStatement statement = esperAdmin.getStatement(name);
                 boolean needListen = statement.getUpdateListeners().hasNext();
				 result.add(new EsperEPL(name, statement.getText(),
                         extractState(statement), needListen, extractPriority(statement)));
			 }
		 } else {
			 throw new EsperException("getRules failed, no esper found");
		 }

        result.sort(EsperEPL.comparatorDESC);
        return result;
	}

	public boolean removeRule(EsperEPL rule) {
		if (esperProviders == null || esperProviders.isEmpty()) {
			throw new EsperException("removeRule failed, esper doesn't exist");
		}
		if (isInvalidRule(rule)) {
			throw new EsperException("removeRule failed, rule is invalid");
		}

		for(EPServiceProvider provider : esperProviders) {
			EPAdministrator esperAdmin = provider.getEPAdministrator();
			EPStatement statement = esperAdmin.getStatement(rule.getName());
			if (statement != null) {
				statement.destroy();
			} else {
				logger.error("esper:removeRule failed, rule doesn't exist");
			}
		}

		return true;
	}

	public boolean addRule(EsperEPL rule) {
		if (esperProviders == null || esperProviders.isEmpty()) {
			throw new EsperException("addRule failed, esper doesn't exist");
		}
		if (isInvalidRule(rule)) {
            throw new EsperException("addRule failed, rule is invalid");
		}

		for(EPServiceProvider provider : esperProviders) {
			EPAdministrator esperAdmin = provider.getEPAdministrator();
			EPStatement statement = esperAdmin.getStatement(rule.getName());
			if (statement != null) {
				logger.error("esper:addRule failed, rule already exists, rule:" + statement.getText());
			} else {
				try {
					statement = esperAdmin.createEPL(rule.getStatement(), rule.getName());
					if (listener != null && rule.isNeedListen()) {
						statement.addListener(listener);
					}
				} catch (EPException e) {
					throw new EsperException("esper:fatal:addRule failed, exception from esper:", e);
				}
			}

		}

		return true;
	}

	/**
	 * Update Esper rules
	 * @param newRules
	 * @return false if there is one rule not updated successfully.
	 */
	public boolean updateRules(List<EsperEPL> newRules) {
        if (newRules == null) {
            throw new IllegalStateException("null rules");
        }

		boolean result = true;
		List<EsperEPL> existedRules = listRules();

		// Remove expired rules, the ones with lower priority will be checked and
		// removed first.
        existedRules.sort(EsperEPL.comparatorASC);
		for (EsperEPL r : existedRules) {
			if (!newRules.contains(r)) {
				if (!removeRule(r)) {
					result = false;
				} else {
					logger.warn("remove epl: " + r.getStatement());
				}
			}
		}

        // when adding new rules, we should first check and addDimensionTrigger the ones with higher priority
        newRules.sort(EsperEPL.comparatorDESC);
		for (EsperEPL r : newRules) {
			// Add new rules
			if (!existedRules.contains(r)) {
				if (!addRule(r)) {
					result = false;
				} else {
					logger.warn("add epl: " + r.getStatement());
				}
			}

			// start/stop rules
			if (r.isStarted()) {
				if (!startRule(r))
					result = false;
			} else {
				if (!stopRule(r))
					result = false;
			}
		}

		return result;
	}

	public boolean stopRule(EsperEPL rule) {
		if (esperProviders == null || esperProviders.isEmpty()) {
			throw new EsperException("stopRule failed, esper doesn't exist");
		}
		if (isInvalidRule(rule)) {
			throw new EsperException("stopRule failed, rule is invalid");
		}

		for(EPServiceProvider provider : esperProviders) {
			EPAdministrator esperAdmin = provider.getEPAdministrator();
			EPStatement statement = esperAdmin.getStatement(rule.getName());
			if (statement != null) {
				statement.stop();
			} else {
				logger.error("esper:stopRule failed, rule doesn't exist");
			}
		}

		return true;
	}

	public boolean startRule(EsperEPL rule) {
		if (esperProviders == null || esperProviders.isEmpty()) {
			throw new EsperException("startRule failed, esper doesn't exist");
		}
		if (isInvalidRule(rule)) {
			throw new EsperException("startRule failed, rule is invalid");
		}

		for(EPServiceProvider provider : esperProviders) {
			EPAdministrator	esperAdmin = provider.getEPAdministrator();
			EPStatement statement = esperAdmin.getStatement(rule.getName());
			if (statement != null) {
				statement.start();
			} else {
				logger.error("esper:startRule failed, rule doesn't exist");
			}
		}
		return true;
	}

	private boolean isInvalidRule(EsperEPL rule) {
		String name = rule.getName();
		String statement = rule.getStatement();
		return (name == null || name.isEmpty() 
				|| statement == null || statement.isEmpty());
	}

    private boolean extractState(EPStatement statement) {
        boolean result = false;

        if (statement != null) {
            if (EPStatementState.STARTED == statement.getState()) {
                result = true;
            }
        }

        return result;
    }

    private int extractPriority(EPStatement statement) {
        int result = 0; // default 0
        if (statement != null) {
            Annotation[] annotations = statement.getAnnotations();
            for(Annotation anno : annotations) {
                if (!(anno instanceof Priority)) continue;
                result = ((Priority)anno).value();
            }
        }

        return result;
    }
}
