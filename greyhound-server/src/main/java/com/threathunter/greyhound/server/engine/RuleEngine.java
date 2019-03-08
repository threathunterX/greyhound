package com.threathunter.greyhound.server.engine;

import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.model.Event;

import java.util.List;

/**
 * Created by daisy on 17-10-29
 */
public interface RuleEngine {

    void sendEvent(Event event);

    void updateRules(List<VariableMetaWrapper> metas);

    void start();

    void stop();

    boolean isIdle();
}
