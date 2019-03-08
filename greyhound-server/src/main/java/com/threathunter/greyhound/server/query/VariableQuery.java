package com.threathunter.greyhound.server.query;

import java.util.concurrent.TimeUnit;

/**
 * Created by daisy on 17/2/12.
 */
public interface VariableQuery {
    Object waitForResults(long timeout, TimeUnit unit);
}
