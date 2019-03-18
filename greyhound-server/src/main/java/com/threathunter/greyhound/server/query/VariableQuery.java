package com.threathunter.greyhound.server.query;

import java.util.concurrent.TimeUnit;

/**
 * 
 */
public interface VariableQuery {
    Object waitForResults(long timeout, TimeUnit unit);
}
