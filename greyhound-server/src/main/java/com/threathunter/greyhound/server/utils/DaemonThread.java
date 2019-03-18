package com.threathunter.greyhound.server.utils;

import java.util.concurrent.ThreadFactory;

/**
 * 
 */
public enum  DaemonThread implements ThreadFactory {
    INSTANCE;

    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    }

}
