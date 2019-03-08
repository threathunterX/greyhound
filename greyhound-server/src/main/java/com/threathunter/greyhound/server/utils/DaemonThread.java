package com.threathunter.greyhound.server.utils;

import java.util.concurrent.ThreadFactory;

/**
 * Created by daisy on 17-11-12
 */
public enum  DaemonThread implements ThreadFactory {
    INSTANCE;

    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    }

}
