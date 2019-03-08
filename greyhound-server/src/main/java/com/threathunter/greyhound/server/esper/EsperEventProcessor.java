package com.threathunter.greyhound.server.esper;

import com.threathunter.model.Event;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author Wen Lu
 */
public class EsperEventProcessor implements Runnable {

    private Queue<Event> processingEvents = new ArrayBlockingQueue<>(1000);

    @Override
    public void run() {
    }
}
