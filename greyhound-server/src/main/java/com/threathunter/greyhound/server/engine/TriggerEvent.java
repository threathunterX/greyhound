package com.threathunter.greyhound.server.engine;

import com.threathunter.model.Event;

/**
 * Created by daisy on 17-11-15
 */
public class TriggerEvent {

    private final String identifier;
    private final Event event;

    public String getIdentifier() {
        return identifier;
    }

    public Event getEvent() {
        return event;
    }

    public TriggerEvent(String identifier, Event event) {
        this.identifier = identifier;
        this.event = event;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TriggerEvent)) {
            return false;
        }
        return identifier.equals(((TriggerEvent) obj).identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }
}
