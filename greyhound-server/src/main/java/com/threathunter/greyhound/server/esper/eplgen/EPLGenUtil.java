package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.common.Identifier;
import com.threathunter.model.EventMeta;
import com.threathunter.model.Property;
import com.threathunter.model.VariableMeta;

import java.util.List;

/**
 * Utility class that helps simplify the generator code.
 *
 * @author Wen Lu
 */
public class EPLGenUtil {
    /**
     * Return the qualified property name in epl.
     *
     * <p>In epl, different variables/events may have properties with the same
     * name, so we need to use the variable/event name as the qualifier.
     * <br>The variable/event name is the last key of the property identifier.
     *
     * @param p
     */
    public static String genQualifiedName(Property p) {
        StringBuilder sb = new StringBuilder();
        boolean isValue = p.getName().equals("value");
        if (isValue) {
            sb.append("coalesce(");
        }
        if (p.getIdentifier() != null) {
            List<String> keysOfId = p.getIdentifier().getKeys();
            String lastKey = keysOfId.get(keysOfId.size()-1);
            sb.append(lastKey).append(".");
        }
        sb.append(p.getName());
        if (isValue) {
            sb.append(", 0)");
        }
        return sb.toString();
    }

    public static String genQualifiedValue(Property p) {
        StringBuilder sb = new StringBuilder();
        sb.append("coalesce(");
        if (p.getIdentifier() != null) {
            List<String> keysOfId = p.getIdentifier().getKeys();
            String lastKey = keysOfId.get(keysOfId.size()-1);
            sb.append(lastKey).append(".");
        }
        sb.append("value");
        sb.append(", 0)");
        return sb.toString();
    }

    /**
     * The following four methods generate internal name used in esper
     * for variable/event.
     *
     */
    public static String genVariableName(Identifier variableId) {
        return variableId.getKeys().get(1);
    }

    public static String genVariableName(VariableMeta variable) {
        return variable.getName();
    }

    public static String genEventName(Identifier eventId) {
        return eventId.getKeys().get(1);
    }

    public static String genEventName(EventMeta event) {
        return event.getName();
    }
}
