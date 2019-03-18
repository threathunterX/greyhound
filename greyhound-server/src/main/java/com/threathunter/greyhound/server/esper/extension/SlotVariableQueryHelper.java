package com.threathunter.greyhound.server.esper.extension;

import com.threathunter.common.Identifier;
import com.threathunter.greyhound.server.batch.BatchModeContainer;

/**
 * 
 */
public class SlotVariableQueryHelper {

    public static double getVariableData(String variableName, String... keys) {
        if (variableName == null || variableName.isEmpty()) {
            return 0.0;
        }

        return BatchModeContainer.SlotContainerHolder.getInstance().querySlot(variableName, keys);
    }

    public static String genEPLExpression(Identifier identifier, String joinKeysExpression) {
        return String.format("com.threathunter.greyhound.server.esper.extension.SlotVariableQueryHelper.getVariableData('%s', %s)", identifier.getKeys().get(1), joinKeysExpression);
    }
}
