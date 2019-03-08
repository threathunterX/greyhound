package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.model.EventMeta;
import com.threathunter.model.VariableMeta;

/**
 * Generate an expression used in the epl statement.
 *
 * The {@link com.threathunter.model.PropertyCondition}, {@link com.threathunter.model.PropertyMapping}
 * and {@link com.threathunter.model.PropertyReduction} is used in the definition
 * of {@link VariableMeta} or {@link EventMeta},
 * they can contribute a short expression to the epl representation of the related
 * variable or event.
 *
 *
 * @author Wen Lu
 */
public interface EPLExpressionGenerator {
    String generateEPLExpression();
}
