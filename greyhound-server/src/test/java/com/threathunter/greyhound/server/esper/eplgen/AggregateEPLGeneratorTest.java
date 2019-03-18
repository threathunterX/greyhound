package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.variable.meta.AggregateVariableMeta;
import org.junit.Test;

/**
 * 
 */
public class AggregateEPLGeneratorTest {
    @Test
    public void testAggregate() {
        System.out.println(new AggregationEPLGenerator().generateEPL(new VariableMetaWrapper<>((AggregateVariableMeta) VariableMetaTestHelper.getMetaByName("ip__dynamic_count"), null)));
    }
}
