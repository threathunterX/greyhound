package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.variable.meta.AggregateVariableMeta;
import org.junit.Test;

/**
 * Created by daisy on 17-9-7
 */
public class AggregateEPLGeneratorTest {
    @Test
    public void testAggregate() {
        System.out.println(new AggregationEPLGenerator().generateEPL(new VariableMetaWrapper<>((AggregateVariableMeta) VariableMetaTestHelper.getMetaByName("ip__dynamic_count"), null)));
    }
}
