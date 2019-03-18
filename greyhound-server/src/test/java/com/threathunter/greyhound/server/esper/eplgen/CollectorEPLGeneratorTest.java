package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.variable.meta.CollectorVariableMeta;
import org.junit.Test;

/**
 * 
 */
public class CollectorEPLGeneratorTest {
    @Test
    public void testCollector() {
        System.out.println(new CollectorEPLGenerator().generateEPL(new VariableMetaWrapper<>((CollectorVariableMeta) VariableMetaTestHelper.getMetaByName("__strategy_collector__"), null)));
    }
}
