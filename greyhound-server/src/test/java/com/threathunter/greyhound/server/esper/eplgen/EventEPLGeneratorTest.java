package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.variable.meta.EventVariableMeta;
import org.junit.Test;

/**
 * 
 */
public class EventEPLGeneratorTest {
    @Test
    public void testEventEPL() {
        System.out.println(new EventEPLGenerator().generateEPL(new VariableMetaWrapper<>((EventVariableMeta) VariableMetaTestHelper.getMetaByName("HTTP_DYNAMIC"), null)));
    }

}
