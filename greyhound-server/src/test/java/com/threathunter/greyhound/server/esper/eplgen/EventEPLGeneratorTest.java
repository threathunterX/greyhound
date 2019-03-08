package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.variable.meta.EventVariableMeta;
import org.junit.Test;

/**
 * Created by daisy on 17/8/25.
 */
public class EventEPLGeneratorTest {
    @Test
    public void testEventEPL() {
        System.out.println(new EventEPLGenerator().generateEPL(new VariableMetaWrapper<>((EventVariableMeta) VariableMetaTestHelper.getMetaByName("HTTP_DYNAMIC"), null)));
    }

}
