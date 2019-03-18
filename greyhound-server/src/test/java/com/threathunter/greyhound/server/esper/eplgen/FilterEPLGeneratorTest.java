package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.variable.meta.FilterVariableMeta;
import org.junit.Test;

/**
 * 
 */
public class FilterEPLGeneratorTest {
    @Test
    public void testFilterEPL() {
        System.out.println(new FilterEPLGenerator().generateEPL(new VariableMetaWrapper<>((FilterVariableMeta) VariableMetaTestHelper.getMetaByName("HTTP_CLICK"), null)));
    }

    @Test
    public void testMapValueFilterEPL() {
        System.out.println(new FilterEPLGenerator().generateEPL(new VariableMetaWrapper<>((FilterVariableMeta) VariableMetaTestHelper.getMetaByName("__http_trigger__"), null)));
    }
}
