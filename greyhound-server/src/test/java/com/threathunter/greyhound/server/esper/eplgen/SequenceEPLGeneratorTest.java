package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.variable.meta.SequenceVariableMeta;
import org.junit.Test;

/**
 * Created by daisy on 17-10-8
 */
public class SequenceEPLGeneratorTest {
    @Test
    public void testSequence() {
        System.out.println(new SequenceEPLGenerator().generateEPL(new VariableMetaWrapper<>((SequenceVariableMeta) VariableMetaTestHelper.getMetaByName("ip_click_diff"), null)));
    }
}
