package com.threathunter.greyhound.server.esper.eplgen.strategy;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.GreyhoundServer;
import com.threathunter.greyhound.server.engine.EngineConfiguration;
import com.threathunter.greyhound.server.esper.eplgen.GreyhoundMetaHelper;
import com.threathunter.model.*;
import com.threathunter.variable.DimensionType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by daisy on 17-11-26
 */
public class VargenDemoTest {
    private GreyhoundServer greyhoundServer;

    @BeforeClass
    public static void globalSetup() throws IOException {
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();
        CommonDynamicConfig.getInstance().addOverrideProperty("babel_server", "redis");

        VariableMetaRegistry.getInstance().updateVariableMetas(new ArrayList<>());
        GreyhoundMetaHelper.loadEvents("events.json");
        GreyhoundMetaHelper.loadVariables("output.json");
    }

    @Test
    public void test() throws InterruptedException {
        System.out.println("variable is ok");

        EngineConfiguration configuration = new EngineConfiguration();
        configuration.setBatchModeDimensions(null);
        configuration.setEnableDimensions(new HashSet<>(Arrays.asList(DimensionType.IP, DimensionType.UID, DimensionType.DID)));
        configuration.setRedisBabel(true);

        List<VariableMeta> metas = VariableMetaRegistry.getInstance().getAllVariableMetas();
        greyhoundServer = new GreyhoundServer(configuration, metas);
        greyhoundServer.start();

        Thread.sleep(60 * 1000);

        greyhoundServer.stop();
    }
}
