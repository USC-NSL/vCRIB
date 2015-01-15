package edu.usc.enl.cacheflow.scripts.preliminaries.rule;

import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.generator.PrefixRandomRuleGenerator;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/1/12
 * Time: 5:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateRandomRules {
    public static void main(String[] args) throws Exception {
        int numRules =200 * 1024;
        int numRuleSets = 5;
        String outputFolder = "input/nsdi/randomrules";
        new File(outputFolder).mkdirs();

        List<DimensionInfo> infos = new ArrayList<DimensionInfo>(5);
        infos.add(Util.SRC_IP_INFO);
        infos.add(Util.DST_IP_INFO);
        infos.add(Util.PROTOCOL_INFO);
        infos.add(Util.SRC_PORT_INFO);
        infos.add(Util.DST_PORT_INFO);
        Util.setDimensionInfos(infos);



        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("rule.num", numRules);
        parameters.put("rule.generator", "Random");
        for (int i = 0; i < numRuleSets; i++) {
            System.out.println("Start "+i);
            Util.setRandom(i);
            parameters.put("randomSeedIndex", i);
            Collection<Rule> rules = new PrefixRandomRuleGenerator(Util.random, infos, numRules).generateRules();
            WriterSerializableUtil.writeFile(rules, new File(outputFolder + "/" + i + "_" + numRules + ".txt"), false, parameters);
        }
    }
}
