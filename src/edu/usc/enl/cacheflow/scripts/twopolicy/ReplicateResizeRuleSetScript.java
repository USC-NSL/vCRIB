package edu.usc.enl.cacheflow.scripts.twopolicy;

import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 9/13/12
 * Time: 12:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReplicateResizeRuleSetScript {
    public static void main(String[] args) {
        int randomSeedIndex = Integer.parseInt(args[0]);
        String ruleFileName = args[1];
        String outputFileName1 = args[2];
        String outputFileName2 = args[3];
        String outputFileName3 = args[4];

        Util.setRandom(randomSeedIndex);

        try {
            //load rule
            HashMap<String, Object> parameters = new HashMap<>();
            List<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()), ruleFileName, parameters, new ArrayList<Rule>());
            int initRulesSize = rules.size();
            List<RangeDimensionRange> defaultRuleProperties = new ArrayList<>();
            for (DimensionInfo dimensionInfo : Util.getDimensionInfos()) {
                defaultRuleProperties.add(dimensionInfo.getDimensionRange());
            }


            int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
            int dstIPIndex = Util.getDimensionInfoIndex(Util.DST_IP_INFO);
            Collections.shuffle(rules, Util.random);
            long bias = 0;
            {
                List<Rule> firstHalf;
                List<Rule> firstSubList = rules.subList(0, rules.size() / 2);
                firstHalf = new ArrayList<>(firstSubList);
                firstSubList.clear();
                halfBiasSave(outputFileName1, parameters, initRulesSize, defaultRuleProperties, srcIPIndex, dstIPIndex, bias, firstHalf, 0);
            }
            bias = Util.SRC_IP_INFO.getDimensionRange().getSize() / 2;
            {
                halfBiasSave(outputFileName2, parameters, initRulesSize, defaultRuleProperties, srcIPIndex, dstIPIndex, bias, rules, 0);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void halfBiasSave(String outputFileName1, HashMap<String, Object> parameters, int initRulesSize, List<RangeDimensionRange> defaultRuleProperties, int srcIPIndex, int dstIPIndex, long bias, List<Rule> firstHalf, int priorityStart) throws IOException {
        Rule defaultRule = new Rule(DenyAction.getInstance(), defaultRuleProperties, initRulesSize * 2, initRulesSize * 2);
        for (Rule rule : firstHalf) {
            rule.setPriority(priorityStart++);
            RangeDimensionRange srcIP = rule.getProperty(srcIPIndex);
            RangeDimensionRange dstIP = rule.getProperty(dstIPIndex);
            if (srcIP.getStart() % 2 == 1 || dstIP.getStart() % 2 == 1) {
                continue;
            }
            halfIP(srcIP, bias);
            halfIP(dstIP, bias);
        }
        firstHalf.add(defaultRule);
        File file1 = new File(outputFileName1);
        file1.getParentFile().mkdirs();
        WriterSerializableUtil.writeFile(firstHalf, file1, false, parameters);
    }

    private static void halfIP(RangeDimensionRange srcIP, long bias) {
        long size = srcIP.getSize();
        srcIP.setStart(srcIP.getStart() / 2 + bias);
        srcIP.setEnd(srcIP.getStart() + Math.max(1, size / 2)-1);
    }
}
