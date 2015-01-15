package edu.usc.enl.cacheflow.scripts.preliminaries.rule;

import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.BreakPrefixWildcardProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveCoveredRulesProcessor;
import edu.usc.enl.cacheflow.processor.rule.generator.ClassBenchRuleGenerator;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/26/11
 * Time: 5:18 P
 */
public class ConvertAllClassbenchRules {


    public static void main(String[] args) {
        String inputFolder = args[0];
        String outputFolder = args[1];
        String[] numRules = args[2].split(",");
        int paramSets = Integer.parseInt(args[3]);

        new File(outputFolder).mkdirs();

        Map<String, Object> parameters = new HashMap<String, Object>();
        int randomSeedIndex = 0;
        Util.setRandom(randomSeedIndex);
        parameters.put("randomSeedIndex",randomSeedIndex);
        try {
            for (String numRule : numRules) {
                for (int j = 1; j <= paramSets; j++) {
                    Rule.resetMaxId();
                    String inputFile = inputFolder+"/classbench_" + numRule + "," + j + ".txt";
                    String outputFile = outputFolder+"/classbench_" + numRule + "_" + j + ".txt";
                    parameters.put("rule.num", numRule);
                    parameters.put("rule.paramSet", j);
                    parameters.put("rule.generator", "ClassBench");
                    System.out.println(outputFile);
                    final LinkedList<String> strings = Util.loadFile(new File(inputFile));
                    final Collection<Rule> rules = ClassBenchRuleGenerator.generate(strings, Util.random);
                    final List<Rule> brokenRules = BreakPrefixWildcardProcessor.process(rules);
                    final Collection<Rule> aggregatedRules = new RemoveCoveredRulesProcessor(brokenRules).run();
                    int id=1;
                    for (Rule aggregatedRule : aggregatedRules) {
                        aggregatedRule.setId(id++);
                    }
                    WriterSerializableUtil.writeFile(aggregatedRules, new File(outputFile), false, parameters);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
