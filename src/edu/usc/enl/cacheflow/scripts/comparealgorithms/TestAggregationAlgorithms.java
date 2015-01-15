package edu.usc.enl.cacheflow.scripts.comparealgorithms;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.IntegratedSemiGridAndMergeProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveCoveredRulesProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.liu.ACLCompressionProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.patch.PatchMergeProcessor;
import edu.usc.enl.cacheflow.processor.statistics.RulesStatisticsProcessor;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/13/11
 * Time: 4:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestAggregationAlgorithms {
    public static void main(String[] args) {
        ///////////////////////////////////////////// PARAMETERS

        File ruleSpec = new File("input/ruletemplaterandomlarge.txt");
        final int paramSets = 5;
//        int[] numberOfRulesChoices = new int[]{2, 4, 8, 16, 32, 64};
        int[] numberOfRulesChoices = new int[]{32, 64, 128, 256, 512, 1024, 2048};

        ///////////////////////////////////////////// SIMULATION
        try {
            int j = 0;
            for (int numberOfRules : numberOfRulesChoices) {
                System.out.println(numberOfRules);

                /////////////////////////////////////// INITIALIZATION
                List<Statistics> stats = new LinkedList<Statistics>();

                // create output directory
                String baseOutputFolder = "output/aggregate/" + numberOfRules;
                File baseOutputFolderFile = new File(baseOutputFolder);
                if (!baseOutputFolderFile.exists()) {
                    boolean mkdirs = baseOutputFolderFile.mkdirs();
                    if (!mkdirs) {
                        System.out.println("cannot create " + baseOutputFolderFile.getAbsolutePath());
                        System.exit(1);
                    }
                }

                Map<String, Object> parameters = new HashMap<String, Object>();
                ///////////////////////////////////// RUN EACH CONFIG MULTIPLE TIMES
                for (int i = 0; i < paramSets; i++) {
                    System.out.println(i);

                    //load or create rules
//                    BufferProcessor<List<Rule>> randomRuleProcessor = new BufferProcessor<List<Rule>>(
//                            new RandomRuleProcessor(infos, numberOfRules, new Random(System.currentTimeMillis())));
                    Collection<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()),
                            "input/classbench/converted/classbench_" + numberOfRules + "," + (i + 1) + ".txt"
                            , parameters, new HashSet<Rule>());

                    parameters.put("topology.aggregation", "Remove Covered");
                    stats.add(new RulesStatisticsProcessor(new RemoveCoveredRulesProcessor(rules), parameters).run());
                    System.out.println("Remove Covered");

                    parameters.put("topology.aggregation", "Patch Semi-Grid and Merge Together");
                    stats.add(new RulesStatisticsProcessor(new PatchMergeProcessor(new IntegratedSemiGridAndMergeProcessor(rules)), parameters).run());
                    System.out.println("Patch Semi-Grid and Merge Together");

                    parameters.put("topology.aggregation", "ACL compression");
                    stats.add(new RulesStatisticsProcessor(new ACLCompressionProcessor(rules), parameters).run());
                    System.out.println("ACL compression");

                    /*parameters.put("Algorithm", "TCAM compression");
                    stats.add(new RulesStatisticsProcessor(new TCAMCompressionProcessor(randomRuleProcessor), parameters).run());
                    System.out.println("TCAM compression");*/
                }

                //////////////////////////////////////// STATISTICS
                //categorize statistics
                Map<List<Statistics.Parameter>, List<Statistics>> categorizedStatistics = Statistics.categorize(Arrays.asList("Algorithm"), stats);
                boolean append = j > 0;

                Util.writeFile(Statistics.csvStatistics(parameters.keySet(), Statistics.categorize(parameters.keySet(), stats),
                        stats.get(0).getStatNames(), true, !append), new File("output/aggregate/aggregate.txt"), append);

                j++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

