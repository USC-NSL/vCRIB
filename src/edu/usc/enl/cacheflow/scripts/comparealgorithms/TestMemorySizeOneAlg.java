package edu.usc.enl.cacheflow.scripts.comparealgorithms;

import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.BipartitePartitionMultiModel;
import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.partitioner.HalfNoSplitMinCutPartitioner;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.statistics.PartitionStatisticsProcessor;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/26/11
 * Time: 8:06 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestMemorySizeOneAlg {


    public static void main(String[] args) {
        ///////////////////////////////////////////// PARAMETERS
        int paramSet = 4;
        File ruleSpec = new File("input/ruletemplaterandomlarge.txt");
        //
        int[] numberOfRulesChoices = new int[]{4096, 8192, 16384, 32768};
        //
        int[] partitionSizes = new int[]{16, 64, 256, 1024};

        ///////////////////////////////////////////// SIMULATION
        try {
            /////////////////////////////////////// INITIALIZATION
            //List<DimensionInfo> infos = StringRuleGenerator.extractHeaderWithComment(Util.loadFile(ruleSpec));

            List<Statistics> stats = new LinkedList<Statistics>();
            int j = 0;
            for (int numberOfRules : numberOfRulesChoices) {

                // create output directory
                String baseOutputFolder = "output/partitionSizeOneAlg3/" + numberOfRules;
                File baseOutputFolderFile = new File(baseOutputFolder);
                if (!baseOutputFolderFile.exists()) {
                    boolean mkdirs = baseOutputFolderFile.mkdirs();
                    if (!mkdirs) {
                        System.out.println("cannot create " + baseOutputFolderFile.getAbsolutePath());
                        System.exit(1);
                    }
                }


                ///////////////////////////////////// RUN EACH CONFIG MULTIPLE TIMES
                Map<String, Object> parameters = new HashMap<String, Object>();
                for (int i = 3; i < paramSet; i++) {
                    System.out.println(i + " out of " + paramSet + " for " + numberOfRules);
                    //load or create rules
                    Collection<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()),
                            "input/hotcloud/classbenchrules/converted/classbench_" + numberOfRules + "_" + (i + 1) + ".txt"
                            , parameters,new HashSet<Rule>());
                    //save a copy
                    //new SaveFileProcessor<String>(new RuleToStringProcessor(randomRuleProcessor), new File(baseOutputFolder + "/rules" + i + ".txt"), false).run();

                    for (int partitionNum : partitionSizes) {
                        System.out.println(partitionNum);
                        parameters.put("partition.num", partitionNum);


                        //parameters.put("Merge Algorithm", "NoCov");
                        //stats.add(new PartitionStatisticsProcessor(new PartitionProcessor(rules, partitioner, maxPartitionSize, new ACLCompressionProcessor(EMPTY_LIST)), parameters).run());

                        /*parameters.put("PartitionMethod", "Old");
                        parameters.put("weight",0);
                        parameters.put("area",100);
                        System.out.println(Statistics.getParameterLine(parameters));
                        stats.add(new PartitionStatisticsProcessor(new PartitionProcessor(rules, partitioner, maxPartitionSize, new ProcessorFactory<Processor<List<Rule>, List<Rule>>>() {
                            @Override
                            public Processor<List<Rule>, List<Rule>> getProcessor() {
                                return new RemoveCoveredRulesProcessor(Util.EMPTY_LIST);
                                //new ACLCompressionProcessor(EMPTY_LIST);
                            }
                        }), parameters).run());
                          */
                        /* parameters.put("PartitionMethod", "Balanced");
               parameters.put("weight",0);
               parameters.put("area",100);
               System.out.println(Statistics.getParameterLine(parameters));

               stats.add(new PartitionStatisticsProcessor(new BipartiteMemoryPartitionProcessor(rules,
                       new RuleBalancedPartitioner(new RemoveCoveredRulesProcessor(Util.EMPTY_LIST)),6,memory)
                       , parameters).run());*/


                        parameters.put("partition.alg", "MinCut");
                        {
                            parameters.put("partition.mincut.weight", 0);
                            System.out.println(Statistics.getParameterLine(parameters));
                            final List<Partition> partitions = new BipartitePartitionMultiModel(
                                    new HalfNoSplitMinCutPartitioner(0, 0), partitionNum, true).sequentialPartition(rules);
                            stats.add(new PartitionStatisticsProcessor(partitions, parameters).run());
                        }
                        {
                            parameters.put("partition.mincut.weight", 0.1);
                            System.out.println(Statistics.getParameterLine(parameters));
                            final List<Partition> partitions = new BipartitePartitionMultiModel(
                                    new HalfNoSplitMinCutPartitioner(0.1, 0), partitionNum, true).sequentialPartition(rules);
                            stats.add(new PartitionStatisticsProcessor(partitions, parameters).run());
                        }
                        {
                            parameters.put("partition.mincut.weight", 0.25);
                            final List<Partition> partitions = new BipartitePartitionMultiModel(
                                    new HalfNoSplitMinCutPartitioner(0.25, 0), partitionNum, true).sequentialPartition(rules);
                            stats.add(new PartitionStatisticsProcessor(partitions, parameters).run());
                        }
                    }
                }


                //////////////////////////////////////// STATISTICS
                //categorize statistics
                boolean append = j > 0;
                Util.writeFile(Statistics.csvStatistics(parameters.keySet(),
                        Statistics.categorize(parameters.keySet(), stats),
                        stats.get(0).getStatNames(), true, !append), new File("output/partitionSizeOneAlg/partitioner.csv"), append);
                stats.clear();
                j++;
            }
        } catch (IOException e) {
            e.printStackTrace();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
