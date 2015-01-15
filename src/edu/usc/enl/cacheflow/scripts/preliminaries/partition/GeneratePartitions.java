package edu.usc.enl.cacheflow.scripts.preliminaries.partition;

import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.equalsize.EqualSizePartition;
import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.BipartitePartitionMultiModel;
import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.partitioner.HalfNoSplitMinCutPartitioner;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.partition.UnifiedPartitionWriter;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/20/12
 * Time: 11:34 AM
 * To change this template use File | Settings | File Templates.
 */
public class GeneratePartitions {
    public static void main(String[] args) {
        String inputFolder = args[0];
        String outputFolder = args[1];
        String[] partitionSizes = args[2].split(",");
        String algorithm = args[3];
        final double balanceWeight = 0.1;
        final double addWildCardWeight = 0;

        new File(outputFolder).mkdirs();
        final List<File> files = Arrays.asList(new File(inputFolder).listFiles());
        Collections.sort(files);
        try {
            for (File ruleFile : files) {
                Map<String, Object> parameters = new HashMap<String, Object>();
                Collection<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()),
                        ruleFile.getPath(), parameters,new HashSet<Rule>());
                for (String partitionSize : partitionSizes) {
                    int partitionNum = Integer.parseInt(partitionSize);
                    parameters.put("partition.num", partitionNum);
                    System.out.println(ruleFile.getName() + " to " + partitionNum + " partitions");
                    Collection<Partition> partitions = null;
                    if (algorithm.equalsIgnoreCase("Half")) {
                        final HalfNoSplitMinCutPartitioner partitioner = new HalfNoSplitMinCutPartitioner(
                                balanceWeight, addWildCardWeight);
                        partitions = new BipartitePartitionMultiModel(partitioner, partitionNum, true).sequentialPartition(rules);
                        parameters.put("balanceWeight", balanceWeight);
                        parameters.put("addWildCardWeight", addWildCardWeight);
                    } else if (algorithm.equalsIgnoreCase("Equal")) {
                        final EqualSizePartition partitioner = new EqualSizePartition();
                        partitions = partitioner.sequentialPartition(rules, partitionNum);
                        parameters.put("partition.alg", partitioner);
                    } else {
                        System.err.println("Partition algorithm " + algorithm + " not found");
                        System.exit(1);
                    }
                    final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFolder + "/" + partitionNum + "_" + ruleFile.getName())));
                    new UnifiedPartitionWriter().write(partitions, writer, parameters);
                    writer.close();
                    Runtime.getRuntime().gc();
                }

            }
        } catch (
                Exception e
                )

        {
            e.printStackTrace();
        }
    }
}
