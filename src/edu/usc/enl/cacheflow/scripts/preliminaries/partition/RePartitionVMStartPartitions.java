package edu.usc.enl.cacheflow.scripts.preliminaries.partition;

import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.BipartitePartitionMultiModel;
import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.partitioner.HalfNoSplitMinCutPartitioner;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.partition.UnifiedPartitionWriter;
import edu.usc.enl.cacheflow.util.Util;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/17/12
 * Time: 11:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class RePartitionVMStartPartitions {
    public static void main(String[] args) throws IOException {
        final List<Partition> partitions;
        final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
        int partitionSize = 4076;
        String partitionFile = "input\\nsdi\\partitionclassbench\\vmstart\\uniform\\-1\\20480_classbench_131072_4.txt";
        String partition2File = "input\\nsdi\\partitionclassbench\\vmstart\\uniform\\-1\\" + partitionSize + "\\20480_classbench_131072_4.txt";
        new File(partition2File).getParentFile().mkdirs();
        HashMap<String, Object> parameters = new HashMap<String, Object>();
        partitions = Util.loadFile(partitionFactory, partitionFile, parameters,new LinkedList<Partition>());
        final double addWildCardWeight = 0;
        final double balanceWeight = 0.1;
        final HalfNoSplitMinCutPartitioner partitioner = new HalfNoSplitMinCutPartitioner(
                balanceWeight, addWildCardWeight);
        parameters.put("repartition.alg", partitioner);
        parameters.put("repartition." + partitioner + ".balanceWeight", balanceWeight);
        parameters.put("repartition." + partitioner + ".addWildCardWeight", balanceWeight);
        try {
            final Collection<Partition> partitions2 = new BipartitePartitionMultiModel(partitioner, partitionSize,
                    false).sequentialPartition(partitions, partitionFactory.getRules());
            parameters.put("partition.num", partitions2.size());
            final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(partition2File)));
            new UnifiedPartitionWriter().write(partitions2, writer, parameters);
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
