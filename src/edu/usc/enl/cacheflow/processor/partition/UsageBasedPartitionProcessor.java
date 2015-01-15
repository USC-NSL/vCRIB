package edu.usc.enl.cacheflow.processor.partition;

import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.BipartitePartitioner;
import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.usagebased.UsageBasedPartition;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.Processor;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/30/12
 * Time: 10:58 AM
 * To change this template use File | Settings | File Templates.
 */
public class UsageBasedPartitionProcessor  {
    private final int maxPartitionSize;
    private BipartitePartitioner partitioner;

    public UsageBasedPartitionProcessor(Processor<?, List<Rule>> ruleProcessor,
                                        Processor<?,List<Flow>> flowProcessor,
                                        int maxPartitionSize,
                                        BipartitePartitioner partitioner) {
        this.maxPartitionSize = maxPartitionSize;
        this.partitioner = partitioner;
    }

    protected List<Partition> processRequirements(List<Rule> rules, List<Flow> flows) throws Exception {
        if (rules.size() == 0) {
            return new LinkedList<Partition>();
        }
        //select the algorithm
        //run
        UsageBasedPartition ruleSpacePartitioner = new UsageBasedPartition(partitioner,flows);
        //return ruleSpacePartitioner.partition(rules, maxPartitionSize);
        return ruleSpacePartitioner.sequentialPartition(rules, maxPartitionSize);
    }
}
