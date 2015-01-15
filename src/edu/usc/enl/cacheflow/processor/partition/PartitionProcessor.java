package edu.usc.enl.cacheflow.processor.partition;

import edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.RuleSpacePartitioner;
import edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.partitioner.Partitioner;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.Util;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/13/11
 * Time: 10:09 AM
 * To change this template use File | Settings | File Templates.
 */
public class PartitionProcessor {
    private final Partitioner partitioner;
    private final int maxPartitionSize;
    private Aggregator aggregator;
    private Aggregator.AggregatorFactory aggregatorFactory;

    public PartitionProcessor( Partitioner partitioner, int maxPartitionSize,
                              Aggregator aggregator) {
        this.partitioner = partitioner;
        this.maxPartitionSize = maxPartitionSize;
        this.aggregator = aggregator;
    }

    public PartitionProcessor( Partitioner partitioner, int maxPartitionSize,
                              Aggregator.AggregatorFactory aggregatorFactory) {
        this.partitioner = partitioner;
        this.maxPartitionSize = maxPartitionSize;
        this.aggregatorFactory = aggregatorFactory;
    }

    public List<Partition> process(List<Rule> rules) throws Exception {
        if (rules.size() == 0) {
            return new LinkedList<Partition>();
        }
        //select the algorithm
        //run
        RuleSpacePartitioner ruleSpacePartitioner;
        if (aggregatorFactory != null) {
            ruleSpacePartitioner = new RuleSpacePartitioner(Util.getDimensionInfos(), partitioner, aggregatorFactory);
            return ruleSpacePartitioner.partitionThread(maxPartitionSize, rules);
        } else {
            ruleSpacePartitioner = new RuleSpacePartitioner(Util.getDimensionInfos(), partitioner, aggregator);
            return ruleSpacePartitioner.partition(maxPartitionSize, rules);
        }
    }
}
