package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.partitioner;

import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.BipartitePartitioner;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/1/12
 * Time: 10:25 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class RuleBipartitePartitioner extends BipartitePartitioner {
    public RuleBipartitePartitioner(Aggregator aggregator) {
        super(aggregator);
    }

    public abstract void partition(Collection<Rule> rules) throws Exception;
}
