package edu.usc.enl.cacheflow.processor.rule.aggregator;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PartitionTree2;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.Processor;
import edu.usc.enl.cacheflow.util.Util;

import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/11/11
 * Time: 11:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class SemiGridAndMergeProcessor extends Aggregator {
    public SemiGridAndMergeProcessor(Collection<Rule> input) {
        super(input);
    }

    public SemiGridAndMergeProcessor(Processor<?, Collection<Rule>> processorInput) {
        super(processorInput);
    }

    @Override
    public Collection<Rule> process(Collection<Rule> rules) throws Exception {
        PartitionTree2 partitionTree = new PartitionTree2();
        partitionTree.semigridAndMerge(rules,Util.getDimensionInfos(), PartitionTree2.findPermutation(rules, Util.getDimensionInfos()));
        return partitionTree.getRules();
    }
}
