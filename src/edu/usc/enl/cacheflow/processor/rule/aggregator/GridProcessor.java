package edu.usc.enl.cacheflow.processor.rule.aggregator;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PartitionTree2;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.Processor;
import edu.usc.enl.cacheflow.util.Util;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/11/11
 * Time: 11:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class GridProcessor extends Aggregator{
    public GridProcessor(Collection<Rule> input) {
        super(input);
    }

    public GridProcessor(Processor<?, Collection<Rule>> processorInput) {
        super(processorInput);
    }

    @Override
    public Collection<Rule> process(Collection<Rule> input) throws Exception {
        PartitionTree2 partitionTree = new PartitionTree2();
        partitionTree.grid0(input,Util.getDimensionInfos(), PartitionTree2.findPermutation(input, Util.getDimensionInfos()));
        return partitionTree.getRules();
    }
}
