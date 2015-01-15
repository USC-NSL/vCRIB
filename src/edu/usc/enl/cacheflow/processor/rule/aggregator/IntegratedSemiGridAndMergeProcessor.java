package edu.usc.enl.cacheflow.processor.rule.aggregator;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PartitionTree2;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.Processor;
import edu.usc.enl.cacheflow.util.Util;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/19/11
 * Time: 9:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class IntegratedSemiGridAndMergeProcessor extends Aggregator {
    public IntegratedSemiGridAndMergeProcessor(Collection<Rule> input) {
        super(input);
    }

    public IntegratedSemiGridAndMergeProcessor(Processor<?, Collection<Rule>> processorInput) {
        super(processorInput);
    }

    @Override
    public Collection<Rule> process(Collection<Rule> rules) throws Exception {
        PartitionTree2 partitionTree = new PartitionTree2();
        if (rules.size()==0){
            System.out.println();
        }
        partitionTree.semigridAndMergeTogether(rules,Util.getDimensionInfos(), PartitionTree2.findPermutation(rules, Util.getDimensionInfos()));
        return partitionTree.getRules();
    }
}