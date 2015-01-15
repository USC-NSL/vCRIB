package edu.usc.enl.cacheflow.processor.partition;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/13/12
 * Time: 12:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class UnifyPartitions {
    public Collection<Partition> unify(Collection<Partition> partitions) {
        Map<Rule, Rule> rules = new HashMap<Rule, Rule>();
        List<Partition> outputPartitions = new ArrayList<Partition>(partitions.size());
        for (Partition partition : partitions) {
            List<Rule> newPartitionRules = new LinkedList<Rule>();
            for (Rule rule : partition.getRules()) {
                final Rule oldRule = rules.get(rule);
                if (oldRule == null) {
                    rule.setId(rules.size()+1);
                    rules.put(rule, rule);
                    newPartitionRules.add(rule);
                } else {
                    newPartitionRules.add(oldRule);
                }
            }
            outputPartitions.add(new Partition(newPartitionRules, partition.getProperties()));
        }
        return outputPartitions;
    }
}
