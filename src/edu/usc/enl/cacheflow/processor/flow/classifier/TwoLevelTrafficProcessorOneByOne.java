package edu.usc.enl.cacheflow.processor.flow.classifier;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.model.rule.action.ForwardAction;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/16/12
 * Time: 6:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class TwoLevelTrafficProcessorOneByOne {
    private RuleClassifier classifierPartition;
    public final Map<Rule, Partition> RulePartitionMap;

    public TwoLevelTrafficProcessorOneByOne(RuleClassifier classifierPartition, Collection<Partition> partitions) {
        super();
        this.classifierPartition = classifierPartition;
        for (Partition partition : partitions) {
            Collections.sort((List<Rule>) partition.getRules(), Rule.PRIORITY_COMPARATOR);
        }

        int i = 0;
        //create forwarding rules
        RulePartitionMap = new HashMap<Rule, Partition>(partitions.size(), 1);
        int j = -1;
        for (Partition partition : partitions) {
            final Rule forwardingRule = new Rule(DenyAction.getInstance(), partition.getProperties(), 0, j--);
            RulePartitionMap.put(forwardingRule, partition);
        }

        classifierPartition.setRules(RulePartitionMap.keySet());
    }

    public Rule classify(Flow flow) {
        final Rule partitionFlowMap = classifierPartition.classify(flow);
        final Partition partition1 = RulePartitionMap.get(partitionFlowMap);
        for (Rule rule : partition1.getRules()) {
            if (rule.match(flow)) {
                return rule;
            }
        }
        return null;
    }

}
