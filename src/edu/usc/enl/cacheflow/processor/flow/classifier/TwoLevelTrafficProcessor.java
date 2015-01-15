package edu.usc.enl.cacheflow.processor.flow.classifier;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/16/12
 * Time: 6:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class TwoLevelTrafficProcessor implements PartitionClassifier {
    private RuleClassifier classifierPartition;
    private RuleClassifier classifierRule;

    public TwoLevelTrafficProcessor(RuleClassifier classifierPartition,
                                    RuleClassifier classifierRule) {
        super();
        this.classifierPartition = classifierPartition;
        this.classifierRule = classifierRule;
    }

    public Map<Partition, Map<Rule, Collection<Flow>>> classify(Collection<Flow> flows, Collection<Partition> partitions) {

        List<Rule> rules = new LinkedList<Rule>();
        int i = 0;
        //create forwarding rules
        Map<Rule, Partition> RulePartitionMap = new HashMap<Rule, Partition>(partitions.size(), 1);
        int j = -1;
        for (Partition partition : partitions) {
            final Rule forwardingRule = new Rule(DenyAction.getInstance(), partition.getProperties(), 0, j--);
            rules.add(forwardingRule);
            RulePartitionMap.put(forwardingRule, partition);
        }

        final Map<Rule, Collection<Flow>> partitionFlowMap = classifierPartition.classify(flows, rules);

        Map<Partition, Map<Rule, Collection<Flow>>> partitionTraffic = new HashMap<Partition, Map<Rule, Collection<Flow>>>();
        Map<Rule,Collection<Flow>> emptyMap = new HashMap<Rule, Collection<Flow>>();
        for (Partition partition : partitions) {
            partitionTraffic.put(partition,emptyMap);
        }
        for (Map.Entry<Rule, Collection<Flow>> entry : partitionFlowMap.entrySet()) {
            Partition partition = RulePartitionMap.get(entry.getKey());
            final Map<Rule, Collection<Flow>> rulesFlowMap = classifierRule.classify(entry.getValue(), partition.getRules());
            partitionTraffic.put(partition, rulesFlowMap);
        }
        return partitionTraffic;
    }



}
