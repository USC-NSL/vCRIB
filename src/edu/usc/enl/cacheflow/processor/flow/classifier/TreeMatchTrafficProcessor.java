package edu.usc.enl.cacheflow.processor.flow.classifier;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PartitionTree2;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/16/12
 * Time: 6:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class TreeMatchTrafficProcessor implements RuleClassifier {
    private PartitionTree2 partitionTree2;

    public TreeMatchTrafficProcessor() {
    }

    public TreeMatchTrafficProcessor(Collection<Rule>rules) {
        setRules(rules);
    }

    public Map<Rule, Collection<Flow>> classify(Collection<Flow> flows, Collection<Rule> rules) {

        //create decision tree for forwarding rules no need to merge as forwarding rules are not overlapping
        //setRules(rules);
        //TO MAKE THREAD SAFE
        PartitionTree2 partitionTree2 = new PartitionTree2();
        partitionTree2.semigridAndMergeTogether(rules, Util.getDimensionInfos(), PartitionTree2.findPermutation(rules, Util.getDimensionInfos()));

        //find flows of each partition
        Map<Rule, Collection<Flow>> flowsOfRule = new HashMap<Rule, Collection<Flow>>();
        for (Rule rule : rules) {
            flowsOfRule.put(rule, new LinkedList<Flow>());
        }

        //classify flows on partitions
        //final Map<Rule, List<Flow>> ruleListMap = Util.CalculateRuleTrafficMap(flows, rules);
        for (Flow flow : flows) {
            final Rule rule = partitionTree2.getRule(flow, true);

            /*if (!ruleListMap.get(rule).contains(flow)){
                System.out.println(flow+ " "+rule);
               partitionTree2.getRule(flow);
            }*/

            flowsOfRule.get(rule).add(flow);
        }


        /*int sum=0;
        for (Map<Rule, List<Flow>> ruleListMap : partitionTraffic.values()) {
            for (Map.Entry<Rule, List<Flow>> entry : ruleListMap.entrySet()) {
                if (entry.getKey().getPriority()==3734){
                    sum+=entry.getValue().size();
                }
            }
        }
        System.out.println(sum);*/

        return flowsOfRule;
    }

    public void setRules(Collection<Rule> rules) {
        partitionTree2 = new PartitionTree2();
        partitionTree2.semigridAndMergeTogether(rules, Util.getDimensionInfos(), PartitionTree2.findPermutation(rules, Util.getDimensionInfos()));
    }

    public RuleClassifier cloneNew() {
        return new TreeMatchTrafficProcessor();
    }

    public Rule classify(Flow flow) {
        return partitionTree2.getRule(flow, true);
    }
}
