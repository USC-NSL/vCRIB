package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.usagebased;

import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.BipartitePartitioner;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.ForwardAction;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/29/12
 * Time: 5:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class UsageBasedPartition {

    private BipartitePartitioner bipartitePartitioner;
    private List<Flow> flows;

    public UsageBasedPartition(BipartitePartitioner bipartitePartitioner, List<Flow> flows) {
        this.bipartitePartitioner = bipartitePartitioner;
        this.flows = flows;
    }


    public List<Partition> sequentialPartition(List<Rule> rules0, int maxSize) throws Exception {
        List<Rule> usageRules0 = createUsageRules(flows);

        LinkedList<RulesUsageRulesPair> toBePartitioned = new LinkedList<RulesUsageRulesPair>();
        toBePartitioned.push(new RulesUsageRulesPair(rules0, usageRules0));
        List<Partition> partitions = new ArrayList<Partition>();
        while (toBePartitioned.size() > 0) {
            final RulesUsageRulesPair pair = toBePartitioned.pop();
            Collection<Rule> rules = pair.getRules();
            Collection<Rule> usageRules = pair.getUsageRules();
            if (rules.size() == 0) {
                continue;
            }
            if (rules.size() <= maxSize) {
                partitions.add(new Partition(rules));
                continue;
            }
            ((MinCutUsagePartitioner) bipartitePartitioner).partition(rules, usageRules);
            Collection<Rule> bestPartitionRules1 = bipartitePartitioner.getBestPartitionRules1();
            Collection<Rule> bestPartitionRules2 = bipartitePartitioner.getBestPartitionRules2();
            Collection<Rule> usageRule1 = ((MinCutUsagePartitioner) bipartitePartitioner).getUsageRules1();
            Collection<Rule> usageRule2 = ((MinCutUsagePartitioner) bipartitePartitioner).getUsageRules2();

            if (bestPartitionRules1 != null) {
                //keep only to be partitioned
                if (bestPartitionRules1.size() > maxSize) {
                    toBePartitioned.push(new RulesUsageRulesPair(bestPartitionRules1, usageRule1));
                    //partitions.addAll(partition(bestPartitionRules1, maxSize));
                } else {
                    partitions.add(new Partition(bestPartitionRules1));
                }
                if (bestPartitionRules2.size() > maxSize) {
                    //partitions.addAll(partition(bestPartitionRules2, maxSize));
                    toBePartitioned.push(new RulesUsageRulesPair(bestPartitionRules2, usageRule2));
                } else {
                    partitions.add(new Partition(bestPartitionRules2));
                }
            } else {
                Util.logger.severe("No partition point found");
                System.exit(1);
            }
        }

        return partitions;
    }


    public List<Rule> createUsageRules(List<Flow> flows) {
        if (flows.size() == 0) {
            return null;
        }
        final List<DimensionInfo> dimensionNames = Util.getDimensionInfos();

        Map<Switch, List<RangeDimensionRange>> sourceRules = new HashMap<Switch, List< RangeDimensionRange>>();
        for (Flow flow : flows) {
            List<RangeDimensionRange> ruleProperties = sourceRules.get(flow.getSource());
            if (ruleProperties == null) {
                ruleProperties = new ArrayList<RangeDimensionRange>(dimensionNames.size());
                sourceRules.put(flow.getSource(), ruleProperties);
                int i = 0;
                for (DimensionInfo info : dimensionNames) {
                    final Long property = flow.getProperty(i++);
                    ruleProperties.add(new RangeDimensionRange(property, property, info));
                }
                continue;
            }
            int i = 0;
            for (DimensionInfo dimensionName : dimensionNames) {
                //extend the properties to include the flow
                final RangeDimensionRange ruleProperty = ruleProperties.get(i);
                final Long flowProperty = flow.getProperty(i++);
                ruleProperty.extend(flowProperty);
            }
        }

        List<Rule> newRules = new ArrayList<Rule>(sourceRules.size());
        for (Map.Entry<Switch, List<RangeDimensionRange>> switchPropertiesMapEntry : sourceRules.entrySet()) {
            newRules.add(new Rule(new ForwardAction(switchPropertiesMapEntry.getKey()), switchPropertiesMapEntry.getValue(), 0,Rule.maxId+1));
        }
        return newRules;
    }

    private class RulesUsageRulesPair {
        private Collection<Rule> rules;
        private Collection<Rule> usageRules;

        private RulesUsageRulesPair(Collection<Rule> rules, Collection<Rule> usageRules) {
            this.rules = rules;
            this.usageRules = usageRules;
        }

        public Collection<Rule> getRules() {
            return rules;
        }

        public Collection<Rule> getUsageRules() {
            return usageRules;
        }
    }
}
