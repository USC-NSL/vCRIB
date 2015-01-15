package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.partitioner;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/25/12
 * Time: 2:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class RuleBalancedPartitioner extends RuleBipartitePartitioner {
    public RuleBalancedPartitioner(Aggregator aggregator) {
        super(aggregator);
    }

    public void partition(Collection<Rule> rules) throws Exception {
        //find points in each dimension
        int minimumRuleDelta = Integer.MAX_VALUE;
        bestPartitionRules1 = null;
        bestPartitionRules2 = null;
        final List<DimensionInfo> dimensionInfos = Util.getDimensionInfos();
        for (int dim =0; dim< dimensionInfos.size(); dim++) {
            DimensionInfo dimensionInfo= dimensionInfos.get(dim);
            //find median for each dimension
            List<Long> endpointsList = new ArrayList<Long>(rules.size());
            for (Rule rule : rules) {
                final RangeDimensionRange property = rule.getProperty(dim);
                endpointsList.add(property.getStart()-1);
                endpointsList.add(property.getEnd());
            }
            Collections.sort(endpointsList); // can be optimized
            //check the median should be interior point
            int medianIndex = endpointsList.size() / 2;
            long median = endpointsList.get(medianIndex);
            long min = endpointsList.get(0);
            long max = endpointsList.get(endpointsList.size() - 1);
            removeMinMax(endpointsList, min, max);

            if (endpointsList.size() == 0) {//no interior median
                continue;
            }
            if (median == min) {
                median = endpointsList.get(0);
            }
            if (median == max) {
                median = endpointsList.get(endpointsList.size() - 1);
            }

            //partition and get the number of rules
            //create superranges
            List<RangeDimensionRange> superRanges = Arrays.asList(new RangeDimensionRange(min+1, median, dimensionInfo),
                    new RangeDimensionRange(median + 1, max, dimensionInfo));
            final Map<RangeDimensionRange, Collection<Rule>> newRules = Rule.partitionRuleSpace(rules, superRanges, dim);
            final Iterator<Collection<Rule>> iterator = newRules.values().iterator();
            Collection<Rule> rules1 = iterator.next();
            Collection<Rule> rules2 = iterator.next();
            rules1 = aggregate(rules1);
            rules2 = aggregate(rules2);


            int newSizeDelta = Math.abs(rules1.size() - rules2.size());
            if (newSizeDelta < minimumRuleDelta) {
                minimumRuleDelta = newSizeDelta;
                bestPartitionRules1 = rules1;
                bestPartitionRules2 = rules2;
            }
        }
    }
}
