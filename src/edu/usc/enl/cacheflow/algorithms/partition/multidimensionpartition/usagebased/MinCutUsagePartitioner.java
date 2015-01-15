package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.usagebased;

import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.BipartitePartitioner;
import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.partitioner.RuleMinCutPartitioner;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/30/12
 * Time: 9:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class MinCutUsagePartitioner extends RuleMinCutPartitioner {
    private Collection<Rule> usageRules1;
    private Collection<Rule> usageRules2;

    public MinCutUsagePartitioner(Aggregator aggregator, double balanceWeight, double searchArea) {
        super(aggregator, balanceWeight, searchArea);
    }


    public BipartitePartitioner partition(Collection<Rule> originalRules, Collection<Rule> usageRules) throws Exception {
        //find points in each dimension
        List<Rule> rules = new ArrayList<Rule>(originalRules.size() + usageRules.size());
        rules.addAll(originalRules);
        rules.addAll(usageRules);
        bestPartitionRules1 = null;
        bestPartitionRules2 = null;

        long minEndpoint = 0;
        int minEndPointDimension = -1;
        double minWeight = Double.MAX_VALUE;
        long minMin = 0;
        long minMax = 0;

        final List<DimensionInfo> dimensionInfos = Util.getDimensionInfos();
        PreparePoints preparePoints = new PreparePoints(rules);
        for (int dim =0; dim<dimensionInfos.size(); dim++){
            //find median for each dimension
            preparePoints.invoke(dim);
            if (preparePoints.skip()) continue;
            int start = preparePoints.getStart();
            int end = preparePoints.getEnd();
            List<Long> endpointsList = preparePoints.getEndpointsList();
            long min = preparePoints.getMin();
            long max = preparePoints.getMax();

            //count the number of rules that cut each interior endpoint
            for (int i = start; i < end; i++) {
                Long endpoint = endpointsList.get(i);
                int sum = 0;
                int left = 0;
                int right = 0;
                //must use RuleSpacePartitioner.partitionRuleSpace if we want to use aggregation
                for (Rule rule : rules) {
                    final RangeDimensionRange property = rule.getProperty(dim);
                    if (property.getEnd() > endpoint) {
                        right++;
                    }
                    if (property.getStart() <= endpoint) {
                        left++;
                    }
                    if (property.getStart() <= endpoint && property.getEnd() > endpoint) {
                        sum++;
                    }
                }
                final double weight = (1 - balanceWeight) * sum + balanceWeight * (Math.abs(left - right));
                if (minWeight > weight) {
                    minWeight = weight;
                    minEndpoint = endpoint;
                    minEndPointDimension = dim;
                    minMin = min;
                    minMax = max;
                }
            }
        }

        final DimensionInfo minInfo = dimensionInfos.get(minEndPointDimension);
        List<RangeDimensionRange> superRanges = Arrays.asList(new RangeDimensionRange(minMin+1, minEndpoint, minInfo),
                new RangeDimensionRange(minEndpoint + 1, minMax, minInfo));
        final Map<RangeDimensionRange, Collection<Rule>> newRules = Rule.partitionRuleSpace(originalRules, superRanges, minEndPointDimension);
        final Iterator<Collection<Rule>> iterator = newRules.values().iterator();
        bestPartitionRules1 = aggregate(iterator.next());
        bestPartitionRules2 = aggregate(iterator.next());

        final Map<RangeDimensionRange, Collection<Rule>> newUsageRules = Rule.partitionRuleSpace(usageRules, superRanges, minEndPointDimension);
        final Iterator<Collection<Rule>> iterator2 = newUsageRules.values().iterator();
        usageRules1 = iterator2.next();
        usageRules2 = iterator2.next();
        return this;
    }

    public Collection<Rule> getUsageRules1() {
        return usageRules1;
    }

    public Collection<Rule> getUsageRules2() {
        return usageRules2;
    }
}