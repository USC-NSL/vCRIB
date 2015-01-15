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
 * Time: 2:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class RuleMinCutPartitionerAggregate extends RuleMinCutPartitioner {
    public RuleMinCutPartitionerAggregate(Aggregator aggregator, double balanceWeight, double searchArea) {
        super(aggregator, balanceWeight, searchArea);
    }

    public void partition(List<Rule> rules) throws Exception {
        //find points in each dimension
        bestPartitionRules1 = null;
        bestPartitionRules2 = null;

        double minWeight = Double.MAX_VALUE;

        final List<DimensionInfo> dimensionInfos = Util.getDimensionInfos();
        PreparePoints preparePoints = new PreparePoints(rules);
        for (int dim = 0; dim < dimensionInfos.size(); dim++) {
            DimensionInfo dimensionInfo = dimensionInfos.get(dim);
            preparePoints.invoke(dim);
            if (preparePoints.skip()) continue;
            int start = preparePoints.getStart();
            int end = preparePoints.getEnd();
            List<Long> endpointsList = preparePoints.getEndpointsList();
            long min = preparePoints.getMin();
            long max = preparePoints.getMax();

            //count the number of rules in that cutting on each endpoint may produce.
            for (int i = start; i < end; i++) {
                Long endpoint = endpointsList.get(i);

                List<RangeDimensionRange> superRanges = Arrays.asList(new RangeDimensionRange(min+1, endpoint, dimensionInfo),
                        new RangeDimensionRange(endpoint + 1, max, dimensionInfo));
                final Map<RangeDimensionRange, Collection<Rule>> newRules = Rule.partitionRuleSpace(rules, superRanges, dim);
                final Iterator<Collection<Rule>> iterator = newRules.values().iterator();
                Collection<Rule> rules1 = aggregate(iterator.next());
                Collection<Rule> rules2 = aggregate(iterator.next());
                rules1 = aggregate(rules1);
                rules2 = aggregate(rules2);
                int sum = rules1.size() + rules2.size();
                final double weight = (1 - balanceWeight) * sum + balanceWeight * (Math.abs(rules1.size() - rules2.size()));
                if (minWeight > weight) {
                    minWeight = weight;
                    bestPartitionRules1 = rules1;
                    bestPartitionRules2 = rules2;
                }
            }
        }
    }
}