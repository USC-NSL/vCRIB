package edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.partitioner;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PartitionTree2;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/6/11
 * Time: 12:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class TrafficBalancedPartitioner extends Partitioner {
    private Map<Rule, Long> ruleTrafficMap;

    public TrafficBalancedPartitioner(Map<Rule, Long> ruleTrafficMap) {
        this.ruleTrafficMap = ruleTrafficMap;
    }

    public List<RangeDimensionRange> partition(List<RangeDimensionRange> ranges, int num, Collection<Rule> rules, Aggregator aggregator) {
        List<RangeDimensionRange> output = new ArrayList<RangeDimensionRange>(num);
        // calculate rules per range
        double sum = 0;
        Map<RangeDimensionRange, Long> rangeTraffic = calculateTraffic(ranges, rules);
        for (Long traffic : rangeTraffic.values()) {
            sum += traffic;
        }
        long trafficPerPartition = (int) (Math.ceil(sum / num));
        long currentRangeTraffic = 0;
        long currentStart = -1;
        long currentEnd = -1;
        DimensionInfo info = null;
        for (RangeDimensionRange dimensionRange : rangeTraffic.keySet()) {
            info = dimensionRange.getInfo();
            if (currentStart == -1) {
                // this range is empty so add at least one
                currentStart = dimensionRange.getStart();
                currentEnd = dimensionRange.getEnd();
                currentRangeTraffic += rangeTraffic.get(dimensionRange);
            } else if (currentRangeTraffic + rangeTraffic.get(dimensionRange) > trafficPerPartition) {
                //commit the current dimension
                output.add(new RangeDimensionRange(currentStart, currentEnd, info));

                //reset current statistics
                currentStart = dimensionRange.getStart();
                currentEnd = dimensionRange.getEnd();
                currentRangeTraffic = rangeTraffic.get(dimensionRange);

            } else {
                currentRangeTraffic += rangeTraffic.get(dimensionRange);
                currentEnd = dimensionRange.getEnd();
            }
        }
        //commit last range
        if (rangeTraffic.size() > 0) {
            output.add(new RangeDimensionRange(currentStart, currentEnd, info));
        }
        return output;
    }

    private Map<RangeDimensionRange, Long> calculateTraffic(List<RangeDimensionRange> ranges, Collection<Rule> rules) {
        Map<RangeDimensionRange, Long> output = new HashMap<RangeDimensionRange, Long>();
        final int infoIndex = Util.getDimensionInfoIndex(ranges.get(0).getInfo());
        for (RangeDimensionRange range : ranges) {
            Collection<Rule> matchedRules = PartitionTree2.getMatchedRules(rules, infoIndex, range);
            long sum = 0;
            for (Rule matchedRule : matchedRules) {
                sum += ruleTrafficMap.get(matchedRule);
            }
            output.put(range, sum);
        }
        return output;
    }

    @Override
    public String toString() {
        return "Balanced Traffic";
    }
}