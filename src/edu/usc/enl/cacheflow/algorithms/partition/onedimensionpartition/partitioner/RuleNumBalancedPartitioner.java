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
 * Time: 12:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class RuleNumBalancedPartitioner extends Partitioner {

    public List<RangeDimensionRange> partition(List<RangeDimensionRange> ranges, int num, Collection<Rule> rules, Aggregator aggregator) {
        List<RangeDimensionRange> output = new ArrayList<RangeDimensionRange>(num);
        // calculate rules per range
        double sum = 0;
        SortedMap<RangeDimensionRange, Integer> rangeRuleNum = calculateMatchedRules(ranges, rules);
        //SortedMap<RangeDimensionRange, Integer> rangeRuleNum = calculateRequiredRules(ranges, rules, aggregator);
        for (Integer numberOfRules : rangeRuleNum.values()) {
            sum += numberOfRules;
        }
        int rulePerPartition = (int) (Math.ceil(sum / num));
        int currentRangeRules = 0;
        long currentStart = -1;
        long currentEnd = -1;
        DimensionInfo info = null;
        int usedRanges = 0;
        info = ranges.get(0).getInfo();
        for (Iterator<RangeDimensionRange> iterator = rangeRuleNum.keySet().iterator(); iterator.hasNext(); ) {
            RangeDimensionRange dimensionRange = iterator.next();
            if (rangeRuleNum.size() - usedRanges <= num - output.size()) {
                //there is not any choice to select from now just commit until now and create one output range for each input range

                currentEnd = dimensionRange.getEnd();

                //commit the current dimension
                output.add(new RangeDimensionRange(currentStart, currentEnd, info));
                for (; iterator.hasNext(); ) {
                    final RangeDimensionRange nextDimensionRange = iterator.next();
                    output.add(nextDimensionRange);
                }

                return output;
            }
            if (currentStart == -1) {
                // this range is empty so add at least one
                currentStart = dimensionRange.getStart();
                currentEnd = dimensionRange.getEnd();
                currentRangeRules += rangeRuleNum.get(dimensionRange);
            } else if (currentRangeRules + rangeRuleNum.get(dimensionRange) > rulePerPartition) {

                currentEnd = dimensionRange.getEnd();

                //commit the current dimension
                output.add(new RangeDimensionRange(currentStart, currentEnd, info));

                //reset current statistics
                currentStart = -1;
                currentEnd = -1;
                currentRangeRules = 0;

            } else {
                currentRangeRules += rangeRuleNum.get(dimensionRange);
                currentEnd = dimensionRange.getEnd();
            }
            usedRanges++;
        }
        //commit last range
        if (rangeRuleNum.size() > 0) {
            output.add(new RangeDimensionRange(currentStart, currentEnd, info));
        }
        return output;

    }

    private SortedMap<RangeDimensionRange, Integer> calculateMatchedRules(List<RangeDimensionRange> ranges, Collection<Rule> rules) {
        SortedMap<RangeDimensionRange, Integer> rangeRuleNum = new TreeMap<RangeDimensionRange, Integer>();
        final int infoIndex = Util.getDimensionInfoIndex(ranges.get(0).getInfo());
        for (RangeDimensionRange range : ranges) {
            rangeRuleNum.put(range, PartitionTree2.getMatchedRules(rules, infoIndex, range).size());
        }
        return rangeRuleNum;
    }

    @Override
    public String toString() {
        return "Balanced Number of Rules";
    }
}