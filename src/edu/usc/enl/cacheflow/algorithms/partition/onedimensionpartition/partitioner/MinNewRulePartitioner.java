package edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.partitioner;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.CombinationGenerator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/6/11
 * Time: 12:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class MinNewRulePartitioner extends Partitioner{
    public List<RangeDimensionRange> partition(List<RangeDimensionRange> ranges, int num, Collection<Rule> rules, Aggregator aggregator) {
        //check num<=ranges.size()

        double bestCutValue = Integer.MAX_VALUE;
        List<RangeDimensionRange> bestCut = null;

        //generate combination, we have ranges.size() -1 points to cut, and want num-1 of these points.
        CombinationGenerator x = new CombinationGenerator(ranges.size() - 1, num - 1);
        while (x.hasMore()) {

            //create ranges from combination
            List<RangeDimensionRange> thisCombinationRanges = new ArrayList<RangeDimensionRange>(num);
            int[] indices = x.getNext();
            //first start is the start of the first range
            RangeDimensionRange firstDimensionRange = ranges.get(0);
            long currentStart = firstDimensionRange.getStart();
            DimensionInfo info = firstDimensionRange.getInfo();
            for (int index : indices) {
                //create ranges based on indices
                thisCombinationRanges.add(new RangeDimensionRange(currentStart, ranges.get(index).getEnd(),info));
                currentStart = ranges.get(index + 1).getStart();
            }
            //from the last point to the end of last range
            thisCombinationRanges.add(new RangeDimensionRange(currentStart, ranges.get(ranges.size() - 1).getEnd(),info));

            //calculate the metric for this combination

            //metric= sum(rules in each dimension)-old number of rules )/num
            int sum = 0;
            Map<RangeDimensionRange, Integer> rangeNumMap = calculateRequiredRules(thisCombinationRanges, rules,aggregator);
            for (Integer numOfRules : rangeNumMap.values()) {
                sum += numOfRules;
            }
            double metric = 1.0 * (sum - rules.size()) / num;
            if (metric < bestCutValue) {
                bestCut = thisCombinationRanges;
                bestCutValue = metric;
            }

        }
        return bestCut;
    }

    @Override
    public String toString() {
        return "Minimum New Rule";
    }
}
