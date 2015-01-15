package edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.partitioner;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/6/11
 * Time: 12:08 PM
 */
public class RangeNumBalancedPartitioner extends Partitioner {
    public List<RangeDimensionRange> partition(List<RangeDimensionRange> ranges, int num, Collection<Rule> rules, Aggregator aggregator) {
        int rangePerOutputRange = (int) (Math.ceil(1.0 * ranges.size() / num));
        List<RangeDimensionRange> outputRanges = new ArrayList<RangeDimensionRange>(num);
        for (int i = 0; i < ranges.size(); i += rangePerOutputRange) {
            RangeDimensionRange dimensionRange = ranges.get(i);
            outputRanges.add(new RangeDimensionRange(
                    dimensionRange.getStart(), ranges.get(Math.min(i + rangePerOutputRange, ranges.size()) - 1).getEnd(), dimensionRange.getInfo()));
        }
        return outputRanges;
    }

    @Override
    public String toString() {
        return "Balanced number of Ranges";
    }
}