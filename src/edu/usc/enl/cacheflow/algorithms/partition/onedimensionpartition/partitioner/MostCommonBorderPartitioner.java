package edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.partitioner;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/24/11
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class MostCommonBorderPartitioner extends Partitioner {
    @Override
    public List<RangeDimensionRange> partition(List<RangeDimensionRange> ranges, int num, Collection<Rule> rules, Aggregator aggregator) {

        Iterator<RangeDimensionRange> iterator = ranges.iterator();
        RangeDimensionRange range = iterator.next();//skip first. check start of each range   WHY?
        Map<Long, BorderOccuranceTuple> occuranciesBorderTubuleMap = new HashMap<Long, BorderOccuranceTuple>(ranges.size());
        final DimensionInfo info = range.getInfo();
        final int infoIndex = Util.getDimensionInfoIndex(info);
        //create borders and occurrences
        for (; iterator.hasNext(); ) {
            range = iterator.next();
            long occurrance = 0;
            final long border = range.getStart();
            for (Rule rule : rules) {
                if (rule.getProperty(infoIndex).getStart() == border) {
                    occurrance++;
                }
                if (rule.getProperty(infoIndex).getEnd() == border - 1) {
                    occurrance++;
                }
            }
            BorderOccuranceTuple borderOccuranceTuple = occuranciesBorderTubuleMap.get(occurrance);
            if (borderOccuranceTuple == null) {
                borderOccuranceTuple = new BorderOccuranceTuple(occurrance);
                occuranciesBorderTubuleMap.put(occurrance, borderOccuranceTuple);
            }
            borderOccuranceTuple.addBorder(border);
        }
        List<BorderOccuranceTuple> borderList = new ArrayList<BorderOccuranceTuple>(occuranciesBorderTubuleMap.values());

        //sort based on occurrence (reversed);
        Collections.sort(borderList);

        //we need num-1 borders
        List<Long> borders = new ArrayList<Long>(num - 1);

        for (BorderOccuranceTuple borderOccuranceTuple : borderList) {
            borders.addAll(borderOccuranceTuple.getBorders(num - 1 - borders.size()));
        }

        //sort borders based on their values
        Collections.sort(borders);

        long currentStart = ranges.get(0).getStart();
        final Iterator<Long> borderIterator = borders.iterator();
        List<RangeDimensionRange> outputRanges = new ArrayList<RangeDimensionRange>(num);
        long border = borderIterator.next();
        for (RangeDimensionRange dimensionRange : ranges) {
            final long start = dimensionRange.getStart();
            if (start >= border) {
                //commit range
                outputRanges.add(new RangeDimensionRange(currentStart, start - 1, info));
                currentStart = start;
                if (!borderIterator.hasNext()) {
                    //last range
                    outputRanges.add(new RangeDimensionRange(currentStart, ranges.get(ranges.size() - 1).getEnd(), info));
                    break;
                }
                border = borderIterator.next();
            }
        }

        return outputRanges;
    }

    @Override
    public String toString() {
        return "Most Common Border";
    }

    private static final class BorderOccuranceTuple implements Comparable<BorderOccuranceTuple> {
        List<Long> borders;
        long occurance;

        private BorderOccuranceTuple(long occurance) {
            this.occurance = occurance;
            borders = new LinkedList<Long>();
        }

        public void addBorder(long border) {
            borders.add(border);
        }

        public int compareTo(BorderOccuranceTuple o) {
            return -(int) (occurance - o.occurance);
        }

        public List<Long> getBorders(int num) {
            if (num >= borders.size()) {
                return borders;
            }
            int step = borders.size() / (num+1); //num is <= size
            List<Long> outputList = new ArrayList<Long>(num);
            for (int i = 0; i < num; i ++) {
                outputList.add(borders.get((i+1)*step));
            }
            return outputList;
        }

        @Override
        public String toString() {
            return "BorderOccuranceTuple{" +
                    "borders=" + borders +
                    ", occurance=" + occurance +
                    '}';
        }
    }


}
