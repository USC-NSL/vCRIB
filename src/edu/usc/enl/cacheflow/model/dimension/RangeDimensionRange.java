package edu.usc.enl.cacheflow.model.dimension;

import java.io.PrintWriter;
import java.util.*;

import static java.lang.Math.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 9/22/11
 * Time: 6:42 AM
 */
public class RangeDimensionRange implements Comparable<RangeDimensionRange> {
    public final static RangeDimensionRangeInfoComparator RANGE_COMPARATOR = new RangeDimensionRangeInfoComparator();
    private long start;
    private long end;
    protected DimensionInfo info;
    public final static Long ZERO_LONG = 0l;
    private int hashCodeCache = 0;


    public RangeDimensionRange(long start, long end, DimensionInfo info) {
        this.start = start;
        this.end = end;
        this.info = info;
    }

    @Override
    public int hashCode() {
        if (hashCodeCache == 0) {
            int result = (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (end ^ (end >>> 32));
            hashCodeCache = result;
        }
        return hashCodeCache;
    }

    public DimensionInfo getInfo() {
        return info;
    }

    public boolean match(long dimension) {
        return start <= dimension && end >= dimension;
    }

    public void setStart(long start) {
        this.start = start;
        hashCodeCache = 0;
    }

    public void setEnd(long end) {
        this.end = end;
        hashCodeCache = 0;
    }

    public RangeDimensionRange canAggregate(RangeDimensionRange otherRuleProperty) {
        if (hasIntersect(otherRuleProperty)) {
            return new RangeDimensionRange(min(start, otherRuleProperty.start), max(end, otherRuleProperty.end), info);
        }
/*
        if (match(otherRuleProperty.start) && match(otherRuleProperty.end)) { //covers it completely
            return this;
        }
        if (match(otherRuleProperty.start)) {
            return new RangeDimensionRange(start, otherRuleProperty.end, info);
        }
        if (match(otherRuleProperty.end)) {
            return new RangeDimensionRange(otherRuleProperty.start, end, info);
        }
*/
        if (isAdjacent(start, otherRuleProperty.end) || isAdjacent(end, otherRuleProperty.start)) {// need to do this to handle two points!
            return new RangeDimensionRange(min(start, otherRuleProperty.start), max(end, otherRuleProperty.end), info);
        }
/*
        if (otherRuleProperty.match(start) && otherRuleProperty.match(end)) { //The other covers me completely
            return otherRuleProperty;
        }
*/
        return null;
    }

    public void extend(long value) {
        final long start1 = getStart();
        final long end1 = getEnd();
        if (value >= start1 && value <= end1) {
            return;
        }
        if (value > start1 && value > end1) {
            setEnd(value);
            return;
        }
        if (value < start1 && value < end1) {
            setStart(value);
        }
    }

    public static boolean isAdjacent(long l1, long l2) {
        return (l1 == l2 - 1) || (l1 == l2 + 1);
    }

    public List<RangeDimensionRange> minus(RangeDimensionRange range) {
        RangeDimensionRange intersect = intersect(range);
        if (intersect == null) {
            return Arrays.asList(this);
        }
        if (start == intersect.end && end == intersect.end) {
            return new ArrayList<RangeDimensionRange>();
        }
        if (start == intersect.start) {
            return Arrays.asList(new RangeDimensionRange(intersect.end + 1, end, info));
        }
        if (end == intersect.getEnd()) {
            return Arrays.asList(new RangeDimensionRange(start, intersect.end - 1, info));
        }
        return Arrays.asList(new RangeDimensionRange(start, intersect.start - 1, info),
                new RangeDimensionRange(intersect.end + 1, getEnd(), info));
    }

    public static List<RangeDimensionRange> grid(List<RangeDimensionRange> ranges) {
        DimensionInfo info = ranges.get(0).getInfo();
        List<Long> lows1 = new ArrayList<Long>(ranges.size());
        List<Long> highs1 = new ArrayList<Long>(ranges.size());
        for (RangeDimensionRange range : ranges) {
            lows1.add(range.getStart());
            highs1.add(range.getEnd());
        }
        //remove duplicate
        lows1 = new ArrayList<Long>(new HashSet<Long>(lows1));
        Collections.sort(lows1);

        highs1 = new ArrayList<Long>(new HashSet<Long>(highs1));
        Collections.sort(highs1);

        LinkedList<Long> lows = new LinkedList<Long>(lows1);
        LinkedList<Long> highs = new LinkedList<Long>(highs1);

        List<RangeDimensionRange> outputRanges = new LinkedList<RangeDimensionRange>();

        Long l = lows.pop();
        while (lows.size() > 0 || highs.size() > 0) {
            if (lows.size() > 0 && lows.getFirst() <= highs.getFirst()) {
                outputRanges.add(new RangeDimensionRange(l, lows.getFirst() - 1, info));
                l = lows.pop();
            } else {
                outputRanges.add(new RangeDimensionRange(l, highs.getFirst(), info));
                l = highs.pop() + 1;
                if (lows.size() > 0 && lows.getFirst().equals(l)) {
                    lows.removeFirst();
                }
            }
        }
        return outputRanges;
    }

    public RangeDimensionRange intersect(RangeDimensionRange other) {
//        return match(other.getStart()) || match(other.getEnd()) ||
//                other.match(getStart()) || other.match(getEnd());
        long newStart = max(getStart(), other.getStart());
        long newEnd = min(getEnd(), other.getEnd());
        if (newStart <= newEnd) {
            return new RangeDimensionRange(newStart, newEnd, getInfo());
        }
        return null;
    }

    public static boolean covers(List<RangeDimensionRange> ranges1, List<RangeDimensionRange> ranges2) {
        for (int i = 0; i < ranges1.size(); i++) {
            final RangeDimensionRange range1 = ranges1.get(i);
            final RangeDimensionRange range2 = ranges2.get(i);
            if (!(range1.start <= range2.start && range1.end >= range2.end)) {
                return false;
            }
        }
        return true;
    }

    public boolean covers(RangeDimensionRange other) {
        return start <= other.start && end >= other.end;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RangeDimensionRange that = (RangeDimensionRange) o;

        if (end != that.end) return false;
        if (start != that.start) return false;

        return true;
    }

    public String toString() {
        return info.toString(this);
    }

    //supposes always the range length is a multiple of numberOfPartitions
    public List<RangeDimensionRange> partition(int numberOfPartitions) {
        List<RangeDimensionRange> output = new ArrayList<RangeDimensionRange>(numberOfPartitions);
        long startValue = start;
        long endValue = end;
        long partitionLength = (endValue - startValue + 1) / numberOfPartitions;
        long lastStartValue = startValue;
        for (int i = 0; i < numberOfPartitions; i++) {
            long lastEnd = lastStartValue + partitionLength - 1;
            output.add(new RangeDimensionRange(lastStartValue, lastEnd, info));
            lastStartValue = lastEnd + 1;
        }
        return output;
    }


    public long getRandomNumber(Random random) {
        return start + (long) (random.nextDouble() * (end + 1 - start));
    }

    public long getSize() {
        return end - start + 1;
    }

    public boolean hasIntersect(RangeDimensionRange other) {
        long s1 = start;
        long s2 = other.start;
        long e1 = end;
        long e2 = other.end;
        long maxs = s1 < s2 ? s2 : s1;
        long mine = e1 < e2 ? e1 : e2;
        return maxs <= mine;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public int compareTo(RangeDimensionRange o) {
        return -ZERO_LONG.compareTo(start - o.start);
    }


    public void setInfo(DimensionInfo info) {
        this.info = info;
    }

    public int getNumberOfWildcardBits() throws UnalignedRangeException {
        long x = (end + 1) - start;
        final int log = binlog(x);
        if ( (1l << log) == x && start % x == 0) {
            return log;
        }
        throw new UnalignedRangeException(this);
        /*double output = log(x) / log(2);
        if (((int) output) != output) {
            //double check
            final long round = round(output);
            if (pow(2, round) != x) {
                throw new UnalignedRangeException(this);
            }else{
                return (int) round;
            }
        }
        return (int) output;*/

    }

    public static List<RangeDimensionRange> breakRange(RangeDimensionRange range) {
        List<RangeDimensionRange> output = new LinkedList<RangeDimensionRange>();
        long start = range.getStart();
        long end = range.getEnd();
        int bitToZero = binlog(end ^ (start));
        long nearest2 = (end) >>> bitToZero;
        nearest2 <<= bitToZero;
        if (nearest2 == 0) {
            nearest2 = 1 << bitToZero;//middle power of 2
        }
        start = start - 1;
        end = end + 1;
        //int nearest2 = (int) (pow(2, binlog(start) + 1));
        //come down from nearest
        long current = nearest2 - 1;
        while (current > start) {
            int step = (int) (1l << binlog(current - start));
            output.add(new RangeDimensionRange(current - step + 1, current, range.getInfo()));
            current -= step;
        }

        //go up to the end
        current = nearest2;
        while (current < end) {
            int step = (int) (1l << binlog(end - current));
            output.add(new RangeDimensionRange(current, current + step - 1, range.getInfo()));
            current += step;
        }
        return output;
    }

    public static int binlog(long bits) // returns 0 for bits=0
    {
        int log = 0;
        if ((bits & 0xffffffff00000000l) != 0) {
            bits >>>= 32;
            log = 32;
        }
        if ((bits & 0xffff0000) != 0) {
            bits >>>= 16;
            log += 16;
        }
        if (bits >= 256) {
            bits >>>= 8;
            log += 8;
        }
        if (bits >= 16) {
            bits >>>= 4;
            log += 4;
        }
        if (bits >= 4) {
            bits >>>= 2;
            log += 2;
        }
        if (bits >= 2) {
            bits >>>= 1;
            log += 1;
        }
        return log;
    }

    private static class RangeDimensionRangeInfoComparator implements Comparator<RangeDimensionRange> {

        public int compare(RangeDimensionRange o1, RangeDimensionRange o2) {
            return o1.getInfo().compareTo(o2.getInfo());
        }
    }

    public static long computeWildcardPattern(List<RangeDimensionRange> properties) throws UnalignedRangeException {
        long output = 0;
        for (RangeDimensionRange property : properties) {
            /*int maxBitPattern = property.getInfo().getMaxBitPattern();
            int shiftBits = binlog(maxBitPattern);
            if (((1l) << shiftBits) <= maxBitPattern) {
                shiftBits++;
            }
            //(int) ceil(log(property.getInfo().getMaxBitPattern() + 1) / log(2));
            if (i > 0) {
                output = output << shiftBits;
            }*/
            final int wBits = property.getNumberOfWildcardBits();
            output += (wBits << property.getInfo().getWcShift());
        }
        return output;
    }

    public static void toString(PrintWriter writer, List<RangeDimensionRange> ranges) {
        boolean first = true;
        for (RangeDimensionRange range : ranges) {
            if (!first) {
                writer.print(",");
            }
            first = false;
            writer.print(range);
        }
    }

}
