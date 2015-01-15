package edu.usc.enl.cacheflow.model.dimension;

import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/6/11
 * Time: 12:34 AM
 * To change this template use File | Settings | File Templates.
 */
public class DimensionInfo implements Comparable<DimensionInfo> {
    private String name;
    private final RangeDimensionRange range;
    private int wcCache = -1;
    private int wcShift;
    public long mask;

    public DimensionInfo(String name, long min, long max) {
        this.name = name;
        range = new RangeDimensionRange(min, max, this);
    }

    protected DimensionInfo(String name, DimensionInfo info) {
        this.name = name;
        long start = info.getDimensionRange().getStart();
        long end = info.getDimensionRange().getEnd();
        this.range = new RangeDimensionRange(start, end, this);
    }

    public String getName() {
        return name;
    }

    public long getMin() {
        return range.getStart();
    }

    public long getMax() {
        return range.getEnd();
    }

    public RangeDimensionRange getDimensionRange() {
        return range;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        DimensionInfo that = (DimensionInfo) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    public int compareTo(DimensionInfo o) {
        return name.compareTo(o.name);
    }

    public String toString(RangeDimensionRange range) {
        return range.getStart() + ":" + range.getEnd();
    }

    public static List<RangeDimensionRange> getTotalRanges(List<DimensionInfo> ranges) {
        List<RangeDimensionRange> totalRanges = new LinkedList<RangeDimensionRange>();
        for (DimensionInfo range : ranges) {
            totalRanges.add(range.getDimensionRange());
        }
        return totalRanges;
    }

    public RangeDimensionRange parseRange(String s) {
        String[] startEnd = s.split("\\s*:\\s*");
        return new RangeDimensionRange(Long.parseLong(startEnd[0]), Long.parseLong(startEnd[1]), this);
    }

    private int getMaxBitPattern() throws UnalignedRangeException {
        if (wcCache < 0) {
            wcCache = range.getNumberOfWildcardBits();
        }
        return wcCache;
    }

    public int getWcShift() {
        return wcShift;
    }

    public void setWcShift(int wcShift) throws UnalignedRangeException {
        this.wcShift = wcShift;
        long shiftBits = getShiftBits();
        mask = ((1 << shiftBits) - 1) << wcShift;
    }

    public long getMask() {
        return mask;
    }

    public  int getShiftBits() throws UnalignedRangeException {
        int maxBitPattern = getMaxBitPattern();
        int shiftBits = RangeDimensionRange.binlog(maxBitPattern);
        if (((1l) << shiftBits) <= maxBitPattern) {
            shiftBits++;
        }
        return shiftBits;
    }

    public static void dimensionInfosToString(List<DimensionInfo> dimensionInfos, PrintWriter pw) {
        boolean first = true;
        for (DimensionInfo dimensionInfo : dimensionInfos) {
            if (!first) {
                pw.print(",");
            }
            first = false;
            pw.print(dimensionInfo.toString());
        }
        pw.println();
        first = true;
        for (DimensionInfo dimensionInfo : dimensionInfos) {
            if (!first) {
                pw.print(",");
            }
            first = false;
            pw.print(dimensionInfo.getDimensionRange().toString());
        }
        pw.println();
    }
}
