package edu.usc.enl.cacheflow.model.dimension;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/20/11
 * Time: 2:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class IPDimensionInfo extends DimensionInfo {
    private static final long ALL_ONE = 0xFFFFFFFF;

    public IPDimensionInfo(String name, long min, long max) {
        super(name, min, max);
    }

    public IPDimensionInfo(String name, DimensionInfo info) {
        super(name, info);
    }

    public RangeDimensionRange parseRange(String s) {
        String[] split = s.split("/");
        int prefix = Integer.parseInt(split[1]);
        split = split[0].split("\\.");
        long start = 0;
        for (String ipPart : split) {
            start = start * 256 + Integer.parseInt(ipPart);
        }

        long prefixMask = ALL_ONE << (32 - prefix);
        return new RangeDimensionRange(start & prefixMask,start | (~prefixMask& ALL_ONE), this);
    }

}
