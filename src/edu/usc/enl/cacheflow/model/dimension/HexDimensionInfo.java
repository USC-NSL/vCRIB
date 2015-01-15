package edu.usc.enl.cacheflow.model.dimension;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/27/11
 * Time: 8:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class HexDimensionInfo extends DimensionInfo {

    public HexDimensionInfo(String name, long min, long max) {
        super(name, min, max);
    }

    public HexDimensionInfo(String name, DimensionInfo info) {
        super(name, info);
    }

    public RangeDimensionRange parseRange(String s) {
        final String[] split = s.split("/");
        final long s1 = Long.parseLong(split[0].substring(2), 16);
        final long mask = Long.parseLong(split[1].substring(2), 16);

        return new RangeDimensionRange(s1 & mask,  s1 | (~mask &getMax()), this);
    }
}
