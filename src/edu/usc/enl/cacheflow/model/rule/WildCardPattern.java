package edu.usc.enl.cacheflow.model.rule;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.util.Util;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/6/12
 * Time: 9:41 AM
 * To change this template use File | Settings | File Templates.
 */
public class WildCardPattern {
    public static String reverseWildcardPattern(long input, List<DimensionInfo> infos) throws UnalignedRangeException {
        StringBuilder sb = new StringBuilder();
        double log2 = Math.log(2);
        for (int infosSize = infos.size(), i = infosSize - 1; i >= 0; i--) {
            DimensionInfo info = infos.get(i);
            int shiftBits = info.getShiftBits();
            long mask = (1 << shiftBits) - 1;
            long wildcardBits = input & mask;
            sb.append(info.toString()).append(":").append(wildcardBits).append(",");
            input >>= shiftBits;
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static void mask(Long[] ranges, long wildcard, long[] buffer) throws UnalignedRangeException {
        List<DimensionInfo> infos = Util.getDimensionInfos();
        for (int i = 0; i < infos.size(); i++) {
            DimensionInfo info = infos.get(i);
            long wcNum = (wildcard & info.getMask()) >> info.getWcShift();
            buffer[i] = (ranges[i] >> wcNum) << wcNum;
        }

    }
}
