package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/23/12
 * Time: 12:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class UniformRangeVMIPSelector extends VMIPSelector {
    private RangeDimensionRange allowedRange = null;

    public UniformRangeVMIPSelector(RangeDimensionRange allowedRange) {
        this.allowedRange = allowedRange;
    }

    public UniformRangeVMIPSelector() {
        this.allowedRange= Util.SRC_IP_INFO.getDimensionRange();
    }

    @Override
    public Collection<Long> getIps(Collection<Rule> rules, Random random, int numberOfIPs) {
        List<Long> ipEdgesSorted = findSortedSrcEdges(rules);

        ///select VMs addresses;
        Set<Long> vmsAddress = new HashSet<Long>(numberOfIPs, 1);
        while (vmsAddress.size() < numberOfIPs) {
            int regionIndex = random.nextInt(ipEdgesSorted.size() - 1);
            Long start = ipEdgesSorted.get(regionIndex);
            long ip = start + (long) (random.nextDouble() * (ipEdgesSorted.get(regionIndex + 1) - start));
            if (allowedRange == null || allowedRange.match(ip)) {
                vmsAddress.add(ip);
            }
        }
        return vmsAddress;
    }

    @Override
    public String toString() {
        return "Uniform Random Range";
    }
}
