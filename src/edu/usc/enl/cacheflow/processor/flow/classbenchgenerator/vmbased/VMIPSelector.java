package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/23/12
 * Time: 12:21 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class VMIPSelector {
    public abstract Collection<Long> getIps(Collection<Rule> rules, Random random, int numberOfIPs);

    protected List<Long> findSortedSrcEdges(Collection<Rule> rules) {
        final Set<Long> srcIPs = new HashSet<Long>(rules.size() * 2 + 1, 1);
        int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        for (Rule rule : rules) {
            RangeDimensionRange property = rule.getProperty(srcIPIndex);
            srcIPs.add(property.getStart());
            srcIPs.add(property.getEnd());
        }
        List<Long> ipsSorted = new ArrayList<Long>(srcIPs);
        for (Long srcIP : srcIPs) {
            ipsSorted.add(srcIP);
        }
        Collections.sort(ipsSorted);
        return ipsSorted;
    }

    public abstract  String toString();

}
