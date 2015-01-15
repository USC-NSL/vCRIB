package edu.usc.enl.cacheflow.processor.flow.ipassigner;

import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/10/12
 * Time: 9:23 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class IPAssigner {
    public static final int MACHINE_LEVEL_AGGREGATE = -1;
    public static final int RACK_LEVEL_AGGREGATE = -2;
    public static final int DATACENTER_LEVEL_AGGREGATE = -3;

    public abstract Map<Switch, List<Long>> assignIPs(Random random, Collection<Switch> sources, final Collection<Long> ips,
                                                      Topology topology, Map<Switch, Integer> vmPerSource);

    protected List<List<Long>> blockify(List<Long> ipsSorted, int numberOfBlocks) {
        final int IPperBlock = (int) Math.floor(1.0 * ipsSorted.size() / numberOfBlocks);
        List<List<Long>> blocks = new LinkedList<List<Long>>();
        for (int i1 = 0; i1 < numberOfBlocks; i1++) {
            //find IPs that are in the block
            List<Long> ipsInBlock = ipsSorted.subList(IPperBlock * i1, Math.min(ipsSorted.size(), IPperBlock * (i1 + 1)));
            blocks.add(ipsInBlock);
        }
        return blocks;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public Map<Switch, List<Long>> convert(Random random, Map<Switch, List<Long>> input, Topology topology) {
        //get all IPs
        List<Long> ips = new ArrayList<>();
        Map<Switch, Integer> vmPerSource = new HashMap<>();
        for (Map.Entry<Switch, List<Long>> entry : input.entrySet()) {
            List<Long> longs = entry.getValue();
            ips.addAll(longs);
            vmPerSource.put(entry.getKey(), longs.size());
        }
        return assignIPs(random, input.keySet(), ips, topology, vmPerSource);
    }
}
