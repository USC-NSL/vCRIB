package edu.usc.enl.cacheflow.processor.flow.ipassigner;

import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/10/12
 * Time: 10:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class RackAggregateIPAssigner extends IPAssigner {
    private final int blockPerMachine;

    public RackAggregateIPAssigner(int blockPerMachine) {
        this.blockPerMachine = blockPerMachine;
    }

    @Override
    public Map<Switch, List<Long>> assignIPs(Random random, Collection<Switch> sources, Collection<Long> ips, Topology topology, Map<Switch, Integer> vmPerSource) {
        int vmPerSource2 = vmPerSource.values().iterator().next();
        for (Integer integer : vmPerSource.values()) {
            if (integer !=vmPerSource2){
                throw new RuntimeException("Not supporting different VMs per machines");
            }
        }

        List<Long> ipsSorted = new ArrayList<Long>(ips);
        Collections.sort(ipsSorted);
        Map<Switch, List<Long>> output2 = new HashMap<Switch, List<Long>>();
        //find racks
        Map<Switch, Collection<Switch>> racks = topology.getRacks();
        List<List<Long>> blocks = blockify(ipsSorted, racks.size());
        Collections.shuffle(blocks, random);
        Iterator<List<Long>> ipBlocksIterator = blocks.iterator();
        for (Map.Entry<Switch, Collection<Switch>> torRack : racks.entrySet()) {
            List<Long> block = ipBlocksIterator.next();
            List<List<Long>> blocks2 = blockify(block, torRack.getValue().size() * blockPerMachine);
            Collections.shuffle(blocks2, random);

            final Iterator<List<Long>> iterator = blocks2.iterator();
            for (Switch switchInRack : torRack.getValue()) {
                final List<Long> switchIPs = new ArrayList<Long>(vmPerSource2);
                output2.put(switchInRack, switchIPs);
                for (int i = 0; i < blockPerMachine; i++) {
                    switchIPs.addAll(iterator.next());
                }
            }
        }
        return output2;
    }
}
