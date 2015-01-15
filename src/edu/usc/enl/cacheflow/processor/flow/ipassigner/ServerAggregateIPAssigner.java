package edu.usc.enl.cacheflow.processor.flow.ipassigner;

import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/10/12
 * Time: 10:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class ServerAggregateIPAssigner extends IPAssigner {
    public static final int NO_AGGREGATION = 0;
    private int blockPerMachine;

    public ServerAggregateIPAssigner(int blockPerMachine) {
        this.blockPerMachine = blockPerMachine;
    }

    @Override
    public Map<Switch, List<Long>> assignIPs(Random random, Collection<Switch> sources, Collection<Long> ips,
                                             Topology topology, Map<Switch, Integer> vmPerSource) {
        if (!(blockPerMachine == 1 || blockPerMachine == NO_AGGREGATION)) {
            int vmPerSource2 = vmPerSource.values().iterator().next();
            for (Integer integer : vmPerSource.values()) {
                if (integer != vmPerSource2) {
                    throw new RuntimeException("Not supporting different VMs per machines");
                }
            }
            Map<Switch, List<Long>> output2 = new HashMap<Switch, List<Long>>();
            List<Long> ipsSorted = new ArrayList<Long>(ips);
            Collections.sort(ipsSorted);
            final int numberOfBlocks = sources.size() * blockPerMachine;
            List<List<Long>> blocks = blockify(ipsSorted, numberOfBlocks);

            //now for each source pick k blocks
            Collections.shuffle(blocks, random);

            final Iterator<List<Long>> iterator = blocks.iterator();
            for (Switch aSwitch : sources) {
                final List<Long> switchIPs = new ArrayList<Long>(vmPerSource2);
                output2.put(aSwitch, switchIPs);
                for (int i = 0; i < blockPerMachine; i++) {
                    switchIPs.addAll(iterator.next());
                }
            }
            return output2;
        } else {
            Map<Switch, List<Long>> output2 = new HashMap<Switch, List<Long>>();
            if (blockPerMachine == 0) {
                ArrayList<Long> ipsShuffle = new ArrayList<>(ips);
                Collections.shuffle(ipsShuffle, random);
                Iterator<Long> itr = ipsShuffle.iterator();
                selectIPs(sources, vmPerSource,  output2, itr);
            } else {
                ArrayList<Switch> shuffleSources = new ArrayList<>(sources);
                Collections.shuffle(shuffleSources, random);
                List<Long> ipsSorted = new ArrayList<Long>(ips);
                Collections.sort(ipsSorted);
                Iterator<Long> itr = ipsSorted.iterator();
                selectIPs(shuffleSources, vmPerSource,  output2, itr);
            }
            return output2;
        }


    }

    private void selectIPs(Collection<Switch> sources, Map<Switch, Integer> vmPerSource, Map<Switch, List<Long>> output2, Iterator<Long> itr) {
        for (Switch aSwitch : sources) {
            Integer vmsNum = vmPerSource.get(aSwitch);
            final List<Long> switchIPs = new ArrayList<Long>(vmsNum);
            output2.put(aSwitch, switchIPs);
            for (int i = 0; i < vmsNum; i++) {
                switchIPs.add(itr.next());
            }
        }
    }
}
