package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.rulebased;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.DestinationSelector;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.IPAssigner;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/24/12
 * Time: 10:03 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class RuleBasedFlowGenerator {
    protected int srcPortIndex;
    protected int dstPortIndex;
    protected int protocolIndex;
    protected int srcIPIndex;
    protected int dstIPIndex;
    protected final IPAssigner ipAssigner;

    public RuleBasedFlowGenerator(IPAssigner ipAssigner) {
        this.ipAssigner = ipAssigner;
    }

    public abstract void generate(List<Rule> rules, Random random, List<Switch> sources,
                                  DestinationSelector destinationSelector, CustomRandomFlowDistribution flowDistribution,
                                  PrintWriter writer, Topology topology);

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    protected void fillIndexes() {

        srcPortIndex = Util.getDimensionInfoIndex(Util.SRC_PORT_INFO);
        dstPortIndex = Util.getDimensionInfoIndex(Util.DST_PORT_INFO);
        protocolIndex = Util.getDimensionInfoIndex(Util.PROTOCOL_INFO);
        srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        dstIPIndex = Util.getDimensionInfoIndex(Util.DST_IP_INFO);
    }

    protected List<Flow> generateFlows(Switch src, Switch dst, Rule rule, long srcIP, long dstIP, Random random, int choices) {

        List<Flow> output = new ArrayList<Flow>(choices * choices * choices);
        for (int i = 0; i < choices; i++) {
            final long srcPort = rule.getProperty(srcPortIndex).getRandomNumber(random);
            for (int j = 0; j < choices; j++) {
                final long dstPort = rule.getProperty(dstPortIndex).getRandomNumber(random);
                for (int k = 0; k < choices; k++) {
                    final long protocol = rule.getProperty(protocolIndex).getRandomNumber(random);
                    Long[] properties = new Long[5];
                    properties[srcIPIndex] = srcIP;
                    properties[dstIPIndex] = dstIP;
                    properties[srcPortIndex] = srcPort;
                    properties[dstPortIndex] = dstPort;
                    properties[protocolIndex] = protocol;
                    output.add(new Flow(0, src, dst, properties));
                }
            }
        }
        return output;
    }

    protected void update(Long key, Map<Long, Set<Long>> map, Long value) {
        Set<Long> v = map.get(key);
        if (v == null) {
            v = new HashSet<Long>();
            map.put(key, v);
        }
        v.add(value);
    }

    protected void updateMin(Long key, Map<Long, Integer> map, int value) {
        final Integer v = map.get(key);
        if (v == null) {
            map.put(key, value);
        } else {
            map.put(key, Math.min(v, value));
        }
    }

    protected void update(Long key, Map<Long, Integer> map, int value) {
        final Integer v = map.get(key);
        if (v == null) {
            map.put(key, value);
        } else {
            map.put(key, v + value);
        }
    }

    protected void writeToWriter(Random random, PrintWriter writer, CustomRandomFlowDistribution flowDistribution, Set<Flow> microFlows) {
        for (Flow flow : microFlows) {
            flow.setTraffic(flowDistribution.getRandomFlowSize(random.nextDouble()));
            flow.toString(writer);
        }
        microFlows.clear();
    }

    /*   protected Map<Switch, List<Long>> assignIPs(Random random, List<Switch> sources,
                                                final Set<Long> ips, Topology topology) {
        List<Long> ipsSorted = new ArrayList<Long>(ips);
        Collections.sort(ipsSorted);
        Map<Switch, List<Long>> output2 = new HashMap<Switch, List<Long>>();

        if (aggLevel == OnlyTenantAccessControlRuleFlowGenerator.RACK_LEVEL_AGGREGATE) {
            //find racks
            Map<Switch, Collection<Switch>> racks = topology.getRacks();
            List<List<Long>> blocks = blockify(ipsSorted, racks.size());
            Collections.shuffle(blocks, random);
            Iterator<List<Long>> ipBlocksIterator = blocks.iterator();
            for (Map.Entry<Switch, Collection<Switch>> torRack : racks.entrySet()) {
                List<Long> block = ipBlocksIterator.next();
                List<List<Long>> blocks2 = blockify(block, torRack.getValue().size());
                Collections.shuffle(blocks2, random);

                final Iterator<List<Long>> iterator = blocks2.iterator();
                for (Switch switchInRack : torRack.getValue()) {
                    List<Long> next = iterator.next();
                    output2.put(switchInRack, next);
                }
            }

        } else {
            int blockPerMachine = aggLevel;
            if (aggLevel == OnlyTenantAccessControlRuleFlowGenerator.MACHINE_LEVEL_AGGREGATE) {
                blockPerMachine = 1;
            } else if (aggLevel == OnlyTenantAccessControlRuleFlowGenerator.DATACENTER_LEVEL_AGGREGATE) {
                blockPerMachine = vmPerSource;
            }

            final int numberOfBlocks = sources.size() * blockPerMachine;
            List<List<Long>> blocks = blockify(ipsSorted, numberOfBlocks);

            //now for each source pick k blocks
            Collections.shuffle(blocks, random);


            final Iterator<List<Long>> iterator = blocks.iterator();
            for (Switch aSwitch : sources) {
                final List<Long> switchIPs = new ArrayList<Long>(vmPerSource);
                output2.put(aSwitch, switchIPs);
                for (int i = 0; i < blockPerMachine; i++) {
                    switchIPs.addAll(iterator.next());
                }
            }
        }
        for (Map.Entry<Switch, List<Long>> entry : output2.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        System.out.println("-----------------------------------------");

        return output2;
    }

    private List<List<Long>> blockify(List<Long> ipsSorted, int numberOfBlocks) {
        final int IPperBlock = (int) Math.floor(1.0 * ipsSorted.size() / numberOfBlocks);
        List<List<Long>> blocks = new LinkedList<List<Long>>();
        for (int i1 = 0; i1 < numberOfBlocks; i1++) {
            //find IPs that are in the block
            List<Long> ipsInBlock = ipsSorted.subList(IPperBlock * i1, Math.min(ipsSorted.size(), IPperBlock * (i1 + 1)));
            blocks.add(ipsInBlock);
        }
        return blocks;
    }*/

}
