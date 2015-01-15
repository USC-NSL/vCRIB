package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.rulebased;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PartitionTree2;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.DestinationSelector;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/26/12
 * Time: 10:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class VMIPRangeClassBenchRuleGenerator {
    public int blockPerSource = 50;
    public int vmPerSource = 50;
    public PartitionTree2 partitionTree2;
    public Rule defaultRule;
    public final double defaultRuleProbability = 0.1;

    public VMIPRangeClassBenchRuleGenerator(int blockPerSource, int vmPerSource) {
        this.blockPerSource = blockPerSource;
        this.vmPerSource = vmPerSource;
    }

    public List<Flow> generate(List<Rule> rules, Random random, List<Switch> sources,
                               DestinationSelector destinationSelector, CustomRandomFlowDistribution flowDistribution) {

        Set<Long> ips = new HashSet<Long>();
        Map<Integer, List<Long>> corners = new HashMap<Integer, List<Long>>();
        findCorners(rules, ips, corners);

        defaultRule = Collections.max(rules, Rule.PRIORITY_COMPARATOR);
        long start = System.currentTimeMillis();
        partitionTree2 = new PartitionTree2();
        partitionTree2.semigridAndMergeTogether(rules,Util.getDimensionInfos(), PartitionTree2.findPermutation(rules, Util.getDimensionInfos()));
        final long t2 = System.currentTimeMillis();
        System.out.println(t2 - start);

        Map<Switch, List<Long>> switchTotalIPss = blockBasedIPGenerate(random, sources, ips);
        //Map<Switch, List<Long>> switchTotalIPss = blockBasedIPGenerate2(random, sources);

        //for each source generate flows

        final List<Flow> flows = generateFlows(random, sources, destinationSelector, flowDistribution, corners, switchTotalIPss);
        System.out.println(System.currentTimeMillis() - t2);
        return flows;
    }

    private List<Flow> generateFlows(Random random, List<Switch> sources, DestinationSelector destinationSelector,
                                     CustomRandomFlowDistribution flowDistribution, Map<Integer, List<Long>> corners,
                                     Map<Switch, List<Long>> switchTotalIPss) {
        List<Flow> outputFlows = new LinkedList<Flow>();
        final List<Long> dstPorts = corners.get(Util.getDimensionInfoIndex(Util.DST_PORT_INFO));
        final List<Long> srcPorts = corners.get(Util.getDimensionInfoIndex(Util.SRC_PORT_INFO));
        final List<Long> protocol = corners.get(Util.getDimensionInfoIndex(Util.PROTOCOL_INFO));


        Map<Integer, Integer> ruleFlow = new HashMap<Integer, Integer>();

        for (Switch source : sources) {
            Set<Flow> flows = new HashSet<Flow>(); //to have unique flows
            //get the number of flows
            int flowsPerSource = flowDistribution.getRandomFlowNum(random.nextDouble());
            destinationSelector.setSource(source, sources);
            final List<Long> srcIPs = switchTotalIPss.get(source);
            int i = 0;
            int matchedByDefault = 0;
            while (flows.size() < flowsPerSource) {
                SortedMap<DimensionInfo, Long> properties = new TreeMap<DimensionInfo, Long>();
                properties.put(Util.SRC_IP_INFO, srcIPs.get(random.nextInt(srcIPs.size())));
                final Switch destination = destinationSelector.getDestination(random, source, sources);
                final List<Long> dstIPs = switchTotalIPss.get(destination);
                properties.put(Util.DST_IP_INFO, dstIPs.get(random.nextInt(dstIPs.size())));
                properties.put(Util.DST_PORT_INFO, dstPorts.get(random.nextInt(dstPorts.size())));
                properties.put(Util.SRC_PORT_INFO, srcPorts.get(random.nextInt(srcPorts.size())));
                properties.put(Util.PROTOCOL_INFO, protocol.get(random.nextInt(protocol.size())));

                Flow flow = new Flow(0,
                        source, destination, properties);
                final Rule rule = partitionTree2.getRule(flow, true);
                if (rule.getPriority() == defaultRule.getPriority()) {
                    if (random.nextDouble() > defaultRuleProbability - 1.0 * matchedByDefault / flowsPerSource) {
                        continue;
                    } else {
                        matchedByDefault++;
                    }
                }
                i++;
                final Integer integer = ruleFlow.get(rule.getPriority());
                if (integer == null) {
                    ruleFlow.put(rule.getPriority(), 1);
                } else {
                    ruleFlow.put(rule.getPriority(), integer + 1);
                }
                flows.add(flow);
            }
            System.out.println(matchedByDefault + " out of " + i);
            for (Flow flow : flows) {
                flow.setTraffic(flowDistribution.getRandomFlowSize(random.nextDouble()));
                outputFlows.add(flow);
            }
        }
        for (Map.Entry<Integer, Integer> entry : ruleFlow.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        return outputFlows;
    }

    private Map<Switch, List<Long>> blockBasedIPGenerate2(Random random, List<Switch> sources) {
        final int numberOfBlocks = sources.size() * blockPerSource;
        final List<RangeDimensionRange> partition = Util.SRC_IP_INFO.getDimensionRange().partition(numberOfBlocks);
        //select ips in each range
        List<List<Long>> blocks = new LinkedList<List<Long>>();
        final int vmsperBlock = vmPerSource / blockPerSource;
        for (RangeDimensionRange rangeDimensionRange : partition) {
            Set<Long> ips = new HashSet<Long>(vmsperBlock);
            while(ips.size()<vmsperBlock){
                ips.add(rangeDimensionRange.getRandomNumber(random));
            }
            blocks.add(new ArrayList<Long>(ips));
        }

        //now for each source pick k blocks
        Collections.shuffle(blocks, random);

        //assign blocks to machines
        final Iterator<List<Long>> iterator = blocks.iterator();
        Map<Switch, List<Long>> switchTotalIPss = new HashMap<Switch, List<Long>>();
        for (Switch source : sources) {
            switchTotalIPss.put(source, new LinkedList<Long>());
        }
        for (Switch source : sources) {
            for (int i = 0; i < blockPerSource; i++) {
                final List<Long> next = iterator.next();
                switchTotalIPss.get(source).addAll(next);
            }
        }
        return switchTotalIPss;
    }

    private Map<Switch, List<Long>> blockBasedIPGenerate(Random random, List<Switch> sources, Set<Long> ips) {
        final int numberOfBlocks = sources.size() * blockPerSource;
        List<Long> ipsSorted = new ArrayList<Long>(ips);
        final int IPperBlock = (int) Math.floor(1.0 * ipsSorted.size() / numberOfBlocks);
        final int vmsperBlock = vmPerSource / blockPerSource;
        Collections.sort(ipsSorted);
        List<List<Long>> blocks = new LinkedList<List<Long>>();
        for (int i1 = 0; i1 < numberOfBlocks; i1++) {
            //find IPs that are in the block
            List<Long> ipsInBlock = ipsSorted.subList(IPperBlock * i1, Math.min(ipsSorted.size(), IPperBlock * (i1 + 1)));
            Collections.shuffle(ipsInBlock, random);
            blocks.add(ipsInBlock.subList(0, vmsperBlock));
        }

        //now for each source pick k blocks
        Collections.shuffle(blocks, random);


        //assign blocks to machines
        final Iterator<List<Long>> iterator = blocks.iterator();
        Map<Switch, List<Long>> switchTotalIPss = new HashMap<Switch, List<Long>>();
        for (Switch source : sources) {
            switchTotalIPss.put(source, new LinkedList<Long>());
        }
        for (Switch source : sources) {
            for (int i = 0; i < blockPerSource; i++) {
                final List<Long> next = iterator.next();
                switchTotalIPss.get(source).addAll(next);
            }
        }
        return switchTotalIPss;
    }

    private void findCorners(List<Rule> rules, Set<Long> ips, Map<Integer, List<Long>> corners) {
        Map<Integer, Set<Long>> tempCorners = new HashMap<Integer, Set<Long>>();
        for (int i = 0; i < Util.getDimensionInfos().size(); i++) {
            tempCorners.put(i, new HashSet<Long>());
        }

        for (Rule rule : rules) {
            int i = 0;
            for (RangeDimensionRange rangeDimensionRange : rule.getProperties()) {
                tempCorners.get(i).add(rangeDimensionRange.getStart());
                tempCorners.get(i).add(rangeDimensionRange.getEnd());
                i++;
            }
        }

        final int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        final int dstIPIndex = Util.getDimensionInfoIndex(Util.DST_IP_INFO);
        ips.addAll(tempCorners.remove(srcIPIndex));
        ips.addAll(tempCorners.remove(dstIPIndex));

        for (Map.Entry<Integer, Set<Long>> entry : tempCorners.entrySet()) {
            corners.put(entry.getKey(), new ArrayList<Long>(entry.getValue()));
        }
    }



}

