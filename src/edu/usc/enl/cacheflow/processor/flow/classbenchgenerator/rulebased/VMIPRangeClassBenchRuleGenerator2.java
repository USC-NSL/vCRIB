package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.rulebased;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PartitionTree2;
import edu.usc.enl.cacheflow.model.Flow;
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
public class VMIPRangeClassBenchRuleGenerator2 {
    public int blockPerSource = 50;
    public int vmPerSource = 50;
    public PartitionTree2 partitionTree2;
    public Rule defaultRule;
    public int srcPortIndex;
    public int dstPortIndex;
    public int protocolIndex;
    public int srcIPIndex;
    public int dstIPIndex;

    public VMIPRangeClassBenchRuleGenerator2(int blockPerSource, int vmPerSource) {
        this.blockPerSource = blockPerSource;
        this.vmPerSource = vmPerSource;
    }

    public List<Flow> generate(List<Rule> rules, Random random, List<Switch> sources,
                               DestinationSelector destinationSelector, CustomRandomFlowDistribution flowDistribution) {

        srcPortIndex = Util.getDimensionInfoIndex(Util.SRC_PORT_INFO);
        dstPortIndex = Util.getDimensionInfoIndex(Util.DST_PORT_INFO);
        protocolIndex = Util.getDimensionInfoIndex(Util.PROTOCOL_INFO);
        srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        dstIPIndex = Util.getDimensionInfoIndex(Util.DST_IP_INFO);

        Set<Long> ips = new HashSet<Long>();
        Map<IPTuple, List<Rule>> ipRules = new HashMap<IPTuple, List<Rule>>();
        findCorners(rules, ips, ipRules);

        defaultRule = Collections.max(rules, Rule.PRIORITY_COMPARATOR);
        long start = System.currentTimeMillis();
        partitionTree2 = new PartitionTree2();
        partitionTree2.semigridAndMergeTogether(rules,Util.getDimensionInfos(), PartitionTree2.findPermutation(rules, Util.getDimensionInfos()));
        final long t2 = System.currentTimeMillis();
        System.out.println(t2 - start);

        Map<Switch, List<Long>> switchTotalIPss = blockBasedIPGenerate(random, sources, ips);
        //Map<Switch, List<Long>> switchTotalIPss = blockBasedIPGenerate2(random, sources);

        //for each source generate flows

        final List<Flow> flows = generateFlows(random, sources, destinationSelector, flowDistribution, ipRules, switchTotalIPss);
        System.out.println(System.currentTimeMillis() - t2);
        return flows;
    }

    private List<Flow> generateFlows(Random random, List<Switch> sources, DestinationSelector destinationSelector,
                                     CustomRandomFlowDistribution flowDistribution, Map<IPTuple, List<Rule>> tupleRules,
                                     Map<Switch, List<Long>> switchTotalIPss) {
        List<Flow> outputFlows = new LinkedList<Flow>();


        Map<Integer, Integer> ruleFlow = new HashMap<Integer, Integer>();

        for (Switch source : sources) {
            Set<Flow> flows = new HashSet<Flow>(); //to have unique flows
            //get the number of flows
            int flowsPerSource = flowDistribution.getRandomFlowNum(random.nextDouble());
            destinationSelector.setSource(source, sources);
            final List<Long> srcIPs = switchTotalIPss.get(source);
            int i = 0;
            int notFound = 0;
            while (flows.size() < flowsPerSource) {
                final Long srcIP = srcIPs.get(random.nextInt(srcIPs.size()));
                final Switch destination = destinationSelector.getDestination(random, source, sources);
                final List<Long> dstIPs = switchTotalIPss.get(destination);
                final Long dstIP = dstIPs.get(random.nextInt(dstIPs.size()));
                //create tuple
                IPTuple tuple = new IPTuple(srcIP, dstIP);
                final List<Rule> rules = tupleRules.get(tuple);
                i++;
                if (rules != null) {
                    for (Rule rule : rules) {
                        if (rule.getPriority() < defaultRule.getPriority() ){
                            final List<Flow> flows1 = generateFlows(source, destination, rule, tuple, random);
                            flows.addAll(flows1);
                            final Integer integer = ruleFlow.get(rule.getPriority());
                            if (integer == null) {
                                ruleFlow.put(rule.getPriority(), flows1.size());
                            } else {
                                ruleFlow.put(rule.getPriority(), integer + flows1.size());
                            }
                        }
                    }
                }else{
                    notFound++;
                }
            }
            System.out.println(notFound + " out of " + i);
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

    public List<Flow> generateFlows(Switch src, Switch dst, Rule rule, IPTuple tuple, Random random) {
        int choices = 3;
        List<Long> template = new ArrayList<Long>(Arrays.asList(0l, 0l, 0l, 0l, 0l));

        List<Flow> output = new ArrayList<Flow>(choices * choices * choices);
        for (int i = 0; i < choices; i++) {
            final long srcPort = rule.getProperty(srcPortIndex).getRandomNumber(random);
            for (int j = 0; j < choices; j++) {
                final long dstPort = rule.getProperty(dstPortIndex).getRandomNumber(random);
                for (int k = 0; k < choices; k++) {
                    final long protocol = rule.getProperty(protocolIndex).getRandomNumber(random);
                    Long[] properties = new Long[template.size()];
                    properties[srcIPIndex]= tuple.src;
                    properties[dstIPIndex]= tuple.dst;
                    properties[srcPortIndex]= srcPort;
                    properties[dstPortIndex]= dstPort;
                    properties[protocolIndex]= protocol;
                    output.add(new Flow(0, src, dst, properties));
                }
            }
        }
        return output;
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

    private void findCorners(List<Rule> rules, Set<Long> ips, Map<IPTuple, List<Rule>> ipRules) {

        for (Rule rule : rules) {
            final long srcStart = rule.getProperty(srcIPIndex).getStart();
            final long dstStart = rule.getProperty(dstIPIndex).getStart();
            final long dstEnd = rule.getProperty(dstIPIndex).getEnd();
            final long srcEnd = rule.getProperty(srcIPIndex).getEnd();
            addRuleTuple(ipRules, rule, new IPTuple(srcStart, dstStart));
            addRuleTuple(ipRules, rule, new IPTuple(srcEnd, dstStart));
            addRuleTuple(ipRules, rule, new IPTuple(srcStart, dstEnd));
            addRuleTuple(ipRules, rule, new IPTuple(srcEnd, dstEnd));
            ips.add(srcStart);
            ips.add(srcEnd);
            ips.add(dstStart);
            ips.add(dstEnd);
        }
    }

    private void addRuleTuple(Map<IPTuple, List<Rule>> ipRules, Rule rule, IPTuple ipTuple) {
        List<Rule> list = ipRules.get(ipTuple);
        if (list == null) {
            list = new LinkedList<Rule>();
            ipRules.put(ipTuple, list);
        }
        list.add(rule);
    }

     private static class IPTuple {
        long src;
        long dst;


        public IPTuple(long src, long dst) {
            this.src = src;
            this.dst = dst;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IPTuple ipTuple = (IPTuple) o;

            if (dst != ipTuple.dst) return false;
            if (src != ipTuple.src) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (src ^ (src >>> 32));
            result = 31 * result + (int) (dst ^ (dst >>> 32));
            return result;
        }
    }

}

