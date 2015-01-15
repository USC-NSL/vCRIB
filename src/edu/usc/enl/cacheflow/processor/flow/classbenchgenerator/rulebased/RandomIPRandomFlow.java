package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.rulebased;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
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
 * Time: 9:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class RandomIPRandomFlow extends RuleBasedFlowGenerator {
    public RandomIPRandomFlow(int vmPerSource, IPAssigner ipAssigner) {
        super(ipAssigner);
    }

    public void generate(List<Rule> rules, Random random, List<Switch> sources,
                         DestinationSelector destinationSelector, CustomRandomFlowDistribution flowDistribution,
                         PrintWriter writer, Topology topology) {
        ////////////////////INIT
        fillIndexes();
        HashMap<Switch, Integer> vmsPerSourceMap = new HashMap<>();
        int needIP = flowDistribution.getVMsPerSource(random, sources, vmsPerSourceMap);
        generateRandomFlows(random, sources, destinationSelector, flowDistribution,
                ipAssigner.assignIPs(random, sources, generateIPs(random, needIP), topology, vmsPerSourceMap), writer, topology);
        writer.close();
    }

    private void generateRandomFlows(final Random random, List<Switch> sources, DestinationSelector destinationSelector,
                                     CustomRandomFlowDistribution flowDistribution,
                                     Map<Switch, List<Long>> switchTotalIPss,
                                     PrintWriter writer, Topology topology) {
        Map<Long, Switch> IPSwitchMap = new HashMap<Long, Switch>();
        for (Map.Entry<Switch, List<Long>> entry : switchTotalIPss.entrySet()) {
            for (Long ip : entry.getValue()) {
                IPSwitchMap.put(ip, entry.getKey());
            }
        }
        boolean first = true;
        final Set<Flow> microFlows = new HashSet<Flow>(1000);
        for (Switch source : sources) {
            int flowsPerSource = flowDistribution.getRandomFlowNum(random.nextDouble());
            destinationSelector.setSource(source, sources);
            final Map<? extends Object, Double> categoryProb = destinationSelector.getCategoryProb();
            final List<Long> srcIPs = switchTotalIPss.get(source);
            for (Object category : categoryProb.keySet()) {
                // Map<Integer, Integer> ruleFlow = new HashMap<Integer, Integer>();
                final int numberOfFlowsInThisCategory = (int) (Math.round(flowsPerSource * categoryProb.get(category)));
                final List<Switch> switchInCategory = destinationSelector.getSwitchInCategory(category);
                final List<Long> dstIps = new ArrayList<Long>(switchInCategory.size() * srcIPs.size());
                for (Switch aSwitch : switchInCategory) {
                    dstIps.addAll(switchTotalIPss.get(aSwitch));
                }
                while (microFlows.size() < numberOfFlowsInThisCategory) {
                    //pick a random srcIP
                    final long src = srcIPs.get(random.nextInt(srcIPs.size()));
                    //pick a random dstIP
                    final long dst = dstIps.get(random.nextInt(dstIps.size()));
                    Long[] properties = new Long[Util.getDimensionInfos().size()];
                    properties[srcIPIndex] = src;
                    properties[dstIPIndex] = dst;
                    properties[srcPortIndex] = Util.SRC_PORT_INFO.getDimensionRange().getRandomNumber(random);
                    properties[dstPortIndex] = Util.DST_PORT_INFO.getDimensionRange().getRandomNumber(random);
                    properties[protocolIndex] = Util.PROTOCOL_INFO.getDimensionRange().getRandomNumber(random);
                    microFlows.add(new Flow(flowDistribution.getRandomFlowSize(random.nextDouble()),
                            source, IPSwitchMap.get(dst), properties));
                }
                if (first) {
                    microFlows.iterator().next().headerToString(writer);
                }
                writeToWriter(random, writer, flowDistribution, microFlows);
                first = false;
            }
        }
    }


    private Set<Long> generateIPs(Random random,  int needIP) {
        final int numberOfBlocks = needIP;
        final List<RangeDimensionRange> partition = Util.SRC_IP_INFO.getDimensionRange().partition(numberOfBlocks);
        //select ips in each range
        final int vmsperBlock = 1;//vmPerSource / vmPerSource;
        Set<Long> ips = new HashSet<Long>(vmsperBlock);
        for (RangeDimensionRange rangeDimensionRange : partition) {
            while (ips.size() < vmsperBlock) {
                ips.add(rangeDimensionRange.getRandomNumber(random));
            }
        }
        return ips;
    }
}
