package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.vmbased;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.DestinationSelector;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.IPAssigner;
import edu.usc.enl.cacheflow.util.Util;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/10/12
 * Time: 6:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class SrcVMFlowGenerator {

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    public void generate(Collection<Rule> rules, Random random, CustomRandomFlowDistribution flowDistribution, Topology topology,
                         DestinationSelector destinationSelector, IPAssigner ipAssigner,
                         String flowFileName, boolean useProtocolDstPortModel, VMIPSelector vmipSelector,
                         String vmAssignmentFileName, Map<String, Object> parameters) throws IOException {
        PrintWriter flowWriter = new PrintWriter(new BufferedWriter(new FileWriter(flowFileName)));
        flowWriter.println(Statistics.getParameterLine(parameters));
        new Flow(0, null, null, new Long[]{0l, 0l, 0l, 0l, 0l}).headerToString(flowWriter);
        final TreeMap<Double, RangeDimensionRange> probProtocolRange = new TreeMap<Double, RangeDimensionRange>();
        final HashMap<RangeDimensionRange, List<Long>> protocolSortedPortEdges = new HashMap<RangeDimensionRange, List<Long>>();
        final Map<RangeDimensionRange, TreeMap<Double, Long>> protocolDstPortModel;
        if (useProtocolDstPortModel) {
            protocolDstPortModel = createProtocolDstPortModel(rules, probProtocolRange, protocolSortedPortEdges);
        } else {
            protocolDstPortModel = createRandomDstPortModel(probProtocolRange, protocolSortedPortEdges);
        }
        final List<Switch> edges = topology.findEdges();
        Map<Switch, Integer> vmsPerSourceMap = new HashMap<>();
        int vmsNum = flowDistribution.getVMsPerSource(random, edges, vmsPerSourceMap);


        final Collection<Long> ips = vmipSelector.getIps(rules, random, vmsNum);
        //assign vm addresses
        final Map<Switch, List<Long>> switchListMap = ipAssigner.assignIPs(random, edges, ips, topology, vmsPerSourceMap);
        //write ips
        writeVMAssignment(vmAssignmentFileName, switchListMap, parameters);

        ips.clear();
        final int srcPortIndex = Util.getDimensionInfoIndex(Util.SRC_PORT_INFO);
        final int dstPortIndex = Util.getDimensionInfoIndex(Util.DST_PORT_INFO);
        final int protocolIndex = Util.getDimensionInfoIndex(Util.PROTOCOL_INFO);
        final int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        final int dstIPIndex = Util.getDimensionInfoIndex(Util.DST_IP_INFO);
        List<Flow> hostFlows = new ArrayList<>(1000);
        for (Switch source : edges) {
            int flowsPerSource = flowDistribution.getRandomFlowNum(random.nextDouble());
            destinationSelector.setSource(source, edges);
            final List<Long> srcIPs = switchListMap.get(source);
            hostFlows.clear();
            final Map<? extends Object, Double> categoryProb = destinationSelector.getCategoryProb();
            for (Object category : categoryProb.keySet()) {
                final int numberOfFlowsInThisCategory = (int) (Math.round(flowsPerSource * categoryProb.get(category)));
                final List<Switch> switchInCategory = destinationSelector.getSwitchInCategory(category);
                final Set<Flow> microFlows = new HashSet<Flow>(numberOfFlowsInThisCategory);
                while (microFlows.size() < numberOfFlowsInThisCategory) {
                    final Switch destination = switchInCategory.get(random.nextInt(switchInCategory.size()));
                    final List<Long> destIPs = switchListMap.get(destination);
                    final Long[] properties = new Long[5];
                    properties[srcIPIndex] = srcIPs.get(random.nextInt(srcIPs.size()));
                    properties[dstIPIndex] = destIPs.get(random.nextInt(destIPs.size()));
                    properties[srcPortIndex] = Util.SRC_PORT_INFO.getDimensionRange().getRandomNumber(random);
                    //select protocol and destination port based on the model of rules
                    fillProtocolDstPort(random, probProtocolRange, protocolSortedPortEdges, protocolDstPortModel, dstPortIndex, protocolIndex, properties);
                    microFlows.add(new Flow(flowDistribution.getRandomFlowSize(random.nextDouble()), source, destination, properties));
                }
                hostFlows.addAll(microFlows);
//                for (Flow flow : microFlows) {
//                    flow.toString(writer);
//                }
                microFlows.clear();
            }
            Collections.shuffle(hostFlows, random);
            int newFlowsNum = flowDistribution.getRandomNewFlowsNum(random.nextDouble());
            int i = 0;
            for (Flow hostFlow : hostFlows) {
                if (i < newFlowsNum) {
                    hostFlow.setNew(true);
                } else {
                    hostFlow.setNew(false);
                }
                i++;
            }
            for (Flow flow : hostFlows) {
                flow.toString(flowWriter);
            }
        }
        flowWriter.close();
    }

    public static void writeVMAssignment(String vmAssignmentFileName, Map<Switch, List<Long>> switchListMap, Map<String, Object> parameters) {
        try {
            PrintWriter pw = new PrintWriter(vmAssignmentFileName);
            pw.println(Statistics.getParameterLine(parameters));
            for (Map.Entry<Switch, List<Long>> entry : switchListMap.entrySet()) {
                pw.print(entry.getKey());
                for (Long ip : entry.getValue()) {
                    pw.print("," + ip);
                }
                pw.println();
            }
            pw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private Map<RangeDimensionRange, TreeMap<Double, Long>> createRandomDstPortModel(
            TreeMap<Double, RangeDimensionRange> probProtocolRange, HashMap<RangeDimensionRange,
            List<Long>> protocolSortedPortEdges) {
        Map<RangeDimensionRange, TreeMap<Double, Long>> output = new HashMap<RangeDimensionRange, TreeMap<Double, Long>>();
        RangeDimensionRange protocolRange = Util.PROTOCOL_INFO.getDimensionRange();
        RangeDimensionRange dstDimensionRange = Util.DST_PORT_INFO.getDimensionRange();
        probProtocolRange.put(1d, protocolRange);
        protocolSortedPortEdges.put(protocolRange,
                Arrays.asList(dstDimensionRange.getStart(), dstDimensionRange.getEnd()));
        TreeMap<Double, Long> onlyTreeMap = new TreeMap<Double, Long>();
        onlyTreeMap.put(1d, dstDimensionRange.getStart());
        output.put(protocolRange, onlyTreeMap);
        return output;
    }

    private void fillProtocolDstPort(Random random, TreeMap<Double, RangeDimensionRange> probProtocolRange,
                                     Map<RangeDimensionRange, List<Long>> protocolSortedPortEdges,
                                     Map<RangeDimensionRange, TreeMap<Double, Long>> protocolDestPortModel,
                                     int dstPortIndex, int protocolIndex, Long[] properties) {
        RangeDimensionRange protocolRange = probProtocolRange.ceilingEntry(random.nextDouble()).getValue();
        TreeMap<Double, Long> protocolRangeCDF = protocolDestPortModel.get(protocolRange);
        Long dstPortStart = protocolRangeCDF.ceilingEntry(random.nextDouble()).getValue();
        List<Long> sortedPortEdges = protocolSortedPortEdges.get(protocolRange);
        int index = Collections.binarySearch(sortedPortEdges, dstPortStart);
        long dstPortEnd_1 = sortedPortEdges.get(index + 1);
        properties[protocolIndex] = protocolRange.getRandomNumber(random);
        properties[dstPortIndex] = (long) (dstPortStart + (dstPortEnd_1 - dstPortStart) * random.nextDouble());
    }


    private Map<RangeDimensionRange, TreeMap<Double, Long>> createProtocolDstPortModel(
            Collection<Rule> rules,
            TreeMap<Double, RangeDimensionRange> probProtocolRange
            , Map<RangeDimensionRange, List<Long>> rangeSortedIps //need this to find the end of a start
    ) {
        Map<RangeDimensionRange, List<Rule>> protocolRule = createProtocolModel(rules, probProtocolRange);


        final int dstPortIndex = Util.getDimensionInfoIndex(Util.DST_PORT_INFO);
        double roundingFactor = Math.pow(10, Math.round(Math.log(rules.size() * 2) / Math.log(10)));
        //for each range of protocol we need a kind of CDF of start of region of destination port
        Map<RangeDimensionRange, TreeMap<Double, Long>> output = new HashMap<RangeDimensionRange, TreeMap<Double, Long>>();
        for (Map.Entry<RangeDimensionRange, List<Rule>> entry : protocolRule.entrySet()) {
            RangeDimensionRange protocolRange = entry.getKey();
            List<Rule> thisProtocolRules = entry.getValue();
            TreeMap<Long, Double> startProb = new TreeMap<Long, Double>();
            double residual = 1.0 / thisProtocolRules.size();
            for (Rule rule : thisProtocolRules) {
                RangeDimensionRange property = rule.getProperty(dstPortIndex);
                long start = property.getStart();
                long end = property.getEnd();

                addPoint(startProb, start);
                addPoint(startProb, end + 1);

                NavigableMap<Long, Double> affected = startProb.subMap(start, true, end, true);
                for (Map.Entry<Long, Double> affectedEntry : affected.entrySet()) {
                    affectedEntry.setValue(affectedEntry.getValue() + residual / affected.size());
                }
            }

            //now compute CDF out of probabilities
            double sum = 0;
            Map.Entry<Long, Double> lastEntry = null;
            Map.Entry<Long, Double> oneToLastEntry = null;
            for (Map.Entry<Long, Double> startProbEntry : startProb.entrySet()) {
                Double v = startProbEntry.getValue();
                sum += Math.min(1.0 * Math.round((v * roundingFactor)) / roundingFactor, 1);
                startProbEntry.setValue(sum);
                oneToLastEntry = lastEntry;
                lastEntry = startProbEntry;
            }

            //add last entry to the list
            rangeSortedIps.put(protocolRange, new ArrayList<Long>(startProb.keySet()));
            //remove last entry from probabilities as it is out of range
            if (lastEntry != null) {
                startProb.remove(lastEntry.getKey());
                oneToLastEntry.setValue(1d);//cannot be null as if there is any entry there are 2 entries (Start and end)
            }

            TreeMap<Double, Long> probStart = new TreeMap<Double, Long>();
            for (Map.Entry<Long, Double> entry1 : startProb.entrySet()) {
                probStart.put(entry1.getValue(), entry1.getKey());
            }
            output.put(protocolRange, probStart);
        }


        return output;
    }

    private void addPoint(TreeMap<Long, Double> startProb, long point) {
        //does start of current rule create a new edge in the CDF
        Map.Entry<Long, Double> previous = startProb.floorEntry(point);
        if (previous == null) {
            startProb.put(point, 0d);
        } else if (previous.getKey() < point) {
            double value = previous.getValue() / 2;
            startProb.put(previous.getKey(), value);
            startProb.put(point, value);
        }
    }

    private Map<RangeDimensionRange, List<Rule>> createProtocolModel(Collection<Rule> rules, TreeMap<Double, RangeDimensionRange> probProtocolRange) {
        final int protocolIndex = Util.getDimensionInfoIndex(Util.PROTOCOL_INFO);
        //create probabilities
        final double protocolResidual = 1.0 / rules.size();
        Map<RangeDimensionRange, List<Rule>> protocolRule = new HashMap<RangeDimensionRange, List<Rule>>();
        Map<RangeDimensionRange, Double> protocolRangeProb = new HashMap<RangeDimensionRange, Double>();
        Double prob;
        for (Rule rule : rules) {
            RangeDimensionRange protocol = rule.getProperty(protocolIndex);
            List<Rule> rules1 = protocolRule.get(protocol);
            if (rules1 == null) {
                rules1 = new LinkedList<Rule>();
                protocolRule.put(protocol, rules1);
                prob = 0d;
            } else {
                prob = protocolRangeProb.get(protocol);
            }
            protocolRangeProb.put(protocol, prob + protocolResidual);
            rules1.add(rule);
        }
        //create CDF
        {
            double sum = 0;
            Map.Entry<RangeDimensionRange, Double> lastEntry = null;
            for (Map.Entry<RangeDimensionRange, Double> entry : protocolRangeProb.entrySet()) {
                sum += Math.min(1d, Math.round(entry.getValue() * 1000) / 1000.0);
                entry.setValue(sum);
                lastEntry = entry;
            }
            if (lastEntry != null) {
                lastEntry.setValue(1d);
            }
            for (Map.Entry<RangeDimensionRange, Double> entry : protocolRangeProb.entrySet()) {
                probProtocolRange.put(entry.getValue(), entry.getKey());
            }
            protocolRangeProb.clear();
        }
        return protocolRule;
    }

}
