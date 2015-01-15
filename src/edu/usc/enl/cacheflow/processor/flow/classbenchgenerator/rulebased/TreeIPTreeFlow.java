package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.rulebased;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.Persistanter;
import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PersistentPartitionTree2;
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
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/24/12
 * Time: 9:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class TreeIPTreeFlow extends RuleBasedFlowGenerator {
    private PersistentPartitionTree2 partitionTree2;

    /*Set<Integer> rules1 = new HashSet<Integer>();
    Set<Integer> rules2 = new HashSet<Integer>();*/
    public final Long[] template = new Long[]{0l, 0l, 0l, 0l, 0l};
    public final int maxFlowPerTuple = 1;
    public final int matchLoadSize = 10000;
    public final int maxBlockPerRule = 1000000;
    public final int LoadLeavesBufferSize = 100000;
    //public Map<Integer, Rule> idRuleMap;

    public TreeIPTreeFlow(int vmPerSource, IPAssigner ipAssigner) {
        super(ipAssigner);
    }

    public void generate(List<Rule> rules, final Random random, final List<Switch> sources,
                         final DestinationSelector destinationSelector, final CustomRandomFlowDistribution flowDistribution,
                         final PrintWriter writer, Topology topology) {
        generate(rules.size(), random, sources, destinationSelector, flowDistribution, writer, topology);
    }

    public void generate(int ruleSize, final Random random, final List<Switch> sources,
                         final DestinationSelector destinationSelector, final CustomRandomFlowDistribution flowDistribution,
                         final PrintWriter writer, Topology topology) {
        /*idRuleMap = new HashMap<Integer, Rule>();
        for (Rule rule : rules) {
            idRuleMap.put(rule.getId(), rule);
        }
        this.rules=rules;
        Collections.sort(rules,Rule.PRIORITY_COMPARATOR);*/

        //////////////////// INIT
        super.fillIndexes();
        long start = System.currentTimeMillis();
        //////////////////// Assign IPs
        final Map<Long, Object> corners2;
        final Map<Switch, List<Long>> switchTotalIPss;
        {
            Map<Switch, Integer> vmsPerSourceMap = new HashMap<>();
            int vmsNum = flowDistribution.getVMsPerSource(random, sources, vmsPerSourceMap);

            Set<Long> ips = new HashSet<Long>(vmsNum);
            corners2 = findCorners2(ruleSize, ips, random,vmsNum);

            System.out.println("-----------------------------------------");
            switchTotalIPss = ipAssigner.assignIPs(random, sources, ips,topology, vmsPerSourceMap);
        }
        System.out.println(System.currentTimeMillis() - start);
        ////////////////// Generate Flows
        Collections.shuffle(sources, random);//
        new Flow(0, sources.get(0), sources.get(0), template).headerToString(writer);

        //generateFlowsTuples(random, sources, destinationSelector, flowDistribution, switchTotalIPss, writer, corners2, partitionTree2.getMatchStatementSrc(), new HashSet<Integer>());


        final int threadsNum = 5;
        final int perThreadNum = sources.size() / threadsNum;
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < threadsNum; i++) {
            final int finalI = i;
            final Thread thread = new Thread() {
                @Override
                public void run() {
                    generateFlowsTuples(random, sources.subList(finalI * perThreadNum, Math.min(sources.size(), (finalI + 1) * perThreadNum)),
                            destinationSelector.createNew(), flowDistribution, switchTotalIPss,
                            writer, corners2, null, Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>()));
                    //partitionTree2.getMatchStatementSrc()
                }
            };
            threads.add(thread);
            thread.start();
        }
        try {
            for (Thread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /////////////////////////// FINALIZE
        partitionTree2.close();
        //////////////////////////////////
        writer.close();
        /*List<Integer> rules3 = new ArrayList<Integer>(rules1);
        List<Integer> rules4 = new ArrayList<Integer>(rules2);
        Collections.sort(rules3);
        Collections.sort(rules4);
        for (Integer rule : rules3) {
            System.out.println(rule);
        }
        System.out.println("=========================");
        for (Integer rule : rules4) {
            System.out.println(rule);
        }*/
    }


    private Map<Long, Object> findCorners2(int ruleSize, Set<Long> ips, final Random random, int minNum) {
        initTree(ruleSize);
        final Map<Integer, Set<IPTuple>> ruleTuplesMap = createTuples(random, minNum, ips, ruleSize);
        return pruneIPs2(ips, random, minNum, ruleTuplesMap);
    }

    private void initTree(int ruleSize) {
        final List<Integer> permutation = new ArrayList<Integer>(Arrays.asList(
                Util.getDimensionInfoIndex(Util.SRC_IP_INFO),
                Util.getDimensionInfoIndex(Util.DST_IP_INFO),
                Util.getDimensionInfoIndex(Util.PROTOCOL_INFO),
                Util.getDimensionInfoIndex(Util.DST_PORT_INFO),
                Util.getDimensionInfoIndex(Util.SRC_PORT_INFO)
        ));
        {
            partitionTree2 = new PersistentPartitionTree2();
            //partitionTree2.semigridAndMergeTogether(rules, Util.getDimensionInfos(), permutation);
            partitionTree2.init(ruleSize, Util.getDimensionInfos(), permutation);
            System.out.println("partition tree finished " + System.currentTimeMillis() / 1000);
        }
    }

    private Map<Long, Object> pruneIPs2(Set<Long> ips, Random random, int minNum, Map<Integer, Set<IPTuple>> ruleTuplesMap) {
        ips.clear();//!! it can be over filled
        Map<Long, Object> output = new HashMap<Long, Object>();
        //prune ips as the number can be higher than what is needed. If it is higher then in the selection some dst may come in
        //without their src in
        List<Integer> rulesWithTuple = new ArrayList<Integer>(ruleTuplesMap.size());
        while (ips.size() < minNum) {
            rulesWithTuple.clear();
            rulesWithTuple.addAll(ruleTuplesMap.keySet());
            Collections.shuffle(rulesWithTuple, random);
            for (Integer rule : rulesWithTuple) {
                if (ips.size() >= minNum) {
                    break;
                }
                final Set<IPTuple> ipTuples = ruleTuplesMap.get(rule);
                if (ipTuples == null) {
                    continue;
                } else if (ipTuples.size() == 0) {
                    ruleTuplesMap.remove(rule);
                    continue;
                }
                //rules1.add(rule);
                final Iterator<IPTuple> itr = ipTuples.iterator();
                IPTuple tuple = itr.next();
                itr.remove();
                final long src = tuple.src;
                final long dst = tuple.dst;
                Object objectsForSrc = output.get(src);
                if (objectsForSrc == null) {
                    //there is no tuple for this source just put the tuple
                    output.put(src, tuple);
                } else {
                    if (objectsForSrc instanceof Map) {
                        final Map<Long, Object> dstMapForSrc = (Map<Long, Object>) objectsForSrc;
                        final Object tuplesForDst = dstMapForSrc.get(dst);
                        if (tuplesForDst == null) {
                            //just add the tuple
                            dstMapForSrc.put(dst, tuple);
                        } else {
                            //check if it is a list
                            if (tuplesForDst instanceof List) {
                                //just add to list
                                ((List) tuplesForDst).add(tuple);
                            } else {
                                //need to create a list
                                final IPTuple oldTuple = (IPTuple) tuplesForDst;
                                final LinkedList<IPTuple> tuplesList = new LinkedList<IPTuple>();
                                tuplesList.add(oldTuple);
                                tuplesList.add(tuple);
                                dstMapForSrc.put(dst, tuplesList);
                            }
                        }
                    } else {
                        //Oops I need a map
                        Map<Long, Object> objectsForSrc2 = new HashMap<Long, Object>();
                        output.put(src, objectsForSrc2);
                        final IPTuple oldTuple = (IPTuple) objectsForSrc;
                        if (oldTuple.dst == dst) {
                            //need a list
                            final LinkedList<IPTuple> tuplesList = new LinkedList<IPTuple>();
                            tuplesList.add(tuple);
                            tuplesList.add(oldTuple);
                            objectsForSrc2.put(dst, tuplesList);
                        } else {
                            //no need for a list just add this tuple
                            objectsForSrc2.put(oldTuple.dst, oldTuple);
                            objectsForSrc2.put(dst, tuple);
                        }
                    }
                }
                ips.add(src);
                ips.add(dst);
            }
        }
        return output;
    }

    private Map<Integer, Set<IPTuple>> createTuples(Random random, int minNum, Set<Long> ips, int rulesSize) {
        final Map<Integer, Set<IPTuple>> ruleTuplesMap = new HashMap<Integer, Set<IPTuple>>(rulesSize);
        /*final int srcIPLevel = permutation.indexOf(Util.getDimensionInfoIndex(Util.SRC_IP_INFO));
        final int dstIPLevel = permutation.indexOf(Util.getDimensionInfoIndex(Util.DST_IP_INFO));*/
        final GatherAndSelectBlockNumbers gatherAndSelectBlockNumbers = new GatherAndSelectBlockNumbers(maxBlockPerRule, random, rulesSize);
        int run = 0;
        while (ips.size() < minNum) {
            gatherAndSelectBlockNumbers.init();
            partitionTree2.runActionOnLeavesRuleBased2(gatherAndSelectBlockNumbers, LoadLeavesBufferSize);
            gatherAndSelectBlockNumbers.finish();
            partitionTree2.runActionOn(new CollectIPPairs(random, ruleTuplesMap, ips, run > 0),
                    gatherAndSelectBlockNumbers.selectedBlocks, LoadLeavesBufferSize);
            run++;
            System.out.println(ips.size() + " vs " + minNum);
        }
        return ruleTuplesMap;
    }

    private void generateFlowsTuples(final Random random, List<Switch> sources, DestinationSelector destinationSelector,
                                     CustomRandomFlowDistribution flowDistribution,
                                     final Map<Switch, List<Long>> switchTotalIPss,
                                     final PrintWriter writer, Map<Long, Object> tuples,
                                     Statement statement, Set<Integer> allSeenRules) {
        Map<Long, Switch> reverseIPMap = new HashMap<Long, Switch>();
        for (Map.Entry<Switch, List<Long>> entry : switchTotalIPss.entrySet()) {
            for (Long ip : entry.getValue()) {
                reverseIPMap.put(ip, entry.getKey());
            }
        }
        List<Persistanter.BlockEntry> buffer = null;
        if (statement != null) {
            buffer = createBufferForMatchSrc(matchLoadSize);
        }
        final Set<Flow> microFlows = new HashSet<Flow>(1000);
        for (final Switch source : sources) {
            System.out.println(source);

            //get the number of flows
            int flowsPerSource = flowDistribution.getRandomFlowNum(random.nextDouble());
            destinationSelector.setSource(source, switchTotalIPss.keySet());
            final List<Long> srcIPs = switchTotalIPss.get(source);
            int i = 0;
            final Map<? extends Object, Double> categoryProb = destinationSelector.getCategoryProb();
            for (Object category : categoryProb.keySet()) {
                System.out.println("Category " + category);
                System.out.flush();
                final int numberOfFlowsInThisCategory = (int) (Math.round(flowsPerSource * categoryProb.get(category)));
                final List<Switch> switchInCategory = destinationSelector.getSwitchInCategory(category);
                Set<Long> destinationIPs = new HashSet<Long>(srcIPs.size() * switchInCategory.size());
                for (Switch aDestination : switchInCategory) {
                    destinationIPs.addAll(switchTotalIPss.get(aDestination));
                }
                LinkedList<IPTuple> foundTuples = new LinkedList<IPTuple>();
                Collections.shuffle(srcIPs, random);
                for (Long srcIP : srcIPs) {
                    final Object dstIPTuples = tuples.get(srcIP);
                    if (dstIPTuples == null) {
                        continue;
                    }
                    if (dstIPTuples instanceof IPTuple) {
                        //only one tuple
                        final IPTuple tuple = (IPTuple) dstIPTuples;
                        if (destinationIPs.contains(tuple.dst)) {
                            //good tuple
                            //generate flow
                            foundTuples.add(tuple);
                        }
                    } else {
                        //it is a map
                        final Map<Long, Object> dstIPTuplesMap = (Map<Long, Object>) dstIPTuples;
                        for (Long dst : dstIPTuplesMap.keySet()) {
                            if (destinationIPs.contains(dst)) {
                                //good destination
                                final Object dstTuples = dstIPTuplesMap.get(dst);
                                if (dstTuples instanceof IPTuple) {
                                    //only one tuple
                                    //good tuple
                                    //generate flow
                                    final IPTuple tuple = (IPTuple) dstTuples;
                                    foundTuples.add(tuple);
                                } else {
                                    //it is a list
                                    for (IPTuple tuple : (List<IPTuple>) dstTuples) {
                                        //good tuple
                                        //generate flow
                                        foundTuples.add(tuple);
                                    }
                                }
                            }
                        }
                    }
                }

                //in case we have so many tuples set priority for not seen rules
                if (foundTuples.size() > 0) {
                    Set<Integer> tempSeenRules = new HashSet<Integer>(allSeenRules);
                    //just
                    int size = foundTuples.size();
                    for (int j = 0; j < size; j++) {
                        final IPTuple tuple = foundTuples.pop();
                        if (!tempSeenRules.contains(tuple.ruleId)) {
                            foundTuples.add(tuple);
                            tempSeenRules.add(tuple.ruleId);
                        }
                    }
                    if (foundTuples.size() > 0) {

                        int flowPerTuple = Math.min(Math.max(1, numberOfFlowsInThisCategory / foundTuples.size()), maxFlowPerTuple);
                        for (IPTuple tuple : foundTuples) {
                            allSeenRules.add(tuple.ruleId);
                            //rules2.add(tuple.ruleId);
                            for (int t = 0; t < flowPerTuple; t++) {
                                if (microFlows.size() >= numberOfFlowsInThisCategory) {
                                    break;
                                }
                                microFlows.add(generateFlowTuple(random, source, reverseIPMap.get(tuple.dst), tuple.src, tuple.dst, tuple.getDstPort(), tuple.getProtocol(),
                                        Util.SRC_PORT_INFO.getDimensionRange()));
                            }
                            if (microFlows.size() >= numberOfFlowsInThisCategory) {
                                break;
                            }
                        }
                        //System.out.println(microFlows.size() + " flows generated out of " + foundTuples.size() + " original tuples");
                    }
                    if (allSeenRules.size() < tempSeenRules.size()) {
                        tempSeenRules.removeAll(allSeenRules);
                        System.out.println("skipped rules");
                        System.out.println(tempSeenRules);
                        System.out.println("+++++++++++++++++++++++++++++++");
                    }

                }
                if (statement != null && microFlows.size() < numberOfFlowsInThisCategory) {
                    //generateRandomFlowsByDB(random, switchTotalIPss, microFlows, source, numberOfFlowsInThisCategory, switchInCategory, allSeenRules, statement);

                    generateRandomFlowsByDBAllSrc(random, switchTotalIPss, microFlows, source, numberOfFlowsInThisCategory, allSeenRules, statement,
                            destinationIPs, reverseIPMap, buffer);
                }
                //create random flows
                if (microFlows.size() < numberOfFlowsInThisCategory) {
                    generateRandomFlows(random, switchTotalIPss, microFlows, source, numberOfFlowsInThisCategory, switchInCategory);
                }
                synchronized (writer) {
                    writeToWriter(random, writer, flowDistribution, microFlows);
                }
                microFlows.clear();
            }
        }
    }

    private void generateRandomFlows(Random random, Map<Switch, List<Long>> switchTotalIPss, Set<Flow> microFlows,
                                     Switch source, int numberOfFlowsInThisCategory, List<Switch> switchInCategory) {
        List<Long> srcIPs = switchTotalIPss.get(source);
        while (microFlows.size() < numberOfFlowsInThisCategory) {
            Long srcIP = srcIPs.get(random.nextInt(srcIPs.size()));
            Switch destMachine = switchInCategory.get(random.nextInt(switchInCategory.size()));
            final List<Long> destIPs = switchTotalIPss.get(destMachine);
            Long destIP = destIPs.get(random.nextInt(destIPs.size()));
            //create flow
            microFlows.add(generateFlowTuple(random, source, destMachine, srcIP, destIP, Util.DST_PORT_INFO.getDimensionRange(),
                    Util.PROTOCOL_INFO.getDimensionRange(), Util.SRC_PORT_INFO.getDimensionRange()));
        }
    }

    public List<Persistanter.BlockEntry> createBufferForMatchSrc(int size) {
        List<Persistanter.BlockEntry> buffer = new ArrayList<Persistanter.BlockEntry>(size);
        for (int i = 0; i < size; i++) {
            final RangeDimensionRange[] ranges = new RangeDimensionRange[3];
            ranges[0] = new RangeDimensionRange(0, 0, Util.DST_IP_INFO);
            ranges[1] = new RangeDimensionRange(0, 0, Util.PROTOCOL_INFO);
            ranges[2] = new RangeDimensionRange(0, 0, Util.DST_PORT_INFO);
            buffer.add(new Persistanter.BlockEntry(0, ranges));
        }
        return buffer;
    }

    private void generateRandomFlowsByDBAllSrc(Random random, Map<Switch, List<Long>> switchTotalIPss,
                                               Set<Flow> microFlows, Switch source, int numberOfFlowsInThisCategory,
                                               Set<Integer> allSeenRules,
                                               Statement statement, Collection<Long> destinationIPs,
                                               Map<Long, Switch> reverseIPMap,
                                               List<Persistanter.BlockEntry> buffer) {
        final GenerateFlowActionOnLeafSrc generateFlowActionOnLeaf = new GenerateFlowActionOnLeafSrc();
        List<Long> srcIPs = switchTotalIPss.get(source);
        //long start = System.currentTimeMillis();
        //int oldMicroflowSize = microFlows.size();
        for (final Long srcIP : srcIPs) {
            generateFlowActionOnLeaf.init(allSeenRules, microFlows, numberOfFlowsInThisCategory,
                    source, srcIP, random, destinationIPs, reverseIPMap);
            partitionTree2.runOnMatchSrc(srcIP, buffer, generateFlowActionOnLeaf, statement, buffer.size());
            //allSeenRules.addAll(categorySeenRules);
            if (microFlows.size() >= numberOfFlowsInThisCategory) {
                //System.out.println("early return " + (System.currentTimeMillis() - start) + " with " + (numberOfFlowsInThisCategory - oldMicroflowSize) + " new flows");
                return;
            }
        }

        //System.out.println((System.currentTimeMillis() - start) / 1000.0 + " seconds for db  with " + (microFlows.size() - oldMicroflowSize) + " new flows");
    }


    private Flow generateFlowTuple(Random random, Switch source, Switch destination, Long srcIP, Long dstIP,
                                   RangeDimensionRange dstPortRange, RangeDimensionRange protocolRange,
                                   RangeDimensionRange srcPortRange) {
        final long protocol = protocolRange.getRandomNumber(random);
        Long[] properties = new Long[template.length];
        properties[srcIPIndex] = srcIP;
        properties[dstIPIndex] = dstIP;
        properties[srcPortIndex] = srcPortRange.getRandomNumber(random);
        properties[dstPortIndex] = dstPortRange.getRandomNumber(random);
        properties[protocolIndex] = protocol;
        return new Flow(0, source, destination, properties);
    }

    private class GenerateFlowActionOnLeafSrc implements PersistentPartitionTree2.SmallPActionOnLeaf<Persistanter.BlockEntry> {
        private Set<Integer> seenRules;
        private Set<Flow> microFlows;
        private int numberOfFlowsInThisCategory;
        private Switch source;
        private Long srcIP;
        private Random random;
        private Collection<Long> destinationIPs;
        Map<Long, Switch> reverseIPMap;

        public void init(Set<Integer> seenRules, Set<Flow> microFlows,
                         int numberOfFlowsInThisCategory, Switch source,
                         Long srcIP, Random random, Collection<Long> destinationIPs, Map<Long, Switch> reverseIPMap) {
            this.seenRules = seenRules;
            this.microFlows = microFlows;
            this.numberOfFlowsInThisCategory = numberOfFlowsInThisCategory;
            this.source = source;
            this.srcIP = srcIP;
            this.random = random;
            this.destinationIPs = destinationIPs;
            this.reverseIPMap = reverseIPMap;
        }


        public boolean doAction(Persistanter.BlockEntry block) {
            final Integer rule = block.getRuleID();
            if (!seenRules.contains(rule)) {
                for (Long destinationIP : destinationIPs) {
                    if (block.getRanges()[0].match(destinationIP)) {
                        //rules2.add(rule);

                        seenRules.add(rule);
                        //System.out.println(rule + ":" + srcIP + dstIP);
                        int i = 0;
                        while (i < maxFlowPerTuple) {
                            final Flow f = generateFlowTuple(random, source, reverseIPMap.get(destinationIP), srcIP,
                                    destinationIP, block.getRanges()[2], block.getRanges()[1], Util.SRC_PORT_INFO.getDimensionRange());
                            if (microFlows.size() >= numberOfFlowsInThisCategory) {
                                return false;
                            }
                            microFlows.add(f);
                            i++;
                        }
                    }
                }
            }
            return microFlows.size() < numberOfFlowsInThisCategory;
        }

        public void emptyBuffer() {

        }
    }

    protected Flow generateFlows(Switch src, Switch dst, Persistanter.BlockEntry block, long srcIP, long dstIP, Random random) {

        final long srcPort = Util.SRC_PORT_INFO.getDimensionRange().getRandomNumber(random);
        final long dstPort = block.getRanges()[0].getRandomNumber(random);
        final long protocol = block.getRanges()[1].getRandomNumber(random);
        Long[] properties = new Long[template.length];
        properties[srcIPIndex] = srcIP;
        properties[dstIPIndex] = dstIP;
        properties[srcPortIndex] = srcPort;
        properties[dstPortIndex] = dstPort;
        properties[protocolIndex] = protocol;
        return new Flow(0, src, dst, properties);
    }

    private class CollectIPPairs implements PersistentPartitionTree2.SmallPActionOnLeaf<Persistanter.BlockEntry> {
        int good;
        private final Random random;
        private final Map<Integer, Set<IPTuple>> ruleTuplesMap;
        private final Set<Long> ips;
        private final Map<RangeDimensionRange, Long> selectedForSrcIP;
        private final Map<RangeDimensionRange, Long> selectedForDstIP;
        private final boolean doRandom;


        public CollectIPPairs(Random random, Map<Integer, Set<IPTuple>> ruleTuplesMap, Set<Long> ips, boolean doRandom) {
            this.random = random;
            this.ruleTuplesMap = ruleTuplesMap;
            this.ips = ips;
            good = 0;
            selectedForSrcIP = new HashMap<RangeDimensionRange, Long>();
            selectedForDstIP = new HashMap<RangeDimensionRange, Long>();
            this.doRandom = doRandom;
        }

        private Long getSrcInThisRange(RangeDimensionRange range) {
            for (Map.Entry<RangeDimensionRange, Long> entry : selectedForSrcIP.entrySet()) {
                if (range.match(entry.getValue())) {
                    return entry.getValue();
                }
            }
            return null;
        }

        public boolean doAction(Persistanter.BlockEntry node) {
            good++;

            final RangeDimensionRange srcIPRange = node.getRanges()[0];
            Long selectedSrcIP = selectedForSrcIP.get(srcIPRange);
            if (selectedSrcIP == null) {
                if (doRandom) {
                    selectedSrcIP = srcIPRange.getRandomNumber(random);
                } else {
                    selectedSrcIP = srcIPRange.getStart();
                }
                selectedForSrcIP.put(srcIPRange, selectedSrcIP);
            }
            final RangeDimensionRange dstIPRange = node.getRanges()[1];
            Long selectedDstIP = selectedForDstIP.get(dstIPRange);
            if (selectedDstIP == null) {
                selectedDstIP = getSrcInThisRange(dstIPRange);
                if (selectedDstIP == null) {
                    if (doRandom) {
                        selectedDstIP = dstIPRange.getRandomNumber(random);
                    } else {
                        selectedDstIP = dstIPRange.getStart();
                    }
                }
                selectedForDstIP.put(dstIPRange, selectedDstIP);
            }
            Set<IPTuple> ipTuples = ruleTuplesMap.get(node.getRuleID());
            if (ipTuples == null) {
                ipTuples = new HashSet<IPTuple>();
                ruleTuplesMap.put(node.getRuleID(), ipTuples);
            }
            final IPTuple t = new IPTuple(selectedSrcIP, selectedDstIP, node.getRuleID(), node.getId());
            t.setRange(0, node.getRanges()[2]);
            t.setRange(1, node.getRanges()[3]);
            ipTuples.add(t);
            ips.add(selectedSrcIP);
            ips.add(selectedDstIP);
            return true;
        }

        public void emptyBuffer() {

        }
    }

    static class IPTuple {
        long src;
        long dst;
        int ruleId;
        int blockId;
        RangeDimensionRange[] ranges = new RangeDimensionRange[2];

        public RangeDimensionRange getDstPort() {
            return ranges[1];
        }

        public RangeDimensionRange getProtocol() {
            return ranges[0];
        }

        public void setRange(int index, RangeDimensionRange range) {
            ranges[index] = range;
        }

        public IPTuple(long src, long dst, int ruleId, int blockId) {
            this.src = src;
            this.dst = dst;
            this.ruleId = ruleId;
            this.blockId = blockId;
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

    private static class GatherAndSelectBlockNumbers implements
            PersistentPartitionTree2.SmallPActionOnLeaf<Persistanter.BlockEntry> {
        private int lastRuleSeenId = -1;
        private int[] buffer;
        private Random random;
        private int index = 0;
        List<Integer> selectedBlocks;

        private GatherAndSelectBlockNumbers(int bufferSize, Random random, int selectionSize) {
            buffer = new int[bufferSize];
            this.random = random;
            selectedBlocks = new ArrayList<Integer>(selectionSize);
        }

        private void init() {
            index = 0;
            selectedBlocks.clear();
            lastRuleSeenId = -1;
        }

        public boolean doAction(Persistanter.BlockEntry node) {
            final int ruleId = node.getRuleID();
            if (ruleId != lastRuleSeenId && lastRuleSeenId != -1) {
                if (index == 1) {
                    selectedBlocks.add(buffer[0]);
                } else {
                    selectedBlocks.add(buffer[random.nextInt(index)]);
                }
                index = 0;
            }
            buffer[index % buffer.length] = node.getId();
            index = (index + 1) % buffer.length;//ignore if a rule matched blocks is more than buffer length
            if (index == 0) {
                System.out.println("buffer reset for " + ruleId);
            }
            lastRuleSeenId = ruleId;
            return true;
        }

        public void emptyBuffer() {
            index = 0;
        }

        public void finish() {
            if (index > 0) {
                selectedBlocks.add(buffer[random.nextInt(index)]);
            }
        }
    }
}
