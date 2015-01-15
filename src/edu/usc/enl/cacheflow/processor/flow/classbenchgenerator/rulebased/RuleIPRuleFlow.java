package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.rulebased;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.classifier.TwoLevelTrafficProcessorOneByOne;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.DestinationSelector;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.IPAssigner;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/26/12
 * Time: 10:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class RuleIPRuleFlow extends RuleBasedFlowGenerator {

    public final Long[] template2 = new Long[]{0l, 0l, 0l, 0l, 0l};
    public OVSClassifier classifier;
    private TwoLevelTrafficProcessorOneByOne realClassifier;
    private final  int threadsNum;

    public RuleIPRuleFlow(TwoLevelTrafficProcessorOneByOne realClassifier,
                          IPAssigner ipAssigner, int threadsNum) {
        super(ipAssigner);
        this.realClassifier = realClassifier;
        this.threadsNum=threadsNum;
    }

    public void generate(final List<Rule> rules, final Random random, final List<Switch> sources,
                         final DestinationSelector destinationSelector, final CustomRandomFlowDistribution flowDistribution,
                         final PrintWriter writer, Topology topology) {
        fillIndexes();
        /////////////////// RANDOM IP RULE BASED FLOW
        //generateFlows(rules, random, sources, destinationSelector, flowDistribution, blockBasedIPGenerate2(random, sources), writer);
        /////////////////// RULE IP RULE BASED FLOW

        Set<Long> ips = new HashSet<Long>();
        /*Map<Long, List<Long>> srcDst = new HashMap<Long, List<Long>>();
        Map<Long, Integer> dstIps = new HashMap<Long, Integer>();
        Map<Long, Integer> srcIps = new HashMap<Long, Integer>();*/
        Map<Switch, Integer> vmsPerSourceMap = new HashMap<>();
        int vmsNum = flowDistribution.getVMsPerSource(random, sources, vmsPerSourceMap);
        findCorners(rules, ips, random, vmsNum);

        final Map<Switch, List<Long>> switchTotalIPss = ipAssigner.assignIPs(random, sources, ips, topology, vmsPerSourceMap);
        //blockBasedIPGenerate(random, sources, ips, srcIps, dstIps, srcDst);
        final Map<Long, Switch> reverseIPMap = new HashMap<Long, Switch>();
        for (Map.Entry<Switch, List<Long>> entry : switchTotalIPss.entrySet()) {
            for (Long ip : entry.getValue()) {
                reverseIPMap.put(ip, entry.getKey());
            }
        }
        Runtime.getRuntime().gc();
        new Flow(0, sources.get(0), sources.get(0), template2).headerToString(writer);

        classifier = new OVSClassifier(rules);

//         generateFlows(random, sources, destinationSelector, flowDistribution, switchTotalIPss, writer,
//      new HashSet<Rule>(), reverseIPMap);

        final int perThreadNum = (int) (Math.ceil(sources.size() / threadsNum));
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < threadsNum; i++) {
            final int finalI = i;
            final Thread thread = new Thread() {
                @Override
                public void run() {
                    generateFlows(random, sources.subList(finalI * perThreadNum, Math.min(sources.size(), (finalI + 1) * perThreadNum))
                            , destinationSelector.createNew(), flowDistribution, switchTotalIPss, writer,
                            Collections.newSetFromMap(new ConcurrentHashMap<Rule, Boolean>()), reverseIPMap);
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

        writer.close();
    }

    private void generateFlows(Random random, List<Switch> sources, DestinationSelector destinationSelector,
                               CustomRandomFlowDistribution flowDistribution,
                               Map<Switch, List<Long>> switchTotalIPss,
                               final PrintWriter writer, Set<Rule> allSeenRules,
                               Map<Long, Switch> reverseIPMap) {

        Map<Long, Map<Long, Collection<Rule>>> srcDstRule = new HashMap<Long, Map<Long, Collection<Rule>>>();
        long[] template = new long[]{0, 0};
        long[] maskTemp = new long[]{0, 0};
        List<Rule> matchedRuleBuffer = new ArrayList<Rule>(100);
        for (Switch source : sources) {
            System.out.println(source);
            //get the number of flows
            int flowsPerSource = flowDistribution.getRandomFlowNum(random.nextDouble());
            destinationSelector.setSource(source, switchTotalIPss.keySet());
            final List<Long> srcIPs = switchTotalIPss.get(source);
            Set<Flow> microFlows = new HashSet<Flow>(1000);
            final Map<? extends Object, Double> categoryProb = destinationSelector.getCategoryProb();
            for (Object category : categoryProb.keySet()) {
                System.out.println("Category " + category);
                int numberOfFlowsInThisCategory = (int) (Math.round(flowsPerSource * categoryProb.get(category)));
                final List<Switch> switchInCategory = destinationSelector.getSwitchInCategory(category);
                if (realClassifier == null) {
                    noClassifierGenerate(random, switchTotalIPss, allSeenRules, template, maskTemp, matchedRuleBuffer,
                            source, srcIPs, microFlows, numberOfFlowsInThisCategory, switchInCategory);
                } else {
                    classifierGenerateFlow(random, switchTotalIPss, allSeenRules, reverseIPMap, srcDstRule, template,
                            maskTemp, matchedRuleBuffer, source, srcIPs, microFlows, numberOfFlowsInThisCategory,
                            switchInCategory);
                }
                synchronized (writer) {
                    writeToWriter(random, writer, flowDistribution, microFlows);
                }
                Runtime.getRuntime().gc();
            }
        }
    }

    private void classifierGenerateFlow(Random random, Map<Switch, List<Long>> switchTotalIPss, Set<Rule> allSeenRules, Map<Long, Switch> reverseIPMap, Map<Long, Map<Long, Collection<Rule>>> srcDstRule, long[] template, long[] maskTemp, List<Rule> matchedRuleBuffer, Switch source, List<Long> srcIPs, Set<Flow> microFlows, int numberOfFlowsInThisCategory, List<Switch> switchInCategory) {
        int choices = 1;
        int l = 0;

        srcDstRule.clear();
        Collections.shuffle(srcIPs, random);

        for (Long srcIP : srcIPs) {
            template[0] = srcIP;
            Collections.shuffle(switchInCategory, random);
            Map<Long, Collection<Rule>> destRules = new HashMap<Long, Collection<Rule>>();
            for (Switch destination : switchInCategory) {
                final List<Long> destinationIPs = switchTotalIPss.get(destination);
                Collections.shuffle(destinationIPs, random);
                for (Long dstIP : destinationIPs) {
                    template[1] = dstIP;
                    matchedRuleBuffer.clear();
                    classifier.getRules(template, maskTemp, matchedRuleBuffer);
                    matchedRuleBuffer.removeAll(allSeenRules);
                    if (matchedRuleBuffer.size() > 0) {
                        List<Rule> unseenMatchRules = new LinkedList<Rule>();
                        for (Rule rule : matchedRuleBuffer) {
                            final List<Flow> flows1 = generateFlows(source, destination, rule, srcIP, dstIP, random, choices);
                            for (Flow flow1 : flows1) {
                                final Rule realRule = realClassifier == null ? rule : realClassifier.classify(flow1);
                                if (!allSeenRules.contains(realRule)) {
                                    allSeenRules.add(realRule);
                                    microFlows.add(flow1);
                                    if (microFlows.size() >= numberOfFlowsInThisCategory) {
                                        return;
                                    }
                                }
                                if (realRule != rule && !allSeenRules.contains(rule)) {
                                    unseenMatchRules.add(rule);
                                }
                            }
                        }
                        if (unseenMatchRules.size() > 0) {
                            destRules.put(dstIP, unseenMatchRules);
                        }
                    }
                }
            }
            if (destRules.size() > 0) {
                srcDstRule.put(srcIP, destRules);
            }
            Runtime.getRuntime().gc();
        }
        while (microFlows.size() < numberOfFlowsInThisCategory) {
            //l=100;
            l++;
            choices++;
            if (srcDstRule.size() == 0 || l > 3) {
                //fill with random
                System.out.println(Thread.currentThread() + " " + l + ": " + microFlows.size());
                generateRandomFlows(random, switchTotalIPss, microFlows, source, numberOfFlowsInThisCategory,
                        switchInCategory);
            } else {
                final List<Long> srcIps2 = new ArrayList<Long>(srcDstRule.keySet());
                Collections.shuffle(srcIps2, random);
                for (Long srcIP : srcIps2) {
                    List<Long> toRemove = new LinkedList<Long>();
                    final Map<Long, Collection<Rule>> dstRules = srcDstRule.get(srcIP);
                    for (Map.Entry<Long, Collection<Rule>> entry : dstRules.entrySet()) {
                        final Collection<Rule> rules = entry.getValue();
                        rules.removeAll(allSeenRules);
                        if (rules.size() == 0) {
                            toRemove.add(entry.getKey());
                        } else {
                            for (Rule rule : rules) {
                                //allSeenRules.add(rule);
                                final List<Flow> flows1 = generateFlows(source, reverseIPMap.get(entry.getKey()),
                                        rule, srcIP, entry.getKey(), random, choices);
                                for (Flow flow1 : flows1) {
                                    final Rule realRule = realClassifier.classify(flow1);
                                    if (!allSeenRules.contains(realRule)) {
                                        allSeenRules.add(realRule);
                                        microFlows.add(flow1);
                                        if (microFlows.size() >= numberOfFlowsInThisCategory) {
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    for (Long dstIP : toRemove) {
                        dstRules.remove(dstIP);
                    }
                    if (dstRules.size() == 0) {
                        srcDstRule.remove(srcIP);
                    }
                }
            }
        }
    }

    private void noClassifierGenerate(Random random, Map<Switch, List<Long>> switchTotalIPss,
                                      Set<Rule> allSeenRules, long[] template, long[] maskTemp,
                                      List<Rule> matchedRuleBuffer, Switch source, List<Long> srcIPs,
                                      Set<Flow> microFlows, int numberOfFlowsInThisCategory, List<Switch> switchInCategory) {
        for (Long srcIP : srcIPs) {
            template[0] = srcIP;
            for (Switch destination : switchInCategory) {
                final List<Long> destinationIPs = switchTotalIPss.get(destination);
                Collections.shuffle(destinationIPs, random);
                for (Long dstIP : destinationIPs) {
                    template[1] = dstIP;
                    matchedRuleBuffer.clear();
                    classifier.getRules(template, maskTemp, matchedRuleBuffer);
                    matchedRuleBuffer.removeAll(allSeenRules);
                    if (matchedRuleBuffer.size() > 0) {
                        for (Rule rule : matchedRuleBuffer) {
                            final List<Flow> flows1 = generateFlows(source, destination, rule, srcIP, dstIP, random, 1);
                            for (Flow flow1 : flows1) {
                                if (!allSeenRules.contains(rule)) {
                                    allSeenRules.add(rule);
                                    microFlows.add(flow1);
                                    if (microFlows.size() >= numberOfFlowsInThisCategory) {
                                        //continue full;
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        generateRandomFlows(random, switchTotalIPss, microFlows, source, numberOfFlowsInThisCategory, switchInCategory);
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

    private Flow generateFlowTuple(Random random, Switch source, Switch destination, Long srcIP, Long dstIP,
                                   RangeDimensionRange dstPortRange, RangeDimensionRange protocolRange,
                                   RangeDimensionRange srcPortRange) {
        final long protocol = protocolRange.getRandomNumber(random);
        Long[] properties = new Long[template2.length];
        properties[srcIPIndex] = srcIP;
        properties[dstIPIndex] = dstIP;
        properties[srcPortIndex] = srcPortRange.getRandomNumber(random);
        properties[dstPortIndex] = dstPortRange.getRandomNumber(random);
        properties[protocolIndex] = protocol;
        return new Flow(0, source, destination, properties);
    }


    private void findCorners(List<Rule> rules, Set<Long> ips, Random random, int minNum) {
        TreeSet<Long> selectedIps = new TreeSet<Long>();
        while (ips.size() < minNum) {
            Collections.shuffle(rules, random);
            for (Rule rule : rules) {
                final RangeDimensionRange srcRange = rule.getProperty(srcIPIndex);
                long src = getIP(random, selectedIps, srcRange);
                final RangeDimensionRange dstRange = rule.getProperty(dstIPIndex);
                final long dst = getIP(random, selectedIps, dstRange);
                ips.add(src);
                ips.add(dst);
                if (ips.size() >= minNum) {
                    break;
                }
            }
            selectedIps.clear();
        }

    }

    private long getIP(Random random, TreeSet<Long> selectedIps, RangeDimensionRange srcRange) {
        final Long ceilingIP = selectedIps.ceiling(srcRange.getStart());
        if (ceilingIP != null && ceilingIP <= srcRange.getEnd()) {
            return ceilingIP;
        }
        final long randomNumber = srcRange.getRandomNumber(random);
        selectedIps.add(randomNumber);
        return randomNumber;
    }


    private class OVSClassifier {
        private Map<IntTuple, Bucket> buckets = new HashMap<IntTuple, Bucket>();//how many wc for src and dst to buckets

        protected IntTuple getRuleWC(Rule rule) throws UnalignedRangeException {
            final RangeDimensionRange srcIPRange = rule.getProperties().get(srcIPIndex);
            final RangeDimensionRange dstIPRange = rule.getProperties().get(dstIPIndex);
            return new IntTuple(srcIPRange.getNumberOfWildcardBits(), dstIPRange.getNumberOfWildcardBits());
        }

        public void getRules(long[] template, long[] maskBuffer, List<Rule> output) {
            for (Map.Entry<IntTuple, Bucket> entry : buckets.entrySet()) {
                maskBuffer[0] = template[0];
                maskBuffer[1] = template[1];
                mask(entry.getKey(), maskBuffer);
                entry.getValue().getRule(maskBuffer, output);
            }
            if (output.size() == 0) {
                throw new RuntimeException("No match for flow " + template);
            }
        }

        public OVSClassifier(List<Rule> rules) {
            try {
                for (Rule rule : rules) {
                    final IntTuple wc = getRuleWC(rule);
                    Bucket bucket = buckets.get(wc);
                    if (bucket == null) {
                        bucket = new Bucket();
                        buckets.put(wc, bucket);
                    }
                    bucket.addRule(rule);
                }
            } catch (UnalignedRangeException e) {
                e.printStackTrace();
            }

        }

        private void mask(IntTuple tuple, long[] output) {
            output[0] = output[0] >> tuple.srcIP << tuple.srcIP;
            output[1] = output[1] >> tuple.dstIP << tuple.dstIP;
        }

        private class Bucket {
            private Map<Integer, Object> rules = new HashMap<Integer, Object>();

            private Rule match(Flow flow) {
                for (Map.Entry<Integer, Object> entry : rules.entrySet()) {
                    if (entry.getValue() instanceof Rule) {
                        if (((Rule) entry.getValue()).match(flow)) {
                            return (Rule) entry.getValue();
                        }
                    } else {
                        final List<Rule> list = (List<Rule>) entry.getValue();
                        for (Rule rule : list) {
                            if (rule.match(flow)) {
                                return rule;
                            }
                        }
                    }

                }
                return null;
            }

            private boolean getRule(long[] properties, List<Rule> output) {

                final Object o = rules.get(hash(properties));
                if (o == null) {
                    return false;
                } else if (o instanceof Rule) {
                    if (match(properties, (Rule) o)) {
                        output.add((Rule) o);
                        return true;
                    } else {
                        return false;
                    }
                }
                //its a list
                boolean changed = false;
                for (Rule rule : (List<Rule>) o) {
                    if (match(properties, rule)) {
                        output.add(rule);
                        changed = true;
                    }
                }
                return changed;
            }

            private boolean match(long[] properties, Rule rule) {
                return rule.getProperty(srcIPIndex).match(properties[0]) &&
                        rule.getProperty(dstIPIndex).match(properties[1]);
            }

            private int hash(long[] properties) {
                int output = 1;
                output = 31 * output + (int) (properties[0] ^ (properties[0] >>> 32));
                output = 31 * output + (int) (properties[1] ^ (properties[1] >>> 32));
                return output;
            }

            private int ruleHashCode(Rule rule) {
                int output = 1;
                List<RangeDimensionRange> properties = rule.getProperties();
                {
                    RangeDimensionRange range = properties.get(srcIPIndex);
                    final long start = range.getStart();
                    output = 31 * output + (int) (start ^ (start >>> 32));
                }
                {
                    RangeDimensionRange range = properties.get(dstIPIndex);
                    final long start = range.getStart();
                    output = 31 * output + (int) (start ^ (start >>> 32));
                }
                return output;
            }

            private void addRule(Rule rule) {
                final int ruleStartHashCode = ruleHashCode(rule);
                final Object oldRule = rules.get(ruleStartHashCode);
                if (oldRule != null) {
                    if (oldRule instanceof Rule) {
                        final Rule oldRule1 = (Rule) oldRule;
                        if (!oldRule1.equalProperties(rule) || !oldRule1.getAction().equals(rule.getAction())) {
                            List<Rule> collisionList = new ArrayList<Rule>();
                            collisionList.add(rule);
                            collisionList.add(oldRule1);
                            rules.put(ruleStartHashCode, collisionList);
                        } else {//same rule
                            //Duplicate
                            if (rule.getPriority() < ((Rule) oldRule).getPriority()) {
                                rules.put(ruleStartHashCode, rule);
                            }
                        }
                    } else {
                        //there is a collision list
                        List<Rule> collisionList = (List<Rule>) oldRule;
                        boolean found = false;
                        for (int i = 0, collisionListSize = collisionList.size(); i < collisionListSize; i++) {
                            Rule rule1 = collisionList.get(i);
                            if (rule1.equalProperties(rule) && rule1.getAction().equals(rule.getAction())) {//same rule
                                if (rule.getPriority() < rule1.getPriority()) {
                                    collisionList.set(i, rule);
                                    found = true;
                                    System.out.println(rule + " removed because of " + rule1);
                                    break;
                                }
                            }
                        }
                        if (!found) {//new collision
                            collisionList.add(rule);
                        }
                    }
                } else {
                    rules.put(ruleStartHashCode, rule);
                }
            }
        }

        private class IntTuple {
            Integer srcIP;
            Integer dstIP;

            private IntTuple(Integer srcIP, Integer dstIP) {
                this.srcIP = srcIP;
                this.dstIP = dstIP;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                IntTuple intTuple = (IntTuple) o;

                if (dstIP != null ? !dstIP.equals(intTuple.dstIP) : intTuple.dstIP != null) return false;
                if (srcIP != null ? !srcIP.equals(intTuple.srcIP) : intTuple.srcIP != null) return false;

                return true;
            }

            @Override
            public int hashCode() {
                int result = srcIP != null ? srcIP.hashCode() : 0;
                result = 31 * result + (dstIP != null ? dstIP.hashCode() : 0);
                return result;
            }
        }
    }

}

