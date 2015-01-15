package edu.usc.enl.cacheflow.processor.partition.transform;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 10/30/12
 * Time: 7:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class ExtendRulesPartitionTransformer extends PartitionTransformer {
    private final boolean slowExtend;
    private final boolean changeStepsOrSimilarity;
    private final boolean keepMax;

    public ExtendRulesPartitionTransformer(boolean slowExtend, boolean keepMax, boolean changeStepsOrSimilarity) {
        this.slowExtend = slowExtend;
        this.changeStepsOrSimilarity = changeStepsOrSimilarity;
        this.keepMax = keepMax;
    }

    @Override
    public void transform(Random random, int changesNum, List<Partition> partitions, Map<Switch, Collection<Partition>> sourcePartitions,
                          Topology topology) throws NoAssignmentFoundException, IncompleteTransformException {
        final int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        TreeMap<Long, Partition> maxSourcePartitionMap;

        {
            Assignment assignment = Assignment.getAssignment(sourcePartitions);
            Map<Switch, Double> vmStartAssignmentLoad = getVMStartAssignmentLoad(partitions, sourcePartitions, topology, assignment);
            //find maximum
            Switch maximumSwitch = getMaxSwitch(vmStartAssignmentLoad);
            maxSourcePartitionMap = getSrcIPPartitionMap(sourcePartitions.get(maximumSwitch), srcIPIndex);
        }

        TreeMap<Long, Partition> srcIPPartitionMap = getSrcIPPartitionMap(partitions, srcIPIndex);

        //get unique set of rules and the max priority rule
        Set<Rule> removedRules = new HashSet<>(); //just as a set here
        int maxPriority = getRules(partitions, removedRules);//should contain max rule

        List<Rule> toExtendRules = new ArrayList<>(removedRules);
        removedRules.clear();


        // the map to keep track of partitions affected if we extend. Need to be treemap as we need submap function
        TreeMap<Long, Partition> partitionsInExtended = new TreeMap<>();

        int changesUntilNow = 0;
        double similarityChange = 0;
        //for each change num
        Set<Rule> toBeRemovedRules = new HashSet<>();
        while ((changeStepsOrSimilarity && changesUntilNow < changesNum) || (!changeStepsOrSimilarity && similarityChange < changesNum)) {
            if (toExtendRules.size() == 0) {
                throw new IncompleteTransformException("No rule to extend. " + (changeStepsOrSimilarity ? changesUntilNow : similarityChange) + " changes done");
            }
            //select a random rule in all partitions
            Rule toExtendRule = getARuleToExtend(random, toExtendRules, changesUntilNow);
            if (removedRules.contains(toExtendRule)) {//the rule has removed before
                continue;
            }

            // find the increased src_ip
            RangeDimensionRange srcIP = toExtendRule.getProperty(srcIPIndex);
            if (srcIP.getSize() >= srcIP.getInfo().getDimensionRange().getSize()) {
                continue;
                //throw new IncompleteTransformException("All rules remained are wildcard. " + changesUntilNow + " changes done");
            }

            //find how many max partitions are covered by this rule
            boolean canAddMaxPartition = !keepMax || canCoverMaxPartitions(maxSourcePartitionMap, srcIP);

            //increase its size as much as possible if fastextend
            boolean isExtensible = true;
            boolean usefulExtension = false;
            while (isExtensible && srcIP.getSize() < srcIP.getInfo().getDimensionRange().getSize()) { // while this rule is extensible

                //double the size
                RangeDimensionRange ExtendedSrcIP = getAddedSrcIP(srcIP);

                //check if it does not add similarity to max switch partitions

                NavigableMap<Long, Partition> coveredMaxPartitions = null;
                if (keepMax) {
                    coveredMaxPartitions = maxSourcePartitionMap.subMap(ExtendedSrcIP.getStart(), true, ExtendedSrcIP.getEnd(), true);
                    if (coveredMaxPartitions.size() > 0) {
                        if (canAddMaxPartition) {
                            canAddMaxPartition = false; //when I cover, the next steps cannot
                        } else {
                            isExtensible = false;
                            continue;
                        }
                    }
                }

                // find partitions in the added range
                partitionsInExtended.clear();
                partitionsInExtended.putAll(srcIPPartitionMap.subMap(ExtendedSrcIP.getStart(), true, ExtendedSrcIP.getEnd(), true));
                usefulExtension = partitionsInExtended.size() > 0; //at least one partition is affected by it
                toBeRemovedRules.clear();
                long toRemoveRegionSize = ExtendedSrcIP.getSize();
                //what is possible?
                //we want to double the size of the rule but keep avg and max fixed
                isExtensible = coverPartitionsInExtendedIP(srcIPIndex, maxSourcePartitionMap, maxPriority,
                        partitionsInExtended, toBeRemovedRules, toExtendRule, isExtensible,
                        coveredMaxPartitions, toRemoveRegionSize);

                if (isExtensible) {
                    //do the extension plan. This map should have been emptied by now

                    if (changeStepsOrSimilarity) {
                        partitionsInExtended.clear();
                        partitionsInExtended.putAll(srcIPPartitionMap.subMap(ExtendedSrcIP.getStart(), true, ExtendedSrcIP.getEnd(), true));
                        for (Rule toBeRemovedRule : toBeRemovedRules) {
                            for (Partition partition1 : partitionsInExtended.values()) {
                                partition1.getRules().remove(toBeRemovedRule);
                                partition1.getRules().add(toExtendRule);
                            }
                        }
                    } else {
                        int currentSize = srcIPPartitionMap.subMap(srcIP.getStart(), true, srcIP.getEnd(), true).size();
                        for (Rule toBeRemovedRule : toBeRemovedRules) {
                            NavigableMap<Long, Partition> removedRuleFrom = srcIPPartitionMap.subMap(
                                    toBeRemovedRule.getProperty(srcIPIndex).getStart(), true, toBeRemovedRule.getProperty(srcIPIndex).getEnd(), true);
                            for (Partition partition1 : removedRuleFrom.values()) {
                                partition1.getRules().remove(toBeRemovedRule);
                                partition1.getRules().add(toExtendRule);
                            }
                            similarityChange += 2.0 * currentSize * removedRuleFrom.size() / (partitions.size() * (partitions.size() - 1));
                            currentSize += removedRuleFrom.size();
                        }
                    }
                    //this must be later than previous section
                    srcIP.setStart(Math.min(srcIP.getStart(), ExtendedSrcIP.getStart()));
                    srcIP.setEnd(Math.max(srcIP.getEnd(), ExtendedSrcIP.getEnd()));
                    toExtendRule.updateProperties();
                    removedRules.addAll(toBeRemovedRules);
                    if (slowExtend && usefulExtension) {//if its extension is not useful continue because of performance
                        //one extension is enough
                        toExtendRules.add(toExtendRule);
                        break;
                    }
                }
            }
            if (usefulExtension) {
                changesUntilNow++;
            }
        }

    }

    private boolean coverPartitionsInExtendedIP(int srcIPIndex, TreeMap<Long, Partition> maxSourcePartitionMap,
                                                int maxPriority, TreeMap<Long, Partition> tempMap, Set<Rule> toBeRemovedRules,
                                                Rule toExtendRule, boolean extensible, NavigableMap<Long, Partition> coveredMaxPartitions, long toRemoveRegionSize) {
        while (tempMap.size() > 0) {//while there is a partition in the new area that one of its rule has not been planned to be removed
            //start from the first one and select the largest rule
            Map.Entry<Long, Partition> firstEntry = tempMap.firstEntry();

            //start from the largest rule that is confined in this region
            //it should be confined otherwise will cover current partition (note PREFIX WILDCARD PATTERNS)
            Rule safeToRemoveRule = getLargestConfinedRule(srcIPIndex, maxSourcePartitionMap, maxPriority, toBeRemovedRules,
                    toExtendRule, coveredMaxPartitions, toRemoveRegionSize, firstEntry.getValue());
            if (safeToRemoveRule == null) {
                //extending this rule is not possible
                extensible = false;
                break;
            }
            toBeRemovedRules.add(safeToRemoveRule);
            //no need to consider these partitions
            RangeDimensionRange foundRuleSrcIP = safeToRemoveRule.getProperty(srcIPIndex);
            tempMap.subMap(foundRuleSrcIP.getStart(), true, foundRuleSrcIP.getEnd(), true).clear();
            toRemoveRegionSize -= foundRuleSrcIP.getSize();
        }
        return extensible;
    }

    private Rule getLargestConfinedRule(int srcIPIndex, TreeMap<Long, Partition> maxSourcePartitionMap, int maxPriority,
                                        Set<Rule> toBeRemovedRules, Rule toExtendRule, NavigableMap<Long, Partition> coveredMaxPartitions,
                                        long toRemoveRegionSize, Partition partition) {
        Rule safeToRemoveRule = null;
        long maxRuleSize = -1;
        Collection<Rule> rules = partition.getRules();
        for (Rule toRemoveRuleCandidate : rules) {
            if (toRemoveRuleCandidate.getPriority() == maxPriority && toExtendRule.getPriority() < maxPriority) {
                continue;// this cannot replace the default rule
            }

            long size = toRemoveRuleCandidate.getProperty(srcIPIndex).getSize();
            if (size <= toRemoveRegionSize && //is confined here
                    maxRuleSize < size && //check if it is max
                    !toBeRemovedRules.contains(toRemoveRuleCandidate)//is not planned to be removed before
                    && (!keepMax ||(coveredMaxPartitions.size() < 2 || //has 0 or 1 max switch partitions or (if it is more than two one rule must cover all of them)
                    maxSourcePartitionMap.subMap(toRemoveRuleCandidate.getProperty(srcIPIndex).getStart(), true, toRemoveRuleCandidate.getProperty(srcIPIndex).getEnd(), true).size() == coveredMaxPartitions.size())))// all those max switch partitions are for this rule
            {
                safeToRemoveRule = toRemoveRuleCandidate;
                maxRuleSize = size;
                if (size == toRemoveRegionSize) {
                    break; //cannot find better than this
                }
            }
        }
        return safeToRemoveRule;
    }

    private Rule getARuleToExtend(Random random, List<Rule> toExtendRules, int changesUntilNow) throws IncompleteTransformException {
        if (toExtendRules.size() == 0) {
            throw new IncompleteTransformException("No more rule to extend. " + changesUntilNow + " changes done");
        }
        return toExtendRules.remove(random.nextInt(toExtendRules.size()));
        //return toExtendRules.remove(toExtendRules.size() - 1);
    }

    private int getRules(List<Partition> partitions, Set<Rule> tempSet) {
        //remove default rule as it should not be replaced by extend of another rule as it may make not handled area in the flowspace

        int maxPriority = -1;
        for (Partition partition : partitions) {
            for (Rule rule : partition.getRules()) {
                tempSet.add(rule);
                maxPriority = Math.max(maxPriority, rule.getPriority());
            }
        }
        return maxPriority;
    }

    private boolean canCoverMaxPartitions(TreeMap<Long, Partition> maxSourcePartitionMap, RangeDimensionRange srcIP) {
        NavigableMap<Long, Partition> coveredMaxPartitions = maxSourcePartitionMap.subMap(srcIP.getStart(), true, srcIP.getEnd(), true);
        boolean canAddMaxPartition = false;
        if (coveredMaxPartitions.size() == 0) {
            canAddMaxPartition = true;
        }
        return canAddMaxPartition;
    }

    private RangeDimensionRange getAddedSrcIP(RangeDimensionRange srcIP) {
        long start = srcIP.getStart();
        long size = srcIP.getSize();

        RangeDimensionRange newSrcIP = new RangeDimensionRange(start, srcIP.getEnd() + size, srcIP.getInfo());
        try {
            newSrcIP.getNumberOfWildcardBits();
            newSrcIP.setStart(srcIP.getEnd() + 1);
        } catch (UnalignedRangeException e) {
            //shift it
            newSrcIP.setStart(start - size);
            newSrcIP.setEnd(start - 1);
        }
        return newSrcIP;
    }
}
