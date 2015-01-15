package edu.usc.enl.cacheflow.processor.partition.transform;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.scripts.vcrib.transform.MultipleTransformFeasibilityScript;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 10/30/12
 * Time: 1:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class BreakPartitionTransformer extends PartitionTransformer {
    private final boolean keepMax;
    private final boolean changeStepsOrSimilarity;

    public BreakPartitionTransformer(boolean keepMax, boolean changeStepsOrSimilarity) {
        this.keepMax = keepMax;
        this.changeStepsOrSimilarity = changeStepsOrSimilarity;
    }

    @Override
    public void transform(Random random, int changesNum, List<Partition> partitions, Map<Switch, Collection<Partition>> sourcePartitions,
                          Topology topology) throws NoAssignmentFoundException, IncompleteTransformException {
        final int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        TreeMap<Long, Partition> srcIPPartitionMap = getSrcIPPartitionMap(partitions, srcIPIndex);
        Assignment assignment = Assignment.getAssignment(sourcePartitions);
        Map<Switch, Double> vmStartAssignmentLoad = getVMStartAssignmentLoad(partitions, sourcePartitions, topology, assignment);
        //find maximum
        Switch maximumSwitch = getMaxSwitch(vmStartAssignmentLoad);
        Double maxLoad = vmStartAssignmentLoad.get(maximumSwitch);

        List<Rule> toBreakRules = loadTheRules(changesNum, partitions, srcIPIndex);

        Set<Switch> leftSwitches = new HashSet<>();
        Set<Switch> rightSwitches = new HashSet<>();
        Set<Switch> commonSwitches = new HashSet<>();

        int changesUntilNow = 0;
        double similarityChange = 0;
        boolean useful = false;
        while ((changeStepsOrSimilarity && changesUntilNow < changesNum) || (!changeStepsOrSimilarity && similarityChange < changesNum)) {
            //pick a rule divide by half
            while (!useful && toBreakRules.size() > 0) {
                int toBreakRuleIndex = random.nextInt(toBreakRules.size());
                Rule toBreakRule = toBreakRules.get(toBreakRuleIndex);
                RangeDimensionRange srcIP = toBreakRule.getProperty(srcIPIndex);

                RangeDimensionRange newSrcIP = new RangeDimensionRange(srcIP.getStart() + srcIP.getSize() / 2, srcIP.getEnd(), srcIP.getInfo());
                NavigableMap<Long, Partition> leftPartitions = srcIPPartitionMap.subMap(srcIP.getStart(), true, srcIP.getStart() + srcIP.getSize() / 2 - 1, true);
                NavigableMap<Long, Partition> rightPartitions = srcIPPartitionMap.subMap(newSrcIP.getStart(), true, newSrcIP.getEnd(), true);

                if (keepMax && isMaxViolated(leftPartitions, rightPartitions, assignment, vmStartAssignmentLoad, maxLoad, leftSwitches, rightSwitches, commonSwitches)) {
                    toBreakRules.remove(toBreakRuleIndex);
                    continue;
                }

                //commonSwitches.size() == 0; //having no common switches on both side means that there is no contribution to load of vm start
                //System.out.println(tryNum++ + ":" + changesUntilNow + ": " + leftSwitches.size() + ", " + rightSwitches.size() + ", " + commonSwitches.size() + "=" + srcIP);


                useful = leftPartitions.size() > 0 && rightPartitions.size() > 0;
                similarityChange += 2.0 * leftPartitions.size() * rightPartitions.size() / (partitions.size() * (partitions.size() - 1));

                //update partitions
                srcIP.setEnd(srcIP.getStart() + srcIP.getSize() / 2 - 1);
                toBreakRule.updateProperties();
                toBreakRules.remove(toBreakRuleIndex);
                Rule newRule = null;
                if (rightPartitions.size() > 0) {
                    newRule = getNewRule(toBreakRule, newSrcIP);
                    for (Partition partition1 : rightPartitions.values()) {
                        partition1.getRules().remove(toBreakRule);
                        partition1.getRules().add(newRule);
                    }
                }

                //check if we need to break the rules more
                if (srcIP.getSize() > 1) { //if we can break the range more (both sides have the same size so only check one
                    if (leftPartitions.size() > 1) {
                        toBreakRules.add(toBreakRule);//add a new
                    }
                    if (rightPartitions.size() > 1) { //there should be at least two partitions there
                        toBreakRules.add(newRule);//add a new
                    }
                }
            }

            if (useful) {
                changesUntilNow++;
                useful = false;
            } else {
                throw new IncompleteTransformException("Could not find a non-micro rule. Successful changes= " + changesUntilNow);
            }
        }
    }

    private Rule getNewRule(Rule toBreakRule, RangeDimensionRange newSrcIP) {

        List<RangeDimensionRange> properties = new ArrayList<>(toBreakRule.getProperties());
        properties.set(Util.getDimensionInfoIndex(Util.SRC_IP_INFO), newSrcIP);
        properties.set(Util.getDimensionInfoIndex(Util.DST_IP_INFO), toBreakRule.getProperty(Util.getDimensionInfoIndex(Util.DST_IP_INFO)));
        properties.set(Util.getDimensionInfoIndex(Util.DST_PORT_INFO), toBreakRule.getProperty(Util.getDimensionInfoIndex(Util.DST_PORT_INFO)));
        properties.set(Util.getDimensionInfoIndex(Util.SRC_PORT_INFO), toBreakRule.getProperty(Util.getDimensionInfoIndex(Util.SRC_PORT_INFO)));
        properties.set(Util.getDimensionInfoIndex(Util.PROTOCOL_INFO), toBreakRule.getProperty(Util.getDimensionInfoIndex(Util.PROTOCOL_INFO)));

        return new Rule(toBreakRule.getAction(), properties, toBreakRule.getPriority(), Rule.maxId + 1);
    }

    private List<Rule> loadTheRules(int changesNum, List<Partition> partitions, int srcIPIndex) {
        List<Rule> toBreakRules;
        Map<Rule, Boolean> toBreakRules1 = new HashMap<>();
        for (Partition partition : partitions) {
            for (Rule toBreakRule : partition.getRules()) {
                RangeDimensionRange srcIP = toBreakRule.getProperty(srcIPIndex);
                if (srcIP.getSize() == 1) {
                    continue;
                }
                Boolean seenBefore = toBreakRules1.get(toBreakRule);
                toBreakRules1.put(toBreakRule, seenBefore != null); //keep track of rules that have been seen by at least two partitions
            }
        }
        toBreakRules = new ArrayList<>(toBreakRules1.size() + changesNum);
        for (Map.Entry<Rule, Boolean> entry : toBreakRules1.entrySet()) {
            if (entry.getValue()) {
                toBreakRules.add(entry.getKey());
            }
        }
        //Collections.shuffle(toBreakRules, random); not enough as I need to add more rules later
        return toBreakRules;
    }

    private boolean isMaxViolated(Map<Long, Partition> leftPartitions, Map<Long, Partition> rightPartitions, Assignment assignment,
                                  Map<Switch, Double> vmStartAssignmentLoad, Double maxLoad, Set<Switch> leftSwitches,
                                  Set<Switch> rightSwitches, Set<Switch> commonSwitches) {
        //check machine sizes on both side to be not more than max
        leftSwitches.clear();
        rightSwitches.clear();
        commonSwitches.clear();
        hasPartitionIn(leftPartitions, assignment.getPlacement(), leftSwitches);
        hasPartitionIn(rightPartitions, assignment.getPlacement(), rightSwitches);
        commonSwitches.addAll(leftSwitches);
        commonSwitches.retainAll(rightSwitches);

        boolean maxLoadViolation = false;
        for (Switch increasedSwitch : commonSwitches) {
            Double newLoad = vmStartAssignmentLoad.get(increasedSwitch) + 1;
            if (newLoad > maxLoad) {
                maxLoadViolation = true;
                break;
            }
        }
        if (maxLoadViolation) {
            return true;
        }


        for (Switch increasedSwitch : commonSwitches) {
            Double newLoad = vmStartAssignmentLoad.get(increasedSwitch) + 1;
            vmStartAssignmentLoad.put(increasedSwitch, newLoad);
        }
        return false;
    }

}
