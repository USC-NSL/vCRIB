package edu.usc.enl.cacheflow.processor.partition.transform;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.statistics.PartitionStatisticsProcessor;
import edu.usc.enl.cacheflow.util.CustomRandomGenerator;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 10/30/12
 * Time: 12:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class AddRemoveRandomRulesPartitionTransformer extends PartitionTransformer {
    private final CustomRandomGenerator<Boolean> addOrRemoveDistribution;
    private final CustomRandomGenerator<Long> addDistribution;
    private final CustomRandomGenerator<Long> removeDistribution;
    private final boolean keepMax;
    private boolean changeStepsOrSize;

    public AddRemoveRandomRulesPartitionTransformer(CustomRandomGenerator<Boolean> addOrRemoveDistribution,
                                                    CustomRandomGenerator<Long> addDistribution,
                                                    CustomRandomGenerator<Long> removeDistribution, boolean keepMax,
                                                    boolean changeStepsOrSize) {
        this.addOrRemoveDistribution = addOrRemoveDistribution;
        this.addDistribution = addDistribution;
        this.removeDistribution = removeDistribution;
        this.keepMax = keepMax;
        this.changeStepsOrSize = changeStepsOrSize;
    }

    @Override
    public void transform(Random random, int changesNum, List<Partition> partitions, Map<Switch, Collection<Partition>> sourcePartitions,
                          Topology topology) throws NoAssignmentFoundException, IncompleteTransformException {
        Map<Long, List<Rule>> sizeRulesMap = new TreeMap<>();
        Set<Rule> rules = new HashSet<>(); //just as a set here
        final int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        {
            for (Partition partition : partitions) {
                rules.addAll(partition.getRules());
            }
            for (Rule rule : rules) {
                long size = rule.getProperty(srcIPIndex).getSize();
                addToRuleSize(sizeRulesMap, size, rule);
            }
        }

        Assignment assignment = Assignment.getAssignment(sourcePartitions);
        Map<Switch, Double> vmStartAssignmentLoad = getVMStartAssignmentLoad(partitions, sourcePartitions, topology, assignment);
        //find maximum
        Switch maximumSwitch = getMaxSwitch(vmStartAssignmentLoad);
        Double maxLoad = vmStartAssignmentLoad.get(maximumSwitch);
        Collection<Partition> maxPartitions = sourcePartitions.get(maximumSwitch);

        TreeMap<Long, Partition> srcIPPartitionMap = getSrcIPPartitionMap(partitions, srcIPIndex);
        int maxPriority = getMaxPriority(partitions);

        List<RangeDimensionRange> templateProperties = rules.iterator().next().getProperties();
        HashSet<Switch> toFill = new HashSet<>();
        double changesUntilNow = 0;

        List<Partition> partitionsList = new ArrayList<>(partitions);
        partitionsList.removeAll(sourcePartitions.get(maximumSwitch));

//        double sim = 0;
//        try {
//            final Statistics stats = new PartitionStatisticsProcessor(partitions, new HashMap<String,Object>()).run();
//            sim = stats.getStat(PartitionStatisticsProcessor.MEAN_SIMILARITY).doubleValue();
//            System.out.println(sim);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }


        while (changesUntilNow < changesNum) {
            boolean add = addOrRemoveDistribution.getRandom(random.nextDouble());

//            if (changesUntilNow == 4) {
//                System.out.println();
//            }

            if (add) {
                changesUntilNow = add(random, sizeRulesMap, srcIPIndex, assignment, vmStartAssignmentLoad, maxLoad,
                        srcIPPartitionMap, maxPriority, templateProperties, toFill, changesUntilNow, partitionsList);
            } else {
                changesUntilNow = remove(random, maxPartitions, sizeRulesMap, srcIPIndex, maximumSwitch, srcIPPartitionMap,
                        maxPriority, changesUntilNow);
//                if (changesUntilNow == 5) {
//                    try {
//                        final Statistics stats = new PartitionStatisticsProcessor(partitions, new HashMap<String, Object>()).run();
//                        double newSim = stats.getStat(PartitionStatisticsProcessor.MEAN_SIMILARITY).doubleValue();
//                        if (sim != newSim) {
//                            System.out.println("Not equal sim");
//                        }
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
            }

        }
    }

    private double remove(Random random, Collection<Partition> maxPartitions, Map<Long, List<Rule>> sizeRulesMap,
                          int srcIPIndex, Switch maximumSwitch, TreeMap<Long, Partition> srcIPPartitionMap, int maxPriority,
                          double changesUntilNow) throws IncompleteTransformException {
        Rule toRemove = null;
        NavigableMap<Long, Partition> coveredPartitions = null;
        List<Rule> sizeRules = null;

        final int maxSizeRemoveTry = 50;
        final int maxPositionRemoveTry = 50;
        int sizeRemoveTry = 0;
        while (sizeRemoveTry < maxSizeRemoveTry) {//&& sizeRulesMap.size() > 0 && !successful
            sizeRemoveTry++;
            Long toRemoveRuleSrcSize = removeDistribution.getRandom(random.nextDouble());
            //System.out.println(toRemoveRuleSrcSize);
            sizeRules = sizeRulesMap.get(toRemoveRuleSrcSize);
            if (sizeRules == null) {
                continue;
            }
            if (sizeRules.size() == 0) {
                sizeRulesMap.remove(toRemoveRuleSrcSize);
                continue;
            }
            int positionRemoveTry = 0;
            while (positionRemoveTry < maxPositionRemoveTry) {
                positionRemoveTry++;
                if (sizeRules.size() == 0) {
                    sizeRulesMap.remove(toRemoveRuleSrcSize);
                    break;
                }
                int index = random.nextInt(sizeRules.size());
                toRemove = sizeRules.remove(index);
                if (toRemove.getPriority() == maxPriority) {
                    continue;
                }

                coveredPartitions = isRemoveMaxViolated(maxPartitions, srcIPIndex, maximumSwitch, srcIPPartitionMap, toRemove);
                if (coveredPartitions != null) {
                    break;
                }
            }
            if (coveredPartitions != null) {
                break;
            }
        }

        if (sizeRemoveTry < maxSizeRemoveTry) {
            for (Partition partition : coveredPartitions.values()) {
                partition.getRules().remove(toRemove);
            }

            if (this.changeStepsOrSize) {
                changesUntilNow++;
            } else {
                changesUntilNow += 1.0 * coveredPartitions.size() / srcIPPartitionMap.size();
            }
        } else {
            throw new IncompleteTransformException("Could not find more rule to remove. " + changesUntilNow + " changes done");
        }
        return changesUntilNow;
    }

    private double add(Random random, Map<Long, List<Rule>> sizeRulesMap, int srcIPIndex, Assignment assignment,
                    Map<Switch, Double> vmStartAssignmentLoad, Double maxLoad, TreeMap<Long, Partition> srcIPPartitionMap,
                    int maxPriority, List<RangeDimensionRange> templateProperties, HashSet<Switch> tempSet, double changesUntilNow,
                    List<Partition> notMaxPartitions)
            throws IncompleteTransformException {
        NavigableMap<Long, Partition> coveredPartitions = null;
        RangeDimensionRange srcIP = null;
        Long newRuleSrcSize = null;

        int addPositionMaxTry = 100;
        int addSizeMaxTry = 10;
        int sizeTryNum = 0;
        while (sizeTryNum < addSizeMaxTry) {
            sizeTryNum++;
            newRuleSrcSize = addDistribution.getRandom(random.nextDouble());
            int wildcard = RangeDimensionRange.binlog(newRuleSrcSize);
            int positionTryNum = 0;
            while (positionTryNum < addPositionMaxTry) {
                positionTryNum++;

                //pick a random partition to hint the position
                Partition hintPartition = notMaxPartitions.get(random.nextInt(notMaxPartitions.size()));
                long hintIP = hintPartition.getProperty(srcIPIndex).getStart();
                long srcIPStart = (hintIP >> wildcard) << wildcard;
                //(long) (random.nextDouble() * Util.SRC_IP_INFO.getDimensionRange().getSize() / newRuleSrcSize) * newRuleSrcSize;
                srcIP = new RangeDimensionRange(srcIPStart, srcIPStart + newRuleSrcSize - 1, Util.getDimensionInfos().get(srcIPIndex));
                coveredPartitions = srcIPPartitionMap.subMap(srcIP.getStart(), true, srcIP.getEnd(), true);
                if (!(keepMax && isAddMaxViolated(assignment, vmStartAssignmentLoad, maxLoad, coveredPartitions, tempSet))) {
                    break;
                }
            }
            if (positionTryNum < addPositionMaxTry) {
                break;
            }
        }
        if (sizeTryNum < addSizeMaxTry) {
            Rule newRule = getANewRule(random, templateProperties, srcIP, random.nextInt(maxPriority),
                    random.nextBoolean() ? AcceptAction.getInstance() : DenyAction.getInstance());

            addToRuleSize(sizeRulesMap, newRuleSrcSize, newRule);
            for (Partition partition : coveredPartitions.values()) {
                partition.getRules().add(newRule);
            }
            if (changeStepsOrSize) {
                changesUntilNow++;
            }else{
                changesUntilNow += 1.0*coveredPartitions.size() / srcIPPartitionMap.size();
            }
        } else {
            throw new IncompleteTransformException("Cannot add a new rule without changing max. " + changesUntilNow + " changes done");
        }
        return changesUntilNow;
    }

    private NavigableMap<Long, Partition> isRemoveMaxViolated(Collection<Partition> maxPartitions, int srcIPIndex,
                                                              Switch maximumSwitch, TreeMap<Long, Partition> srcIPPartitionMap, Rule toRemove) {
        //check to not tamper with the max switch load
        RangeDimensionRange srcIP = toRemove.getProperty(srcIPIndex);
        NavigableMap<Long, Partition> tempMap = srcIPPartitionMap.subMap(srcIP.getStart(), true, srcIP.getEnd(), true);
        if (keepMax) {
            boolean isMaxViolated = false;

            for (Partition partition : tempMap.values()) {
                if (maxPartitions.contains(partition)) {
                    isMaxViolated = true;
                    break;
                }
            }

            if (isMaxViolated) {
                return null;
            }
        }
        return tempMap;
    }

    private boolean isAddMaxViolated(Assignment assignment, Map<Switch, Double> vmStartAssignmentLoad, Double maxLoad,
                                     NavigableMap<Long, Partition> tempMap, HashSet<Switch> toFill) {
        toFill.clear();
        hasPartitionIn(tempMap, assignment.getPlacement(), toFill);
        boolean maxIsViolated = false;
        for (Switch aSwitch : toFill) {
            double newLoad = vmStartAssignmentLoad.get(aSwitch) + 1;
            if (newLoad > maxLoad) {
                maxIsViolated = true;
                break;
            }
        }
        if (maxIsViolated) {
            return true;
        }
        for (Switch aSwitch : toFill) {
            double newLoad = vmStartAssignmentLoad.get(aSwitch) + 1;
            vmStartAssignmentLoad.put(aSwitch, newLoad);
        }
        return false;
    }

    private void addToRuleSize(Map<Long, List<Rule>> sizeRule, Long newRuleSrcSize, Rule newRule) {
        List<Rule> rules = sizeRule.get(newRuleSrcSize);
        if (rules == null) {
            rules = new ArrayList<>();
            sizeRule.put(newRuleSrcSize, rules);
        }
        rules.add(newRule);
    }

}
