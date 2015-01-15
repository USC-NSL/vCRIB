package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner;

import edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.rulepartitioner.partitioner.HalfNoSplitMinCutPartitioner;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/23/12
 * Time: 8:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class BipartitePartitionMultiModel extends AbstractBipartitePartition {
    private HalfNoSplitMinCutPartitioner partitioner;
    private int memory = -1;
    private int sources = -1;
    private int numberOfPartitions = -1;
    private int maxPartitionSize = -1;

    public BipartitePartitionMultiModel(HalfNoSplitMinCutPartitioner partitioner, int memory, int sources) {
        this.partitioner = partitioner;
        this.memory = memory;
        this.sources = sources;
    }

    public BipartitePartitionMultiModel(HalfNoSplitMinCutPartitioner partitioner, int numberOfPartitions, boolean numOrMaxSize) {
        this.partitioner = partitioner;
        if (numOrMaxSize) {
            this.numberOfPartitions = numberOfPartitions;
        } else {
            this.maxPartitionSize = numberOfPartitions;
        }
    }


    public List<Partition> sequentialPartition(Collection<Rule> rules2) throws Exception {
        int totalUntilNow = rules2.size();
        Partition initialPartition = new Partition(rules2);
        Map<Long, Integer> wildcardsFreq = new HashMap<Long, Integer>();
        for (Rule rule : rules2) {

            try {
                changeWildcardBitFreq(wildcardsFreq, rule.getWildCardBitPattern(), 1);
            } catch (UnalignedRangeException e) {
                System.out.println(rule);
            }
        }
        System.out.println(wildcardsFreq.size() + " original wildcards");
        changeWildcardBitFreq(wildcardsFreq, initialPartition.getWildcardPattern(), 1);
        PriorityQueue<Partition> toBePartitioned = new PriorityQueue<Partition>(10, Collections.reverseOrder(new Partition.SizeComparator()));
        toBePartitioned.add(initialPartition);
        return doPartitioning(totalUntilNow, wildcardsFreq, toBePartitioned);
    }

    public List<Partition> sequentialPartition(Collection<Partition> initialPartitions, Collection<Rule> rules) throws Exception {
        if (rules == null) {
            rules = new HashSet<Rule>();
            for (Partition initialPartition : initialPartitions) {
                rules.addAll(initialPartition.getRules());
            }
        }
        Map<Long, Integer> wildcardsFreq = new HashMap<Long, Integer>();
        for (Rule rule : rules) {
            try {
                changeWildcardBitFreq(wildcardsFreq, rule.getWildCardBitPattern(), 1);
            } catch (UnalignedRangeException e) {
                System.out.println(rule);
            }
        }

        PriorityQueue<Partition> toBePartitioned = new PriorityQueue<Partition>(initialPartitions.size()*2, Collections.reverseOrder(new Partition.SizeComparator()));
        toBePartitioned.addAll(initialPartitions);
        int totalUntilNow = 0;
        for (Partition initialPartition : initialPartitions) {
            totalUntilNow += initialPartition.getRules().size();
            changeWildcardBitFreq(wildcardsFreq, initialPartition.getWildcardPattern(), 1);
        }
        return doPartitioning(totalUntilNow, wildcardsFreq, toBePartitioned);
    }

    private List<Partition> doPartitioning(int totalUntilNow, Map<Long, Integer> wildcardsFreq, PriorityQueue<Partition> toBePartitioned) {
        if ((numberOfPartitions > 0 && toBePartitioned.size() >= numberOfPartitions)//nothing is in partitions
                || (maxPartitionSize > 0 && toBePartitioned.peek().getRules().size() <= maxPartitionSize)) {
            return new ArrayList<Partition>(toBePartitioned);
        }
        List<Partition> partitions = new ArrayList<Partition>(Math.max(numberOfPartitions, toBePartitioned.size()));
        while (toBePartitioned.size() > 0) {
            Partition partition = toBePartitioned.poll();
            Collection<Rule> rules = partition.getRules();
            if (rules.size() == 0) {
                continue;
            }
            if (rules.size() < 2) {//not partition less than 2
                partitions.add(partition);
                continue;
            }
            partitioner.partition(partition, wildcardsFreq);
            Partition bestPartition1 = partitioner.getBestPartition1();
            Partition bestPartition2 = partitioner.getBestPartition2();


            if (bestPartition1 != null) {
                //System.out.println(toBePartitioned.size() + " " + rules.size() + " -> " + bestPartition1.getSize() + ", " + bestPartition2.getSize());
                //keep only to be partitioned
                int newTotal = totalUntilNow + bestPartition1.getRules().size() + bestPartition2.getRules().size() - rules.size();
                final int memoryForRules = getMemoryForRules(partitions.size() + toBePartitioned.size() + 2);
                if ((memory > 0 && newTotal <= memoryForRules) || (numberOfPartitions > 0) || (maxPartitionSize > 0)) {
                    toBePartitioned.add(bestPartition1);
                    toBePartitioned.add(bestPartition2);
                    final long currentPartitionWC = partitioner.getCurrentPartitionWC();
                    changeWildcardBitFreq(wildcardsFreq, currentPartitionWC, -1);
                    changeWildcardBitFreq(wildcardsFreq, partitioner.getMinHalfWildCard(), 2);
                    totalUntilNow = newTotal;
                    if ((numberOfPartitions > 0 && toBePartitioned.size() + partitions.size() >= numberOfPartitions)
                            || (maxPartitionSize > 0 && toBePartitioned.peek().getRules().size() <= maxPartitionSize)) {
                        //drain all to partitions not necessarily sorted
                        partitions.addAll(toBePartitioned);
                        toBePartitioned.clear();
                    }
                } else {
                    partitions.add(partition);
                }
            } else {
                //Util.logger.info("No partition point found");
                //all overlap
                partitions.add(partition);
            }
        }
        System.out.println(partitions.size() + " created with size " + totalUntilNow + " in " + wildcardsFreq.size() + " wildcard patterns");
        /*try {
            for (Partition partition : partitions) {
                System.out.println(WildCardPattern.reverseWildcardPattern(partition.getWildcardPattern(), Util.getDimensionInfos()));
            }
        } catch (UnalignedRangeException e) {
            e.printStackTrace();
        }*/
        return partitions;
    }

    private int getMemoryForRules(int numberOfPartitions) {
        return memory - numberOfPartitions * sources;
    }

    private void changeWildcardBitFreq(Map<Long, Integer> wildcardPatterns, long pattern, int change) {
        Integer freq = wildcardPatterns.get(pattern);
        if (freq == null && change < 0) {
            throw new RuntimeException("wildcard pattern " + pattern + " not found for change " + change);
        }
        if (change < 0) {
            if (freq + change == 0) {
                wildcardPatterns.remove(pattern);
            } else {
                wildcardPatterns.put(pattern, freq + change);
            }
        } else {
            if (freq == null) {
                freq = 0;
            }
            wildcardPatterns.put(pattern, freq + change);
        }
    }

    /*private boolean checkMemoryBudge(List<Switch> memoryBoundedSwitches, List<Partition> partitions) throws Exception {
        //check if 1/n of partitions can be placed
        final int numberOfPartitionsToBeChecked = partitions.size() / topology.getSwitches().size(); //assume all switches are Memory bound
        List<Partition> toBeCheckedPartitions = new ArrayList<Partition>(numberOfPartitionsToBeChecked);
        Collections.shuffle(partitions, Util.random);
        for (Partition partition : partitions) {
            if (toBeCheckedPartitions.size() >= numberOfPartitionsToBeChecked) {
                break;
            } else {
                toBeCheckedPartitions.add(partition);
            }
        }
        if (toBeCheckedPartitions.size() > 0) {
            List<Rule> rules = new LinkedList<Rule>();
            for (Partition toBeCheckedPartition : toBeCheckedPartitions) {
                rules.addAll(toBeCheckedPartition.getRules());
            }
            final Switch.FeasibleState feasible = memoryBoundedSwitches.get(0).isFeasible(rules, 0);
            return feasible != null;
        } else {
            return true;
        }
    }

    private boolean checkCPUBudget(List<Switch> cpuBoundedSwitches, List<Partition> partitions, int flows) throws Exception {
        if (cpuBoundedSwitches.size() == 0) {
            return true;
        }
        //cpu budge
        //select p/n worst partitions
        Collections.shuffle(partitions, Util.random);
        final int numberOfPartitionsToBeChecked = partitions.size() / topology.getSwitches().size(); //assume all switches are CPU bound
        List<Partition> toBeCheckedPartitions = new ArrayList<Partition>(numberOfPartitionsToBeChecked);
        List<Rule> rules = new LinkedList<Rule>();
        for (Partition partition : partitions) {
            if (toBeCheckedPartitions.size() >= numberOfPartitionsToBeChecked) {
                //add forwarding rule
                rules.add(partition.getForwardingRule(cpuBoundedSwitches.get(0)));
            } else {
                toBeCheckedPartitions.add(partition);
            }
        }
        if (toBeCheckedPartitions.size() > 0) {
            //add rules of those partitions and forwarding rules
            for (Partition toBeCheckedPartition : toBeCheckedPartitions) {
                rules.addAll(toBeCheckedPartition.getRules());
            }
            //to an edge switch and assume uniform traffic

            //check feasibility
            final Switch.FeasibleState feasible = cpuBoundedSwitches.get(0).isFeasible(rules, flows);
            return feasible != null;
        } else {
            return true;
        }
    }

    private List<Switch> getMemoryBoundedSwitches(Topology topology) {
        List<Switch> output = new ArrayList<Switch>(topology.getSwitches().size());
        for (Switch aSwitch : topology.getSwitches()) {
            if (!(aSwitch instanceof OVSSwitch)) {
                output.add(aSwitch);
            }
        }
        return output;
    }

    private List<Switch> getCPUBoundedSwitches(Topology topology) {
        List<Switch> output = new ArrayList<Switch>(topology.getSwitches().size());
        for (Switch aSwitch : topology.getSwitches()) {
            if (aSwitch instanceof OVSSwitch) {
                output.add(aSwitch);
            }
        }
        return output;
    }*/
}
