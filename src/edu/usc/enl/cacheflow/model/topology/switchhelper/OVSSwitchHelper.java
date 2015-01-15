package edu.usc.enl.cacheflow.model.topology.switchhelper;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.OVSSwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/22/12
 * Time: 6:10 AM
 * To change this template use File | Settings | File Templates.
 */
public class OVSSwitchHelper extends SwitchHelper<OVSSwitch> {
    private final int threadNum;
    private Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap;
    private final Map<Partition, PartitionOVSData> partitionData;
    private final Map<Partition, Map<Switch, Rule>> forwardingRules;
    private CollectionPool<Set<Rule>> ruleSetPool;
    private CollectionPool<Set<Long>> wildcardPool;

    public OVSSwitchHelper(List<Partition> partitions, int threadNum,
                           Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap,
                           Map<Partition, Map<Switch, Rule>> forwardingRules
    ) throws UnalignedRangeException {
        this.threadNum = threadNum;
        this.ruleFlowMap = ruleFlowMap;
        this.forwardingRules = forwardingRules;
        partitionData = new HashMap<>(partitions.size(), 1);
        for (Partition partition : partitions) {
            partitionData.put(partition, new PartitionOVSData(partition));
        }
    }

    public void setRuleSetPool(CollectionPool<Set<Rule>> ruleSetPool) {
        this.ruleSetPool = ruleSetPool;
    }

    public void setWildcardPool(CollectionPool<Set<Long>> wildcardPool) {
        this.wildcardPool = wildcardPool;
    }

    @Override
    public double resourceUsage(Switch host, Partition newPartition, Collection<Switch> sources, boolean checkResources)
            throws Switch.InfeasibleStateException {
        final OVSSwitch.OVSState currentState = (OVSSwitch.OVSState) host.getState();
        final int cpuPercentCapacity = ((OVSSwitch) host).getCpuPercentCapacity();
        if (cpuPercentCapacity == 0) {
            if (newPartition.getRules().size() == 0 && currentState.getRules().size() == 0) {
                return 0;
            } else {
                throw new Switch.InfeasibleStateException("Zero capacity in switch " + host);
            }
        }

        final PartitionOVSData partitionOVSData = partitionData.get(newPartition);

        final CollectionPool.TempCollection<Set<Rule>> tempRules = ruleSetPool.getTempCollection();
        Set<Rule> tempRuleSet = tempRules.getData();
        tempRuleSet.addAll(currentState.getRules());
        tempRuleSet.addAll(newPartition.getRules());
        tempRuleSet.remove(forwardingRules.get(newPartition).get(host));

        final CollectionPool.TempCollection<Set<Long>> tempWildcard = wildcardPool.getTempCollection();
        final Set<Long> tempWildcardSet = tempWildcard.getData();
        tempWildcardSet.addAll(currentState.getWildcards());
        tempWildcardSet.addAll(partitionOVSData.getWildcards());
        // TODO: REMOVE THE WILDCARD FOR THE PARTITION

        int newFlows = currentState.getNewFlows();

        for (Switch source : sources) {
            newFlows += partitionOVSData.getFlowsFrom(source);
        }
        newFlows -= partitionOVSData.getFlowsFrom(host);
        newFlows -= partitionOVSData.getFlowsTo(host);

        final int ruleSize = tempRuleSet.size();
        final int wildcardSize = tempWildcardSet.size();
        tempRules.release();
        tempWildcard.release();

        final double cpuUsageRatio = OVSSwitch.computeCPUUsage(newFlows, ruleSize, wildcardSize) / cpuPercentCapacity;
        if (cpuUsageRatio > 1 && checkResources) {
            throw new Switch.InfeasibleStateException(cpuUsageRatio + " CPU Usage (W=" + wildcardSize + ", R=" + ruleSize +
                    ", F=" + newFlows + ") in " + this.toString() + " with capacity " + cpuPercentCapacity);
        }
        return cpuUsageRatio;
    }


    @Override
    public Switch.FeasibleState isAddFeasible(OVSSwitch host,
                                              Partition newPartition, Collection<Switch> sources,
                                              boolean checkResources, boolean selfCommit) throws Switch.InfeasibleStateException {
        final OVSSwitch.OVSState currentState = (OVSSwitch.OVSState) host.getState();
        final PartitionOVSData partitionOVSData = partitionData.get(newPartition);

        final CollectionPool.TempCollection<Set<Rule>> tempRules = ruleSetPool.getTempCollection();
        Set<Rule> tempRuleSet = tempRules.getData();
        tempRuleSet.addAll(currentState.getRules());
        tempRuleSet.addAll(newPartition.getRules());
        tempRuleSet.remove(forwardingRules.get(newPartition).get(host));

        final CollectionPool.TempCollection<Set<Long>> tempWildcards = wildcardPool.getTempCollection();
        final Set<Long> tempWildcardsSet = tempWildcards.getData();
        tempWildcardsSet.addAll(currentState.getWildcards());
        tempWildcardsSet.addAll(partitionOVSData.getWildcards());
        // TODO: REMOVE THE WILDCARD FOR THE PARTITION

        int newFlows = currentState.getNewFlows();

        for (Switch source : sources) {
            newFlows += partitionOVSData.getFlowsFrom(source);
        }
        newFlows -= partitionOVSData.getFlowsFrom(host);
        newFlows -= partitionOVSData.getFlowsTo(host);

        try {

            return isFeasible(host, checkResources, selfCommit, tempRuleSet, tempWildcardsSet, newFlows);
        } finally {
            tempRules.release();
            tempWildcards.release();
        }
    }


    public OVSSwitch.OVSState isFeasible(OVSSwitch host, boolean checkResources, boolean selfCommit,
                                         Set<Rule> tempRuleSet,
                                         Set<Long> tempWildcardsSet,
                                         int newFlows) throws Switch.InfeasibleStateException {
        final OVSSwitch.OVSState currentState = (OVSSwitch.OVSState) host.getState();
        final int ruleSize = tempRuleSet.size();
        final int wildcardSize = tempWildcardsSet.size();
        float cpuUsage = OVSSwitch.computeCPUUsage(newFlows, ruleSize, wildcardSize);
        if (cpuUsage <= host.getCpuPercentCapacity() || !checkResources) {
            OVSSwitch.OVSState output;
            if (selfCommit) {
                currentState.setCpuUsage(cpuUsage);
                currentState.setNewFlows(newFlows);
                currentState.fillRules(tempRuleSet);
                currentState.fillWildcards(tempWildcardsSet);
                output = currentState;
            } else {
                final Set<Rule> ruleSet = Util.getNewCollectionInstance(tempRuleSet);
                ruleSet.addAll(tempRuleSet);

                final Set<Long> wildcardSet = Util.getNewCollectionInstance(tempWildcardsSet);
                wildcardSet.addAll(tempWildcardsSet);
                output = new OVSSwitch.OVSState(ruleSet, cpuUsage, newFlows, wildcardSet);
            }
            return output;

        } else {
            throw new Switch.InfeasibleStateException(cpuUsage + " CPU Usage (W=" + wildcardSize + ", R=" + ruleSize +
                    ", F=" + newFlows + ") in " + host + " with capacity " + host.getCpuPercentCapacity());

        }
    }

    @Override
    public Switch.FeasibleState isRemoveFeasible(OVSSwitch host, Map<Partition, Collection<Switch>> currentPartitions,
                                                 Collection<Partition> sourcePartitions,
                                                 boolean checkResources, boolean selfCommit, Partition removedPartition, Collection<Switch> removedPartitionSources) throws Switch.InfeasibleStateException {
        int newFlows = ((OVSSwitch.OVSState) host.getState()).getNewFlows();
        if (removedPartition != null) {
            final PartitionOVSData partitionOVSData = partitionData.get(removedPartition);
            for (Switch source : removedPartitionSources) {
                newFlows -= partitionOVSData.getFlowsFrom(source);
            }
            newFlows += partitionOVSData.getFlowsFrom(host);
            newFlows += partitionOVSData.getFlowsTo(host);
        }

        final CollectionPool.TempCollection<Set<Rule>> tempRules = ruleSetPool.getTempCollection();
        Set<Rule> tempRuleSet = tempRules.getData();
        final CollectionPool.TempCollection<Set<Long>> tempWildcards = wildcardPool.getTempCollection();
        final Set<Long> tempWildcardsSet = tempWildcards.getData();
        if (currentPartitions != null) {
            for (Map.Entry<Partition, Collection<Switch>> entry : currentPartitions.entrySet()) {
                final Partition partition = entry.getKey();
                if (!partition.equals(removedPartition)) {
                    tempRuleSet.addAll(partition.getRules());
                    final PartitionOVSData partitionOVSData = partitionData.get(partition);
                    tempWildcardsSet.addAll(partitionOVSData.getWildcards());
                }
            }
        }
        if (sourcePartitions != null) {
            for (Partition sourcePartition : sourcePartitions) {
                final PartitionOVSData partitionOVSData = partitionData.get(sourcePartition);
                if (currentPartitions == null || !currentPartitions.containsKey(sourcePartition) ||
                        sourcePartition.equals(removedPartition)) {
                    tempRuleSet.add(forwardingRules.get(sourcePartition).get(host));
                }
                //DO IT ANYWAY AS IN ADD YOU DO NOT REMOVE THIS
                tempWildcardsSet.add(partitionOVSData.getFwWildcard());
            }
        }

        try {
            return isFeasible(host, checkResources, selfCommit, tempRuleSet, tempWildcardsSet, newFlows);
        } finally {
            tempRules.release();
            tempWildcards.release();
        }
    }

    @Override
    public void resetRuleCollections(OVSSwitch aSwitch) throws Switch.InfeasibleStateException {
        final CollectionPool.TempCollection<Set<Rule>> tempCollection = ruleSetPool.getTempCollection();
        final CollectionPool.TempCollection<Set<Long>> tempCollection1 = wildcardPool.getTempCollection();
        try {
            aSwitch.setState(isFeasible(aSwitch, true, false, tempCollection.getData(), tempCollection1.getData(), 0));
        } finally {
            tempCollection.release();
            tempCollection1.release();
        }
    }

    @Override
    public Switch.FeasibleState initToNotOnSrc(OVSSwitch host, Collection<Partition> sourcePartitions, boolean selfCommit) throws Switch.InfeasibleStateException {
        final CollectionPool.TempCollection<Set<Rule>> tempRules = ruleSetPool.getTempCollection();
        Set<Rule> tempRuleSet = tempRules.getData();
        final CollectionPool.TempCollection<Set<Long>> tempWildcards = wildcardPool.getTempCollection();
        final Set<Long> tempWildcardsSet = tempWildcards.getData();

        int newFlows = 0;
        for (Map.Entry<Partition, PartitionOVSData> entry : partitionData.entrySet()) {
            PartitionOVSData partitionOVSData = entry.getValue();
            newFlows += partitionOVSData.getFlowsFrom(host);
            newFlows += partitionOVSData.getFlowsTo(host);
        }
        if (sourcePartitions != null) {
            for (Partition sourcePartition : sourcePartitions) {
                final PartitionOVSData partitionOVSData = partitionData.get(sourcePartition);
                tempRuleSet.add(forwardingRules.get(sourcePartition).get(host));
                tempWildcardsSet.add(partitionOVSData.getFwWildcard());
            }
        }


        try {
            return isFeasible(host, true, selfCommit, tempRuleSet, tempWildcardsSet, newFlows);
        } finally {
            tempRules.release();
            tempWildcards.release();
        }
    }

    @Override
    public void migrate(Switch src, Switch dst, Partition partition) {
        //update data structure
        final int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        final int dstIPIndex = Util.getDimensionInfoIndex(Util.DST_IP_INFO);
        long vmIP = partition.getProperty(srcIPIndex).getStart();
        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> entry : ruleFlowMap.entrySet()) {
            for (Map.Entry<Rule, Collection<Flow>> ruleFlowsMap : entry.getValue().entrySet()) {
                for (Flow flow : ruleFlowsMap.getValue()) {
                    final Long srcIP = flow.getProperty(srcIPIndex);
                    if (srcIP == vmIP && flow.isNew()) {
                        final PartitionOVSData partitionOVSData = partitionData.get(entry.getKey());
                        partitionOVSData.flowsFromSwitch.put(src, partitionOVSData.flowsFromSwitch.get(src) - 1);
                        Integer fromNewSrcFlows = partitionOVSData.flowsFromSwitch.get(dst);
                        if (fromNewSrcFlows == null) {
                            fromNewSrcFlows = 0;
                        }
//                        System.out.println("src: "+flow+" "+src+" to "+dst);
                        partitionOVSData.flowsFromSwitch.put(dst, fromNewSrcFlows + 1);
                    }
                    final Long dstIP = flow.getProperty(dstIPIndex);
                    if (dstIP == vmIP && ruleFlowsMap.getKey().getAction().doAction(flow) != null) {
                        if (flow.isNew()) {
                            final PartitionOVSData partitionOVSData = partitionData.get(entry.getKey());
                            partitionOVSData.flowsToSwitch.put(src, partitionOVSData.flowsToSwitch.get(src) - 1);
                            Integer toNewDstFlows = partitionOVSData.flowsToSwitch.get(dst);
                            if (toNewDstFlows == null) {
                                toNewDstFlows = 0;
                            }
//                        System.out.println("dst: "+flow+" "+src+" to "+dst);
                            partitionOVSData.flowsToSwitch.put(dst, toNewDstFlows + 1);
                        }
                    }
                }
            }
        }
    }


    private class PartitionOVSData {
        private final Map<Switch, Integer> flowsToSwitch;
        private final Map<Switch, Integer> flowsFromSwitch;
        private final Set<Long> wildcards;
        private final Long fwWildcard;

        private PartitionOVSData(Partition partition) throws UnalignedRangeException {
            flowsToSwitch = new HashMap<>();
            flowsFromSwitch = new HashMap<>();
            wildcards = new HashSet<>();
            fwWildcard = partition.getWildcardPattern();
        }

        public Long getFwWildcard() {
            return fwWildcard;
        }

        public void addWildCards(Collection<Rule> rules) throws UnalignedRangeException {
            for (Rule rule : rules) {
                wildcards.add(rule.getWildCardBitPattern());
            }
        }

        public void fillFlowsToSwitch(Map<Rule, Collection<Flow>> flows) {
            for (Map.Entry<Rule, Collection<Flow>> entry : flows.entrySet()) {
                for (Flow flow : entry.getValue()) {
                    if (flow.isNew()) {
                        {
                            final Switch src = flow.getSource();
                            Integer value = flowsFromSwitch.get(src);
                            if (value == null) {
                                value = 0;
                            }
                            flowsFromSwitch.put(src, value + 1);
                        }
                        {
                            if (entry.getKey().getAction().doAction(flow) != null) {
                                final Switch dst = flow.getDestination();
                                Integer value = flowsToSwitch.get(dst);
                                if (value == null) {
                                    value = 0;
                                }
                                flowsToSwitch.put(dst, value + 1);
                            }
                        }
                    }
                }
            }
        }

        public Set<Long> getWildcards() {
            return wildcards;
        }

        public int getFlowsTo(Switch dst) {
            final Integer output = flowsToSwitch.get(dst);
            if (output == null) {
                return 0;
            }
            return output;
        }

        public int getFlowsFrom(Switch src) {
            final Integer output = flowsFromSwitch.get(src);
            if (output == null) {
                return 0;
            }
            return output;
        }
    }


    @Override
    public void init() {

        Thread[] threads = new Thread[threadNum];
        final Iterator<Partition> iterator = partitionData.keySet().iterator();
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            Partition partition;
                            synchronized (iterator) {
                                if (iterator.hasNext()) {
                                    partition = iterator.next();
                                } else {
                                    break;
                                }
                            }
                            final PartitionOVSData partitionOVSData = partitionData.get(partition);
                            partitionOVSData.addWildCards(partition.getRules());
                            partitionOVSData.fillFlowsToSwitch(ruleFlowMap.get(partition));
                        }
                    } catch (UnalignedRangeException e) {
                        e.printStackTrace();
                    }
                }
            };
        }
        Util.runThreads(threads);
    }
}
