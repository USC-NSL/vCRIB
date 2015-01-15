package edu.usc.enl.cacheflow.model.topology.switchhelper;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.CollectionPool;
import edu.usc.enl.cacheflow.util.Util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/22/12
 * Time: 5:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class MemorySwitchHelper extends SwitchHelper<MemorySwitch> {
    private final Map<Partition, Map<Switch, Rule>> forwardingRules;
    private CollectionPool<Set<Rule>> ruleSetPool;

    public MemorySwitchHelper(Map<Partition, Map<Switch, Rule>> forwardingRules) {
        this.forwardingRules = forwardingRules;
    }

    @Override
    public double resourceUsage(Switch host, Partition newPartition, Collection<Switch> sources, boolean checkResources)
            throws Switch.InfeasibleStateException {
        final int memoryCapacity = ((MemorySwitch) host).getMemoryCapacity();
        MemorySwitch.MemoryState memoryState = (MemorySwitch.MemoryState) host.getState();
        if (memoryCapacity == 0) {
            if (newPartition.getRules().size() == 0 && memoryState.getRules().size() == 0) {
                return 0;
            } else {
                throw new Switch.InfeasibleStateException("Zero capacity in switch " + host);
            }
        }

        final CollectionPool.TempCollection<Set<Rule>> tempRules = ruleSetPool.getTempCollection();
        Set<Rule> tempRuleSet = tempRules.getData();
        tempRuleSet.addAll(memoryState.getRules());
        tempRuleSet.addAll(newPartition.getRules());
        tempRuleSet.remove(forwardingRules.get(newPartition).get(host));

        final int ruleSize = tempRuleSet.size();
        tempRules.release();
        final double memoryUsageRatio = (float) (1.0 * ruleSize / memoryCapacity);
        if (memoryUsageRatio > 1 && checkResources) {
            throw new Switch.InfeasibleStateException(ruleSize + " Memory usage in " + host + " with capacity " + memoryCapacity);
        }
        return memoryUsageRatio;
    }

    @Override
    public void init() {

    }

    public void setRuleSetPool(CollectionPool<Set<Rule>> ruleSetPool) {
        this.ruleSetPool = ruleSetPool;
    }

    @Override
    public Switch.FeasibleState isAddFeasible(MemorySwitch host,
                                              Partition newPartition, Collection<Switch> sources,
                                              boolean checkResources, boolean selfCommit) throws Switch.InfeasibleStateException {
        final CollectionPool.TempCollection<Set<Rule>> tempSet = ruleSetPool.getTempCollection();
        Set<Rule> tempRuleSet = tempSet.getData();
        tempRuleSet.addAll(((MemorySwitch.MemoryState) host.getState()).getRules());
        tempRuleSet.addAll(newPartition.getRules());
        tempRuleSet.remove(forwardingRules.get(newPartition).get(host));

        try {
            return isFeasible(host, checkResources, selfCommit, tempSet.getData());
        } finally {
            tempSet.release();
        }
    }

    private Switch.FeasibleState isFeasible(MemorySwitch host, boolean checkResources, boolean selfCommit,
                                            Set<Rule> tempRuleSet) throws Switch.InfeasibleStateException {
        final int size = tempRuleSet.size();
        if (size <= host.getMemoryCapacity() || !checkResources) {
            Switch.FeasibleState output;
            final MemorySwitch.MemoryState currentState = (MemorySwitch.MemoryState) host.getState();
            if (selfCommit) {
                currentState.fillRules(tempRuleSet);
                output = currentState;
            } else {
                final Set<Rule> ruleSet = Util.getNewCollectionInstance(tempRuleSet);
                ruleSet.addAll(tempRuleSet);
                output = new MemorySwitch.MemoryState(ruleSet);
            }
            return output;
        } else {
            throw new Switch.InfeasibleStateException(size + " Memory usage in " + host + " with capacity " + host.getMemoryCapacity());
        }
    }

    @Override
    public Switch.FeasibleState isRemoveFeasible(MemorySwitch host, Map<Partition, Collection<Switch>> currentPartitions,
                                                 Collection<Partition> sourcePartitions,
                                                 boolean checkResources, boolean selfCommit, Partition removedPartition, Collection<Switch> sources) throws Switch.InfeasibleStateException {
        final CollectionPool.TempCollection<Set<Rule>> tempRules = ruleSetPool.getTempCollection();
        Set<Rule> tempRuleSet = tempRules.getData();
        Set<Partition> currentPartitionsSet = null;
        if (currentPartitions != null) {
            currentPartitionsSet = currentPartitions.keySet();
            for (Partition currentPartition : currentPartitionsSet) {
                if (!currentPartition.equals(removedPartition)) {
                    tempRuleSet.addAll(currentPartition.getRules());
                }
            }
        }
        if (sourcePartitions != null) {
            for (Partition sourcePartition : sourcePartitions) {
                if (currentPartitions == null || !currentPartitionsSet.contains(sourcePartition) ||
                        sourcePartition.equals(removedPartition)) {
                    tempRuleSet.add(forwardingRules.get(sourcePartition).get(host));
                }
            }
        }

        try {
            return isFeasible(host, checkResources, selfCommit, tempRules.getData());
        } finally {
            tempRules.release();
        }
    }

    @Override
    public void resetRuleCollections(MemorySwitch aSwitch) throws Switch.InfeasibleStateException {
        final CollectionPool.TempCollection<Set<Rule>> tempRules = ruleSetPool.getTempCollection();
        try {
            aSwitch.setState(isFeasible(aSwitch, true, true, tempRules.getData()));
        } finally {
            tempRules.release();
        }
    }

    @Override
    public Switch.FeasibleState initToNotOnSrc(MemorySwitch host, Collection<Partition> sourcePartitions, boolean selfCommit) throws Switch.InfeasibleStateException {
        final CollectionPool.TempCollection<Set<Rule>> tempRules = ruleSetPool.getTempCollection();
        Set<Rule> tempRuleSet = tempRules.getData();
        if (sourcePartitions != null) {
            for (Partition sourcePartition : sourcePartitions) {
                tempRuleSet.add(forwardingRules.get(sourcePartition).get(host));
            }
        }

        try {
            return isFeasible(host, true, selfCommit, tempRules.getData());
        } finally {
            tempRules.release();
        }
    }

    @Override
    public void migrate(Switch src, Switch dst, Partition partition) {

    }
}
