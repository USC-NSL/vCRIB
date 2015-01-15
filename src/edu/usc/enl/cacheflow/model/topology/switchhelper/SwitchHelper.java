package edu.usc.enl.cacheflow.model.topology.switchhelper;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.OVSSwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/22/12
 * Time: 5:59 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class SwitchHelper<T extends Switch> {


    public abstract double resourceUsage(Switch host, Partition newPartition, Collection<Switch> sources, boolean checkResources)
            throws Switch.InfeasibleStateException;

    public abstract void init();


    public abstract Switch.FeasibleState isAddFeasible(T host,
                                                       Partition newPartition, Collection<Switch> sources,
                                                       boolean checkResources, boolean selfCommit) throws Switch.InfeasibleStateException;

    public abstract Switch.FeasibleState isRemoveFeasible(T host, Map<Partition, Collection<Switch>> currentPartitions,
                                                          Collection<Partition> sourcePartitions,
                                                          boolean checkResources, boolean selfCommit, Partition removedPartition,
                                                          Collection<Switch> sources) throws Switch.InfeasibleStateException;

    public abstract void resetRuleCollections(T aSwitch) throws Switch.InfeasibleStateException;

    public abstract Switch.FeasibleState initToNotOnSrc(T aSwitch, Collection<Partition> sourcePartitions, boolean selfCommit) throws Switch.InfeasibleStateException;

    public abstract void migrate(Switch src, Switch dst, Partition partition);


    public Switch.FeasibleState isAddMultipleFeasible(T aSwitch, Collection<Partition> toAdd,
                                                      Map<Partition, Map<Switch, Rule>> forwardingRules,
                                                      boolean selfCommit) throws Switch.InfeasibleStateException {
        final Switch.FeasibleState oldState = aSwitch.getState();
        int size = toAdd.size();
        if (size == 0) {
            return oldState;
        }
        int i = 0;
        Iterator<Partition> iterator = toAdd.iterator();
        for (; iterator.hasNext(); ) {
            if (i >= size - 1) {
                break;
            }
            Partition partition1 = iterator.next();
            final Switch.FeasibleState feasibleState = isAddFeasible(aSwitch, partition1, forwardingRules.get(partition1).keySet(), false, selfCommit || i > 0);
            aSwitch.setState(feasibleState);
            i++;
        }
        final Partition lastPartition = iterator.next();
        final Switch.FeasibleState lastState = isAddFeasible(aSwitch, lastPartition, forwardingRules.get(lastPartition).keySet(), true, selfCommit || size > 1);
        if (!selfCommit) {
            aSwitch.setState(oldState);
        }
        return lastState;
    }

}
