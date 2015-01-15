package edu.usc.enl.cacheflow.algorithms.replication;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.ForwardAction;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.algorithms.PostPlacer;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/11/12
 * Time: 12:55 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Replicator implements PostPlacer {
    protected final Map<Partition, Map<Switch, Rule>> forwardingRules;
    protected final Topology topology;
    protected Map<Partition, Long> minTraffics;
    protected Map<Partition, Collection<Switch>> partitionSources;

    public Replicator(Map<Partition, Long> minTraffics, Topology topology,
                      Map<Partition, Collection<Switch>> partitionSources,
                      Map<Partition, Map<Switch, Rule>> forwardingRules) {
        this.minTraffics = minTraffics;
        this.topology = topology;
        this.partitionSources = partitionSources;
        this.forwardingRules = forwardingRules;
    }

    protected void pruneSwitches(Collection<Switch> availableSwitches) {
        //prune switches
        Iterator<Switch> iterator = availableSwitches.iterator();
        while (iterator.hasNext()) {
            Switch availableSwitch = iterator.next();
            if (!availableSwitch.canSaveMore()) {
                iterator.remove();
            }
        }
    }

    public static void updateForwardingRules(Map<Partition, Map<Switch, Switch>> partitionSourceReplica, Map<Partition, Map<Switch, Rule>> forwardingRules) {
        //update forwarding rules
        for (Map.Entry<Partition, Map<Switch, Rule>> entry : forwardingRules.entrySet()) {
            final Map<Switch, Switch> switchReplicaMap = partitionSourceReplica.get(entry.getKey());
            for (Map.Entry<Switch, Rule> entry2 : entry.getValue().entrySet()) {
                entry2.getValue().setAction(new ForwardAction(switchReplicaMap.get(entry2.getKey())));
            }
        }
    }
}
