package edu.usc.enl.cacheflow.algorithms.replication.switchselectionpartition;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/9/12
 * Time: 9:02 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class SwitchReplicateSelectionPartition {
    protected Topology topology;
    protected final Map<Partition, Map<Rule, List<Flow>>> ruleFlowMap;

    protected SwitchReplicateSelectionPartition(Map<Partition, Map<Rule, List<Flow>>> ruleFlowMap) {
        this.ruleFlowMap = ruleFlowMap;
    }

    public abstract List<Switch> sortSwitches(Collection<Switch> availableToChoose, Partition partition, Collection<Switch> sources,
                                              Collection<Switch> oldRepicas);

    public void init( Topology topology) {
        this.topology = topology;
    }

    @Override
    public abstract String toString();

    public abstract Map<Switch, Switch> getSelectedReplicaIf(Switch replica);

    public abstract Long getTrafficIf(Switch replica);

}
