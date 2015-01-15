package edu.usc.enl.cacheflow.algorithms.replication.switchselectioncluster;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Cluster;
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
public abstract class SwitchReplicateSelectionCluster {
    protected Topology topology;

    public abstract List<Switch> sortSwitches(Collection<Switch> availableToChoose, Cluster cluster, Collection<Switch> sources,
                                              Collection<Switch> oldRepicas);

    public void init( Topology topology,
                     Collection<Switch> initialSetOfSwitches) {
        this.topology = topology;
    }

    @Override
    public abstract String toString();

    public abstract Map<Switch, Switch> getSelectedReplicaIf(Switch replica);

    public abstract Long getTrafficIf(Switch replica);


    protected static void fillNearestReplica(Collection<Switch> sources, List<Switch> replicas, Map<Switch, Switch>
            sourceReplicaMap,Topology topology) {
        //Collections.shuffle(replicas, Util.random); DON'T DO THIS AS WE DON'T KNOW WHERE WAS THE LATEST REPLCIA ADDED
        for (Switch source : sources) {
            int minPathLength = -1;
            Switch nearestReplica = null;
            for (Switch replica : replicas) {
                int pathLength = topology.getPathLength(source, replica);
                if (minPathLength < 0 || pathLength < minPathLength) {
                    nearestReplica = replica;
                }
            }
            sourceReplicaMap.put(source, nearestReplica);
        }
    }
}
