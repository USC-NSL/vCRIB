package edu.usc.enl.cacheflow.algorithms.placement.switchselectioncluster;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Cluster;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 10/5/11
 * Time: 8:59 PM
 */
public abstract class SwitchSelectionCluster {
    protected Topology topology;

    public abstract  <T extends List<Switch>> T sortSwitches(T toFill, Map<Cluster, Switch> assignment,
                                                    Cluster cluster);

    public void init(Topology topology) {
        this.topology = topology;
    }

    @Override
    public abstract String toString();

}
