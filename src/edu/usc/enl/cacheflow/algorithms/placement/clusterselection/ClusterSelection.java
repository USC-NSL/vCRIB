package edu.usc.enl.cacheflow.algorithms.placement.clusterselection;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Cluster;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/8/12
 * Time: 10:24 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class ClusterSelection {
    protected Topology topology;


    public void init(Topology topology) {
        this.topology = topology;
    }

    @Override
    public abstract String toString();

    public abstract LinkedList<Cluster> getSortedPartitionSpace(Collection<Cluster> partitions);
}