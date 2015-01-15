package edu.usc.enl.cacheflow.algorithms.placement.partitionselection;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 10/5/11
 * Time: 8:59 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class PartitionSorter {
    protected Topology topology;


    public void init(Topology topology) {
        this.topology = topology;
    }

    @Override
    public abstract String toString();

    public abstract LinkedList<Partition> getSortedPartitionSpace(Collection<Partition> partitions);
}
