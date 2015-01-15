package edu.usc.enl.cacheflow.algorithms.placement.partitionselection;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 10/5/11
 * Time: 9:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class RandomPartitionOrder extends PartitionSorter {
    public Partition nextPartition(List<Partition> partitionSpace, Map<Partition, Switch> assignment) {
        return partitionSpace.get(0);
    }

    @Override
    public String toString() {
        return "Random Order";
    }

    @Override
    public LinkedList<Partition> getSortedPartitionSpace(Collection<Partition> partitions) {
        final LinkedList<Partition> shuffled = new LinkedList<Partition>(partitions);
        Collections.shuffle(shuffled);

        return shuffled;
    }
}
