package edu.usc.enl.cacheflow.algorithms.placement.partitionselection;

import edu.usc.enl.cacheflow.model.rule.Partition;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/2/12
 * Time: 7:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class MaxSizePartitionSorter extends PartitionSorter {

    @Override
    public LinkedList<Partition> getSortedPartitionSpace(Collection<Partition> partitions) {
        final Map<Partition,Integer> cachedPartitionTraffic = new HashMap<Partition, Integer>();
        for (Partition partition : partitions) {
            //update cache: calculate partitions traffic

            cachedPartitionTraffic.put(partition, partition.getRules().size());
        }
        LinkedList<Partition> sortedList = new LinkedList<Partition>(partitions);
        Collections.sort(sortedList, new Comparator<Partition>() {
            public int compare(Partition o1, Partition o2) {
                return (int) (cachedPartitionTraffic.get(o2) - cachedPartitionTraffic.get(o1));
            }
        });
        return sortedList;
    }


    @Override
    public String toString() {
        return "Max Size";
    }
}
