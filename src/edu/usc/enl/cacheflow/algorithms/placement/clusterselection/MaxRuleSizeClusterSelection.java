package edu.usc.enl.cacheflow.algorithms.placement.clusterselection;

import edu.usc.enl.cacheflow.model.rule.Cluster;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/8/12
 * Time: 10:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class MaxRuleSizeClusterSelection extends ClusterSelection {
    @Override
    public String toString() {
        return "Max Rule Size";
    }

    @Override
    public LinkedList<Cluster> getSortedPartitionSpace(Collection<Cluster> clusters) {
        final Map<Cluster, Integer> cachedPartitionTraffic = new HashMap<Cluster, Integer>();
        for (Cluster cluster : clusters) {
            //update cache: calculate partitions traffic

            cachedPartitionTraffic.put(cluster, cluster.getRulesNum());
        }

        LinkedList<Cluster> sortedList = new LinkedList<Cluster>(clusters);
        Collections.sort(sortedList, new Comparator<Cluster>() {
            public int compare(Cluster o1, Cluster o2) {
                return (int) (cachedPartitionTraffic.get(o2) - cachedPartitionTraffic.get(o1));
            }
        });
        return sortedList;
    }
}
