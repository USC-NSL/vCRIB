package edu.usc.enl.cacheflow.algorithms.placement.partitionselection;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/31/11
 * Time: 7:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class MaxTrafficPartitionSorter extends PartitionSorter {
    protected final Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap;

    public MaxTrafficPartitionSorter(Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) {
        this.ruleFlowMap = ruleFlowMap;
    }

    @Override
    public String toString() {
        return "Max Traffic";
    }

    @Override
    public LinkedList<Partition> getSortedPartitionSpace(Collection<Partition> partitions) {
        final Map<Partition, Double> cachedPartitionTraffic = new HashMap<Partition, Double>();
        for (Partition partition : partitions) {
            //update cache: calculate partitions traffic
            double traffic = 0;
            for (Map.Entry<Rule, Collection<Flow>> entry : ruleFlowMap.get(partition).entrySet()) {
                for (Flow flow : entry.getValue()) {
                    traffic += flow.getTraffic();
                }
            }
            cachedPartitionTraffic.put(partition, traffic);
        }
        LinkedList<Partition> sortedList = new LinkedList<Partition>(partitions);
        Collections.sort(sortedList,new Comparator<Partition>() {
            public int compare(Partition o1, Partition o2) {
                return (int) (cachedPartitionTraffic.get(o2) - cachedPartitionTraffic.get(o1));
            }
        });
        return sortedList;
    }

}
