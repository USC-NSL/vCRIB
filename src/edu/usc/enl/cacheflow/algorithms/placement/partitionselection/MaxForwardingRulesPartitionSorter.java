package edu.usc.enl.cacheflow.algorithms.placement.partitionselection;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/6/12
 * Time: 10:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class MaxForwardingRulesPartitionSorter extends PartitionSorter {
    protected final Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap;

    public MaxForwardingRulesPartitionSorter(Map<Partition, Map<Rule, Collection<Flow>>> ruleFlowMap) {
        this.ruleFlowMap = ruleFlowMap;
    }

    @Override
    public String toString() {
        return "Max Forwarding Rules";
    }

    @Override
    public LinkedList<Partition> getSortedPartitionSpace(Collection<Partition> partitions) {
        final Map<Partition, Integer> cachedPartitionTraffic;
        cachedPartitionTraffic = new HashMap<Partition, Integer>();
        for (Partition partition : partitions) {
            //update cache: calculate partitions traffic
            Set<Switch> sources = new HashSet<Switch>();
            for (Map.Entry<Rule, Collection<Flow>> entry : ruleFlowMap.get(partition).entrySet()) {
                for (Flow flow : entry.getValue()) {
                    sources.add(flow.getSource());
                }
            }
            cachedPartitionTraffic.put(partition, sources.size());
        }
        LinkedList<Partition> sortedList = new LinkedList<Partition>(partitions);
        Collections.sort(sortedList, new Comparator<Partition>() {
            public int compare(Partition o1, Partition o2) {
                return (int) (cachedPartitionTraffic.get(o2) - cachedPartitionTraffic.get(o1));
            }
        });
        return sortedList;
    }

}