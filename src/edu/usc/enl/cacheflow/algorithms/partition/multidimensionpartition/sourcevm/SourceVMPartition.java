package edu.usc.enl.cacheflow.algorithms.partition.multidimensionpartition.sourcevm;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/6/12
 * Time: 7:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class SourceVMPartition {

    public Collection<Partition> partition(Collection<Rule> rules, Collection<Long> vms, Collection<Rule> partitionRulesTemplate) {
        List<RangeDimensionRange> template = new ArrayList<RangeDimensionRange>();
        for (DimensionInfo dimensionInfo : Util.getDimensionInfos()) {
            template.add(dimensionInfo.getDimensionRange());
        }
        int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        TreeMap<Long, Partition> vmPartitionMap = new TreeMap<Long, Partition>(); // MUST BE A MAP, AND TREEMAP DOES THE BINARY SEARCH FOR US
        for (Long vm : vms) {
            List<RangeDimensionRange> ranges = new ArrayList<RangeDimensionRange>(template);
            ranges.set(srcIPIndex, new RangeDimensionRange(vm, vm, Util.SRC_IP_INFO));
            vmPartitionMap.put(vm, new Partition(Util.getNewCollectionInstance(partitionRulesTemplate), ranges));
        }
        //long sumAffected = 0;
        //int index = 0;
        for (Rule rule : rules) {
            RangeDimensionRange srcIPRange = rule.getProperty(srcIPIndex);
            NavigableMap<Long, Partition> affectedVMs = vmPartitionMap.subMap(srcIPRange.getStart(), true, srcIPRange.getEnd(), true);
            //System.out.println((1.0 * index / rules.size()) + " ("+index+"): sum: " + sumAffected + " affected partitions: " + affectedVMs.size());
            //sumAffected += affectedVMs.size();
            for (Partition partition : affectedVMs.values()) {
                partition.getRules().add(rule);
            }
            //index++;
        }

        /* for (Long vm : vms) {
            List<Rule> partitionRules = new LinkedList<Rule>();
            for (Rule rule : rules) {
                if (rule.getProperty(srcIPIndex).getStart() <= vm && rule.getProperty(srcIPIndex).getEnd() >= vm) {
                    partitionRules.add(rule);
                }
            }
            List<RangeDimensionRange> ranges = new ArrayList<RangeDimensionRange>(template);
            ranges.set(srcIPIndex, new RangeDimensionRange(vm, vm, Util.SRC_IP_INFO));
            partitions.add(new Partition(partitionRules, ranges));
        }*/
        return vmPartitionMap.values();
    }

    @Override
    public String toString() {
        return "VMStart";
    }
}
