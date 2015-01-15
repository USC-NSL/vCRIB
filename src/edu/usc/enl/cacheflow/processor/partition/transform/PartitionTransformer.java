package edu.usc.enl.cacheflow.processor.partition.transform;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.factory.AssignmentFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.model.rule.action.Action;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.rule.generator.PrefixRandomRuleGenerator;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePlacementScript;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 10/30/12
 * Time: 7:52 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class PartitionTransformer {

    public abstract void transform(Random random, int changesNum, List<Partition> partitions, Map<Switch, Collection<Partition>> sourcePartitions,
                                   Topology topology) throws NoAssignmentFoundException, IncompleteTransformException;

    protected Rule getANewRule(Random random, List<RangeDimensionRange> template, RangeDimensionRange srcIPRange, int priority,
                               Action action) {
        List<RangeDimensionRange> properties = new ArrayList<>(template);
        int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        properties.set(srcIPIndex, srcIPRange);
        setProperty(random, properties, Util.DST_IP_INFO);
        setProperty(random, properties, Util.DST_PORT_INFO);
        setProperty(random, properties, Util.SRC_PORT_INFO);
        setProperty(random, properties, Util.PROTOCOL_INFO);
        return new Rule(action, properties, priority, Rule.maxId + 1);
    }

    public Switch getMaxSwitch(Map<Switch, Double> vmStartAssignmentLoad) {
        double maximumValue = -0;
        Switch maximumSwitch = null;
        for (Map.Entry<Switch, Double> entry : vmStartAssignmentLoad.entrySet()) {
            if (entry.getValue() > maximumValue || maximumSwitch == null) {
                maximumSwitch = entry.getKey();
                maximumValue = entry.getValue();
            }
        }
        return maximumSwitch;
    }

    public static Map<Switch, Double> getVMStartAssignmentLoad(Collection<Partition> partitions, Map<Switch, Collection<Partition>> sourcePartitions,
                                                        Topology topology, Assignment assignment) throws NoAssignmentFoundException {

        Map<Partition, Map<Switch, Rule>> forwardingRules = MultiplePlacementScript.createForwardingRules(partitions, topology, sourcePartitions);
        if (assignment == null) {
            assignment = Assignment.getAssignment(sourcePartitions);
        }
        new AssignmentFactory.LoadPlacer(false, forwardingRules, assignment, sourcePartitions).place(topology, partitions);

        Map<Switch, Double> output = new HashMap<>(sourcePartitions.size());
        for (Switch source : sourcePartitions.keySet()) {
            output.put(source, source.getUsedAbsoluteResources(source.getState()));
        }
        return output;
    }

    protected TreeMap<Long, Partition> getSrcIPPartitionMap(Collection<Partition> partitions, int srcIPIndex) {
        TreeMap<Long, Partition> srcIPPartitionMap = new TreeMap<>();
        for (Partition partition : partitions) {
            srcIPPartitionMap.put(partition.getProperty(srcIPIndex).getStart(), partition);
        }
        return srcIPPartitionMap;
    }

    protected int getMaxPriority(Collection<Partition> partitions) {
        int maxPriority = -1;
        boolean first = true;
        for (Partition partition : partitions) {
            for (Rule rule : partition.getRules()) {
                if (rule.getPriority() > maxPriority || first) {
                    first = false;
                    maxPriority = rule.getPriority();
                }
            }
        }
        return maxPriority;

    }

    protected void setProperty(Random random, List<RangeDimensionRange> properties, DimensionInfo info) {
        properties.set(Util.getDimensionInfoIndex(info), PrefixRandomRuleGenerator.getRandomRange(random,
                Util.getDimensionInfos().get(Util.getDimensionInfoIndex(info))));
    }

    protected void hasPartitionIn(Map<Long, Partition> subMap,
                                  Map<Partition, Switch> placement,
                                  Set<Switch> toFill) {
        //NavigableMap<Long, Partition> subMap = srcIPPartitionMap.subMap(range.getStart(), true, range.getEnd(), true);
        for (Partition partition : subMap.values()) {
            toFill.add(placement.get(partition));
        }
    }
}
