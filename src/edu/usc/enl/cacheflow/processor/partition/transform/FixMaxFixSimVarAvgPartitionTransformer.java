package edu.usc.enl.cacheflow.processor.partition.transform;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 10/29/12
 * Time: 12:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class FixMaxFixSimVarAvgPartitionTransformer extends PartitionTransformer{
    public void transform(Random random, int changesNum, List<Partition> partitions, Map<Switch, Collection<Partition>> sourcePartitions,
                          Topology topology) throws NoAssignmentFoundException, IncompleteTransformException {

        final int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        List<Partition> partitions1 = new ArrayList<>(partitions);
        //Collections.sort(partitions1, Partition.getPropertyComparator(Util.getDimensionInfoIndex(Util.SRC_IP_INFO)));

        Assignment assignment = Assignment.getAssignment(sourcePartitions);
        Map<Switch, Double> vmStartAssignmentLoad = getVMStartAssignmentLoad(partitions, sourcePartitions, topology, assignment);
        //find maximum
        Switch maximumSwitch = getMaxSwitch(vmStartAssignmentLoad);
        Double maxLoad = vmStartAssignmentLoad.get(maximumSwitch);
        partitions1.removeAll(sourcePartitions.get(maximumSwitch));
        //know I know max switch
        int changesUntilNow = 0;
        while (changesUntilNow < changesNum && partitions1.size() > 0) {
            //select a random partition
            Partition partition = partitions1.get(random.nextInt(partitions1.size()));
            Switch source = assignment.getPlacement().get(partition);
            if (!source.equals(maximumSwitch)) {
                //check not to create a new max
                double newLoad = vmStartAssignmentLoad.get(source) + 1;
                if (newLoad <= maxLoad) {
                    partition.getRules().add(getANewRule(random, partition.getProperties(),
                            new RangeDimensionRange(partition.getProperty(srcIPIndex).getStart(),
                                    partition.getProperty(srcIPIndex).getStart(), Util.SRC_IP_INFO), (int) (random.nextDouble() * 10000),
                            random.nextBoolean() ? DenyAction.getInstance() : AcceptAction.getInstance()));
                    changesUntilNow++;
                    vmStartAssignmentLoad.put(source, newLoad);
                } else {
                    partitions1.removeAll(sourcePartitions.get(source));
                }
            }
        }

        if (partitions1.size() == 0 && changesUntilNow < changesNum) {
            System.out.println("All machines have equal load to max. Only added " + changesUntilNow + " rules");
            throw new IncompleteTransformException("All machines have equal load to max. Only added " + changesUntilNow + " rules");
        }

    }

    private RangeDimensionRange getRandomRange(Random random, DimensionInfo dimensionInfo) {
        long start = dimensionInfo.getDimensionRange().getRandomNumber(random);
        long end = start - 1;
        while (end < start) {
            end = dimensionInfo.getDimensionRange().getRandomNumber(random);
        }
        return new RangeDimensionRange(start, end, dimensionInfo);
    }

}
