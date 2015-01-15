package edu.usc.enl.cacheflow.scripts.vcrib.transform;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.partition.transform.IncompleteTransformException;
import edu.usc.enl.cacheflow.processor.partition.transform.PartitionTransformer;
import edu.usc.enl.cacheflow.util.Util;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 11/1/12
 * Time: 12:01 PM
 * To change this template use File | Settings | File Templates.
 */
 class BreakExtendExploration extends Exploration {

    private final PartitionTransformer breakTransform;
    private final PartitionTransformer extendTransform;

    private int changeNum;
    private Action lastAction;
    private boolean seenBound;

    private Topology topology;
    private List<Partition> partitions;
    private Map<Switch, Collection<Partition>> sourcePartitions;

    public BreakExtendExploration(Topology topology, List<Partition> partitions, Map<Switch, Collection<Partition>> sourcePartitions,
                                  int initChangeNum, PartitionTransformer breakTransform, PartitionTransformer extendTransform,
                                  Action initLastAction, boolean initSeenBound) {
        this.topology=topology;
        this.partitions=partitions;
        this.sourcePartitions=sourcePartitions;

        this.changeNum = initChangeNum;
        this.breakTransform = breakTransform;
        this.extendTransform = extendTransform;
        this.lastAction = initLastAction;
        this.seenBound = initSeenBound;
    }

    public int explore(int tryNum, boolean feasible) throws NoAssignmentFoundException, IncompleteTransformException {

        if (feasible) {
            if (!lastAction.equals(Action.NULL)) {
                if (! lastAction.equals(Action.BREAK)) {
                    System.out.println(tryNum + " crossed bound after " + lastAction);
                }

                if (seenBound) {
                    changeNum /= 2;
                } else if (lastAction.equals(Action.BREAK)) {
                    changeNum *= 2;
                } else {//crossed the bound for the first time

                    changeNum /= 2;
                    seenBound = true;
                }
            }

            lastAction = Action.BREAK;
            //// break rule
            try {
                breakTransform.transform(Util.random, changeNum, partitions, sourcePartitions, topology);
            } catch (IncompleteTransformException e) {
                //cannot add more rules
                System.out.println(tryNum + ": Cannot add more switch to break (" + e.getMessage() + ")");
                //break rules in the next step
            }
            System.out.println(tryNum + ": " + lastAction + " " + changeNum);
        } else {
            if (!lastAction.equals(Action.NULL)) {
                if (!lastAction.equals(Action.EXTEND)) {
                    System.out.println(tryNum + " crossed bound after " + lastAction);
                }

                if (seenBound) {
                    changeNum /= 2;
                } else if (lastAction.equals(Action.EXTEND)) {
                    changeNum *= 2;
                } else {//crossed the bound for the first time
                    changeNum /= 2;
                    seenBound = true;
                }
            }
            lastAction = Action.EXTEND;
            System.out.println(tryNum + ": " + lastAction + " " + changeNum);
            //// extend rules
            extendTransform.transform(Util.random, changeNum, partitions, sourcePartitions, topology);
        }
        return changeNum;
    }
}
