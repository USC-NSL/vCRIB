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
 class AddRemoveExploration extends Exploration {
    private final PartitionTransformer breakTransform;
    private final PartitionTransformer addSmallRulesTransform;
    private final PartitionTransformer removeSmallRulesTransform;
    private final int similarityResolution;

    private int changeNum;
    private Action lastAction;
    private boolean seenBound;
    private boolean cannotAddMore;

    private Topology topology;
    private List<Partition> partitions;
    private Map<Switch, Collection<Partition>> sourcePartitions;

    public AddRemoveExploration(Topology topology, List<Partition> partitions, Map<Switch, Collection<Partition>> sourcePartitions,
                                int initChangeNum, PartitionTransformer breakTransform, PartitionTransformer addSmallRulesTransform,
                                PartitionTransformer removeSmallRulesTransform, Action initLastAction, int similarityResolution,
                                boolean initSeenBound) {
        this.topology=topology;
        this.partitions=partitions;
        this.sourcePartitions=sourcePartitions;
        this.changeNum = initChangeNum;
        this.breakTransform = breakTransform;
        this.addSmallRulesTransform = addSmallRulesTransform;
        this.removeSmallRulesTransform = removeSmallRulesTransform;
        this.lastAction = initLastAction;
        this.similarityResolution = similarityResolution;
        this.seenBound = initSeenBound;
        this.cannotAddMore = false;
    }

    public int explore(int tryNum, boolean feasible) throws NoAssignmentFoundException, IncompleteTransformException {

        if (feasible) {
            if (!lastAction.equals(Action.NULL)) {
                if (!(lastAction.equals(Action.ADD) || lastAction.equals(Action.BREAK))) {
                    System.out.println(tryNum + " crossed bound after " + lastAction);
                }

                if (seenBound) {
                    changeNum /= 2;
                } else if (lastAction.equals(Action.ADD)) {
                    changeNum *= 2;
                } else {//crossed the bound for the first time

                    changeNum /= 2;
                    seenBound = true;
                }
            }
            if (cannotAddMore) {
                lastAction = Action.BREAK;
                try {
                    cannotAddMore = false;
                    breakTransform.transform(Util.random, similarityResolution, partitions, sourcePartitions, topology);
                } catch (IncompleteTransformException e) {
                    throw new IncompleteTransformException(tryNum + ": cannot add or break", e);
                }
            } else {
                lastAction = Action.ADD;
                //// add rule
                try {
                    addSmallRulesTransform.transform(Util.random, changeNum, partitions, sourcePartitions, topology);
                } catch (IncompleteTransformException e) {
                    //cannot add more rules
                    System.out.println(tryNum + ": Cannot add more switch to break (" + e.getMessage() + ")");
                    //break rules in the next step
                    cannotAddMore = true;
                }
            }
            System.out.println(tryNum + ": " + lastAction + " " + changeNum);
        } else {
            if (!lastAction.equals(Action.NULL)) {
                if (!lastAction.equals(Action.REMOVE)) {
                    System.out.println(tryNum + " crossed bound after " + lastAction);
                }

                if (seenBound) {
                    changeNum /= 2;
                } else if (lastAction.equals(Action.REMOVE)) {
                    changeNum *= 2;
                } else {//crossed the bound for the first time
                    changeNum /= 2;
                    seenBound = true;
                }
            }
            cannotAddMore = false;
            lastAction = Action.REMOVE;
            System.out.println(tryNum + ": " + lastAction + " " + changeNum);
            //// remove rules
            removeSmallRulesTransform.transform(Util.random, changeNum, partitions, sourcePartitions, topology);
        }
        return changeNum;
    }
}
