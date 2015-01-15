package edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate;

import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.AbstractSwitch;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/23/12
 * Time: 12:10 AM
 * To change this template use File | Settings | File Templates.
 */
public class ReplaceCandidate extends AbstractCandidate {

    protected MoveCandidate moveCandidate1;
    protected MoveCandidate moveCandidate2;

    public ReplaceCandidate(MoveCandidate moveCandidate1, MoveCandidate moveCandidate2) {
        super(moveCandidate1.benefit + moveCandidate2.benefit);
        this.moveCandidate1 = moveCandidate1;
        this.moveCandidate2 = moveCandidate2;
    }

    public MoveCandidate getMoveCandidate1() {
        return moveCandidate1;
    }

    public MoveCandidate getMoveCandidate2() {
        return moveCandidate2;
    }

    public long getTrafficChange() {
        return moveCandidate1.getTrafficChange() + moveCandidate2.getOtherSideTrafficChange();
    }

    public long getOtherSideTrafficChange() {
        return moveCandidate1.getOtherSideTrafficChange() + moveCandidate2.getTrafficChange();
    }

    public Collection<Flow> getNewFlows() {
        final LinkedList<Flow> flows = new LinkedList<Flow>(moveCandidate1.getNewFlows());
        flows.addAll(moveCandidate2.getNewFlowsOtherSide());
        return flows;
    }

    public Collection<Flow> getNewFlowsOtherSide() {
        final LinkedList<Flow> flows = new LinkedList<Flow>(moveCandidate2.getNewFlows());
        flows.addAll(moveCandidate1.getNewFlowsOtherSide());
        return flows;
    }

    @Override
    public boolean run(Map<Partition, Switch> switchPartitionMap, Map<Partition, Set<AbstractSwitch>> partitionSources,
                                       Map<Partition, Collection<Flow>> partitionTraffic, Map<Partition, 
            Collection<Flow>> denyFlows, List<AbstractCandidate> newCandidates) throws Exception {

        final AbstractSwitch host1 = moveCandidate1.link.getStart();
        final AbstractSwitch host2 = moveCandidate1.link.getEnd();
        final Partition partition1 = moveCandidate1.partition;
        final Partition partition2 = moveCandidate2.partition;
        if (!host1.hasPartition(partition1)) {
            //System.out.println(partition1.hashCode()+ " not in "+host1);
            return false;
        } else {
            if (!host2.hasPartition(partition2)) {
                //System.out.println(partition2.hashCode()+ " not in "+host2);
                newCandidates.add(host1.getBestCandidateForPartition(partition1));
                return false;
            }
        }
        ////////////////////////////////////////////////////////////if replace is feasible
        final Switch[] replace = moveCandidate1.link.replace(partition1, partition2, partitionSources.get(partition1),
                partitionSources.get(partition2), switchPartitionMap.get(partition1), switchPartitionMap.get(partition2),
                partitionTraffic.get(partition1), partitionTraffic.get(partition2), this);
        if (replace != null) {
            //do replace
            newCandidates.addAll(updateDataStructures(switchPartitionMap, partitionTraffic, denyFlows, replace));
            return true;
        } else {
            //find another replace candidate
            newCandidates.add(host1.getBestCandidateForPartition(partition1));
            return false;
        }


    }

    private List<AbstractCandidate> updateDataStructures(Map<Partition, Switch> switchPartitionMap,
                                                         Map<Partition, Collection<Flow>> partitionTraffic,
                                                         Map<Partition, Collection<Flow>> denyFlows, Switch[] replace) {
        //get partitions again as movecandidates may have been exchanged.
        final Partition partition1 = moveCandidate1.partition;
        final Partition partition2 = moveCandidate2.partition;
        final AbstractSwitch host1 = moveCandidate1.link.getStart();
        final AbstractSwitch host2 = moveCandidate1.link.getEnd();
        //update mapping
        //System.out.println(partition1.hashCode()+" to "+replace[0]);
       // System.out.println(partition2.hashCode()+" to "+replace[1]);
        switchPartitionMap.put(partition1, replace[0]);
        switchPartitionMap.put(partition2, replace[1]);

        //clean state on the previous switches
        host1.removePartition(partition1);
        host2.removePartition(partition2);
        //create new candidates
        host2.addPartition(partition1, partitionTraffic.get(partition1), denyFlows.get(partition1));
        host1.addPartition(partition2, partitionTraffic.get(partition2), denyFlows.get(partition2));
        final AbstractCandidate bestCandidateForPartition1 = host2.getBestCandidateForPartition(partition1);
        final AbstractCandidate bestCandidateForPartition2 = host1.getBestCandidateForPartition(partition2);
        List<AbstractCandidate> outputCandidates = new ArrayList<AbstractCandidate>();
        //if (bestCandidateForPartition1 != null) {
            outputCandidates.add(bestCandidateForPartition1);
        //}
        //if (bestCandidateForPartition2 != null) {
            outputCandidates.add(bestCandidateForPartition2);
//
//       }
        return outputCandidates;
    }


    @Override
    public String toString() {
        return benefit + " -replace- " + moveCandidate1 + " and " + moveCandidate2;
    }

    public ReplaceCandidate reverse() {
        MoveCandidate temp = moveCandidate1;
        moveCandidate1 = moveCandidate2;
        moveCandidate2 = temp;
        return this;
    }
}
