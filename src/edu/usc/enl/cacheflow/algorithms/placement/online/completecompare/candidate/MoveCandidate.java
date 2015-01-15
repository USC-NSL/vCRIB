package edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate;

import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.AbstractSwitch;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.link.AbstractLink;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/21/12
 * Time: 10:18 AM
 * To change this template use File | Settings | File Templates.
 */


public class MoveCandidate extends AbstractCandidate {

    final protected long trafficChange;
    final protected long otherSideTrafficChange;
    final protected Collection<Flow> newFlows;
    final protected Collection<Flow> newFlowsOtherSide;
    final protected AbstractLink link;
    final protected Partition partition;


    public MoveCandidate(AbstractLink link, Partition partition,
                         Collection<Flow> newF3, Collection<Flow> newF4, long oldF3Traffic,
                         long oldF4Traffic, long newF3Traffic, long newF4Traffic) {
        super(oldF3Traffic + oldF4Traffic - newF3Traffic - newF4Traffic);
        this.link = link;
        this.partition = partition;
        newFlows = newF3;
        newFlowsOtherSide = newF4;
        trafficChange = newF3Traffic - oldF3Traffic;
        otherSideTrafficChange = newF4Traffic - oldF4Traffic;
    }

    public AbstractLink getLink() {
        return link;
    }

    public long getTrafficChange() {
        return trafficChange;
    }

    public long getOtherSideTrafficChange() {
        return otherSideTrafficChange;
    }

    public Collection<Flow> getNewFlows() {
        return newFlows;
    }

    public Collection<Flow> getNewFlowsOtherSide() {
        return newFlowsOtherSide;
    }

    @Override
    public String toString() {
        return benefit + " - " + link.toString()+ " - "+ partition.hashCode();

    }

    public boolean run(Map<Partition, Switch> switchPartitionMap,
                                       Map<Partition, Set<AbstractSwitch>> partitionSources,
                                       Map<Partition, Collection<Flow>> partitionTraffic,
                                       Map<Partition, Collection<Flow>> denyFlows, List<AbstractCandidate> newCandidates) throws Exception {
        final AbstractSwitch currentSwitch = link.getStart();

        if (!currentSwitch.hasPartition(partition)) {
            //the scheduled partition is not there anymore!
            //System.out.println(partition.hashCode()+ " not in "+currentSwitch);
            return false;
        }

        ///////////////////////////////////////////////////////////////////if move is feasible
        /*

        final AbstractSwitch.MoveState moveState = newHost.movePartitionFeasible(partition,
                switchPartitionMap.get(partition),
                link, trafficChange, otherSideTrafficChange, null, null);*/
        //TODO check all sources of traffics, change the forwarding rule and reagregate for feasibility
        final Switch newSwitchHost = link.move(partition, partitionTraffic.get(partition), this,
                switchPartitionMap.get(partition), partitionSources.get(partition));
        if (newSwitchHost != null) {

            switchPartitionMap.put(partition, newSwitchHost);
            //clean datastructures in old overhead
            currentSwitch.removePartition(partition);
            ///////////////////////////////////////////////////////// create new candidates based on new location
            final AbstractSwitch newHost = link.getEnd();
            newHost.addPartition(partition, partitionTraffic.get(partition), denyFlows.get(partition));
            newCandidates.add(newHost.getBestCandidateForPartition(partition));
            return true;
        }
        newCandidates.add(currentSwitch.getBestCandidateForPartition(partition));
        return false;
    }
}


