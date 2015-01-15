package edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate;

import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.AbstractSwitch;
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
 * Date: 3/23/12
 * Time: 12:10 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractCandidate implements Comparable<AbstractCandidate> {
    final protected long benefit;

    protected AbstractCandidate(long benefit) {
        this.benefit = benefit;
    }

    public int compareTo(AbstractCandidate o) {
        return (int)(o.benefit - benefit);
    }

    public long getBenefit() {
        return benefit;
    }

    public abstract boolean run(Map<Partition, Switch> switchPartitionMap,
                                                Map<Partition, Set<AbstractSwitch>> partitionSources,
                                                Map<Partition, Collection<Flow>> partitionTraffic,
                                                Map<Partition, Collection<Flow>> denyFlows,
                                                List<AbstractCandidate> newCandidates) throws Exception;

}