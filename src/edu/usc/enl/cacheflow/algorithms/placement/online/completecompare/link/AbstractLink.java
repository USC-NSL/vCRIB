package edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.link;

import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.AbstractSwitch;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate.MoveCandidate;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate.ReplaceCandidate;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/24/12
 * Time: 10:25 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractLink {
    private AbstractSwitch start;
    private AbstractSwitch end;

    protected AbstractLink(AbstractSwitch start, AbstractSwitch end) {
        this.start = start;
        this.end = end;
    }

    public AbstractLink getOtherSide() {
        return end.getLinkTo(start);
    }

    public abstract Collection<Flow> getFlows();


    public AbstractSwitch getStart() {
        return start;
    }

    public AbstractSwitch getEnd() {
        return end;
    }

    public abstract Link getLink(Switch target);


    @Override
    public String toString() {
        return getStart() + "->" + getEnd();
    }

    public abstract Switch move(Partition partition, Collection<Flow> flows, MoveCandidate moveCandidate,
                       Switch oldHost, Collection<AbstractSwitch> partitionSources) throws Exception;


    public abstract Switch[] replace(Partition partition1, Partition partition2, Collection<AbstractSwitch> partitionSources1,
                            Collection<AbstractSwitch> partitionSources2, Switch switch1, Switch switch2,
                            Collection<Flow> flows1, Collection<Flow> flows2, ReplaceCandidate replaceCandidate) throws Exception;
}