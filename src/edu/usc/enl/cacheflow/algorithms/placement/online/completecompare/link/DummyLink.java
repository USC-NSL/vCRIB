package edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.link;

import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.AbstractSwitch;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate.MoveCandidate;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate.ReplaceCandidate;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.Collection;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/24/12
 * Time: 10:26 AM
 * To change this template use File | Settings | File Templates.
 */


public class DummyLink extends AbstractLink {
    private Link link;

    public DummyLink(AbstractSwitch start, AbstractSwitch end, Link link) {
        super(start, end);
        this.link = link;
    }

    @Override
    public Collection<Flow> getFlows() {
        return link.getFlows();
    }

    @Override
    public Link getLink(Switch target) {
        return link;
    }

    @Override
    public String toString() {
        return "DummyLink " + super.toString();
    }

    @Override
    public Switch move(Partition partition, Collection<Flow> flows, MoveCandidate moveCandidate,
                        Switch oldHost, Collection<AbstractSwitch> partitionSources) throws Exception {
        /* }

public AbstractSwitch.MoveState movePartitionFeasible(Partition partition, Switch oldHost,
  long trafficChange, long trafficChangeOtherSide,
  Partition optionalRemove, Rule addForwardingRuleForRemove) throws Exception {*/
        /*final Link otherSide = link.getOtherSide();
        boolean linkFeasibility = link.getUsedCapacity() + moveCandidate.getTrafficChange() <= link.getCapacity() &&
                otherSide.getUsedCapacity() + moveCandidate.getOtherSideTrafficChange() <= otherSide.getCapacity();
        if (linkFeasibility) {
            Rule oldForwardingRule = partition.getForwardingRule(oldHost);//it can be link.getStart();
            final Switch newHost = link.getEnd();
            final List<Rule> newHostFineRules = new ArrayList<Rule>(newHost.getFineRules());
            newHostFineRules.remove(oldForwardingRule);
            newHostFineRules.addAll(partition.getRules());
            List<Rule> newHostAggregatedRules = newHost.aggregate(newHostFineRules);
            if (newHostAggregatedRules.size() <= newHost.getMemoryCapacity()) {
                //return new AbstractSwitch.MoveState(realSwitch, newHostAggregatedRules, newHostFineRules, otherSide);
                newHost.setAggregatedRules(newHostAggregatedRules);
                newHost.setFineRules(newHostFineRules);
                moveOnMemory(partition, partitionSources,link);
                updateFlows(flows, moveCandidate);
                return newHost;
            }
        }*/
        return null;
    }

    protected void updateFlows(Collection<Flow> flows, MoveCandidate moveCandidate) {
        final Link otherSideLink = link.getOtherSide();
        link.getFlows().removeAll(flows);
        link.getFlows().addAll(moveCandidate.getNewFlows());
        otherSideLink.getFlows().removeAll(flows);
        otherSideLink.getFlows().addAll(moveCandidate.getNewFlowsOtherSide());
        link.setUsedCapacity(link.getUsedCapacity() + moveCandidate.getTrafficChange());
        otherSideLink.setUsedCapacity(otherSideLink.getUsedCapacity() + moveCandidate.getOtherSideTrafficChange());
    }

    static void  moveOnMemory(Partition partition, Collection<AbstractSwitch> partitionSources,Link link) throws Exception {
        /*final Switch newHost = link.getEnd();
        final Switch oldHost = link.getStart();

        ////////////////////////////////////////////////////////////do move
        //do the update

        oldHost.getFineRules().removeAll(partition.getRules());
        boolean oldHostSeen = false;

        final Rule newForwardingRule = partition.getForwardingRule(newHost);
        final Rule oldForwardingRule = partition.getForwardingRule(oldHost);
        //for each source of traffic of this partition remove old forwarding rule and add the new one
        for (AbstractSwitch source1 : partitionSources) {
            Switch source = ((DummySwitch) source1).getRealSwitch();
            if (source.equals(newHost)) {
                continue;
            }
            final List<Rule> fineRules = source.getFineRules();
            fineRules.remove(oldForwardingRule);
            fineRules.add(newForwardingRule);
            source.setFineRules(fineRules);
            source.reagregate();
            if (source.equals(oldHost)) {
                oldHostSeen = true;
            }
        }
        if (!oldHostSeen) {
            oldHost.reagregate();
        }*/
    }

    public Switch[] replace(Partition partition1, Partition partition2, Collection<AbstractSwitch> partitionSources1,
                            Collection<AbstractSwitch> partitionSources2, Switch switch1, Switch switch2,
                            Collection<Flow> flows1, Collection<Flow> flows2, ReplaceCandidate replaceCandidate) throws Exception {
        //one link feasibility is enough
        /*final Link otherSide = link.getOtherSide();
        boolean linkFeasibility = link.getUsedCapacity() + replaceCandidate.getTrafficChange() <= link.getCapacity() &&
                otherSide.getUsedCapacity() + replaceCandidate.getOtherSideTrafficChange() <= otherSide.getCapacity();
        if (linkFeasibility) {
            //check switch2
            Rule partition1OldForwardingRule = partition1.getForwardingRule(switch1);
            final Rule partition2NewForwardingRule = partition2.getForwardingRule(switch1);
            //create new fine rules for switch2
            final List<Rule> switch2NewFineRules = new ArrayList<Rule>(switch2.getFineRules());
            switch2NewFineRules.remove(partition1OldForwardingRule);
            switch2NewFineRules.removeAll(partition2.getRules());
            if (partitionSources2.contains(getEnd())) {
                switch2NewFineRules.add(partition2NewForwardingRule);
            }
            switch2NewFineRules.addAll(partition1.getRules());
            List<Rule> switch2NewAggRules = switch2.aggregate(switch2NewFineRules);
            if (switch2NewAggRules.size() <= switch2.getMemoryCapacity()) {
                //check switch1
                Rule partition2OldForwardingRule = partition2.getForwardingRule(switch2);
                final Rule partition1NewForwardingRule = partition1.getForwardingRule(switch2);
                //create new fine rules for switch2
                final List<Rule> switch1NewFineRules = new ArrayList<Rule>(switch1.getFineRules());
                switch1NewFineRules.remove(partition2OldForwardingRule);
                switch1NewFineRules.removeAll(partition1.getRules());
                if (partitionSources1.contains(getStart())) {
                    switch1NewFineRules.add(partition1NewForwardingRule);
                }
                switch1NewFineRules.addAll(partition2.getRules());
                List<Rule> switch1NewAggRules = switch1.aggregate(switch1NewFineRules);
                if (switch1NewAggRules.size() <= switch1.getMemoryCapacity()) {
                    switch1.setAggregatedRules(switch1NewAggRules);
                    switch1.setFineRules(switch1NewFineRules);
                    switch2.setAggregatedRules(switch2NewAggRules);
                    switch2.setFineRules(switch2NewFineRules);

                    Set<Switch> modifiedSwitches = new HashSet<Switch>();
                    updateOtherSwitches(partition1, partitionSources1, switch1, switch2, modifiedSwitches);
                    updateOtherSwitches(partition2, partitionSources2, switch2, switch1, modifiedSwitches);

                    for (Switch modifiedSwitch : modifiedSwitches) {
                        modifiedSwitch.reagregate();
                    }
                    //update flows
                    final Link otherSideLink = link.getOtherSide();
                    link.getFlows().removeAll(flows1);
                    link.getFlows().removeAll(flows2);
                    link.getFlows().addAll(replaceCandidate.getNewFlows());
                    otherSideLink.getFlows().removeAll(flows1);
                    otherSideLink.getFlows().removeAll(flows2);
                    otherSideLink.getFlows().addAll(replaceCandidate.getNewFlowsOtherSide());
                    link.setUsedCapacity(link.getUsedCapacity() + replaceCandidate.getTrafficChange());
                    otherSideLink.setUsedCapacity(otherSideLink.getUsedCapacity() + replaceCandidate.getOtherSideTrafficChange());
                    return new Switch[]{switch2, switch1};
                }

            }
        }*/

        return null;
    }

    public static void updateOtherSwitches(Partition partition1, Collection<AbstractSwitch> partitionSources1, Switch oldHost, Switch newHost, Set<Switch> modifiedSwitches) {
        //for each source of traffic of this partition remove old forwarding rule and add the new one
        /*final Rule oldForwardingRule = partition1.getForwardingRule(oldHost);
        final Rule newForwardingRule = partition1.getForwardingRule(newHost);
        for (AbstractSwitch source1 : partitionSources1) {
            Switch source = ((DummySwitch) source1).getRealSwitch();
            if (source.equals(oldHost) || source.equals(newHost)) {
                continue;
            }
            final List<Rule> fineRules = source.getFineRules();
            fineRules.remove(oldForwardingRule);
            fineRules.add(newForwardingRule);
            source.setFineRules(fineRules);
            modifiedSwitches.add(source);
        }*/
    }
}