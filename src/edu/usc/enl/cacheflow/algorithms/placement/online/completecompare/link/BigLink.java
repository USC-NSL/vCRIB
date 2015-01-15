package edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.link;

import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.AbstractSwitch;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.BigSwitch;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.DummySwitch;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate.MoveCandidate;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate.ReplaceCandidate;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/24/12
 * Time: 10:26 AM
 * To change this template use File | Settings | File Templates.
 */
public class BigLink extends AbstractLink {
    Map<Switch, Link> linksToFrom;
    boolean startIsBig = true;
    List<Flow> flowsCache;
    Topology topology;

    public BigLink(AbstractSwitch start, AbstractSwitch end, Topology topology) {
        super(start, end);
        linksToFrom = new HashMap<Switch, Link>();
        startIsBig = start instanceof BigSwitch;
        this.topology = topology;
    }

    @Override
    public Collection<Flow> getFlows() {
        if (flowsCache == null) {
            flowsCache = new LinkedList<Flow>();
            for (Link link : linksToFrom.values()) {
                flowsCache.addAll(link.getFlows());
            }
        }
        return flowsCache;
    }

    @Override
    public String toString() {
        return "BigLink " + super.toString();
    }

    public void resetFlowCache() {
        flowsCache = null;
    }

    public void addLink(Link link) {
        if (startIsBig) {
            linksToFrom.put(link.getStart(), link);
        } else {
            linksToFrom.put(link.getEnd(), link);
        }
    }

    public Link getLink(Switch aSwitch) {
        return linksToFrom.get(aSwitch);
    }


    public Switch move(Partition partition, Collection<Flow> flows, MoveCandidate moveCandidate,
                       Switch oldHost, Collection<AbstractSwitch> partitionSources) throws Exception {

       /* //if memory is OK
        if (startIsBig) {
            //check memory
            Rule oldForwardingRule = partition.getForwardingRule(oldHost);//it can be link.getStart();
            final Switch newHost = ((DummySwitch) getEnd()).getRealSwitch();
            final List<Rule> newHostFineRules = new ArrayList<Rule>(newHost.getFineRules());
            newHostFineRules.remove(oldForwardingRule);
            newHostFineRules.addAll(partition.getRules());
            List<Rule> newHostAggregatedRules = newHost.aggregate(newHostFineRules);
            if (newHostAggregatedRules.size() <= newHost.getMemoryCapacity()) {
                //memory is OK check the links
                ECMPFeasibilityResult ECMPFeasibilityResult = new ECMPFeasibilityResult(flows, moveCandidate, oldHost,
                        topology).invoke();
                boolean ECMPFeasible = ECMPFeasibilityResult.isEcmpFeasible();
                Map<Link, Collection<Flow>> ECMPFlows = ECMPFeasibilityResult.getEcmpFlows();
                Map<Link, Long> removeTrafficSize = ECMPFeasibilityResult.getRemoveTrafficSize();
                Map<Link, Long> trafficChangeMap = ECMPFeasibilityResult.getTrafficChangeMap();

                if (ECMPFeasible) {
                    //do memory update
                    newHost.setAggregatedRules(newHostAggregatedRules);
                    newHost.setFineRules(newHostFineRules);
                    DummyLink.moveOnMemory(partition, partitionSources, linksToFrom.get(oldHost));
                    //do link update
                    doECMP(flows, ECMPFlows, removeTrafficSize, trafficChangeMap);
                    ((BigSwitch) getStart()).resetAllBigLinks();
                    return newHost;
                }
            }

        } else {

            final ReverseECMPCandidate reverseECMPCandidate = selectReverseECMPCandidate(partition, flows, moveCandidate,
                    oldHost, null);
            if (reverseECMPCandidate != null) {
                //do it
                //memory update
                final Switch candidateSwitch = reverseECMPCandidate.getNewEndCandidate();
                candidateSwitch.setAggregatedRules(reverseECMPCandidate.getNewHostAggregatedRules());
                candidateSwitch.setFineRules(reverseECMPCandidate.getNewHostFineRules());
                DummyLink.moveOnMemory(partition, partitionSources, linksToFrom.get(candidateSwitch));

                //link update
                doReverseECMP(flows, (BigSwitch) getEnd(), reverseECMPCandidate.getNewTrafficOnLinkMap(),
                        reverseECMPCandidate.getTrafficChangeOnLinkMap());
                ((BigSwitch) getEnd()).resetAllBigLinks();
                return reverseECMPCandidate.getNewEndCandidate();
            }
        }*/

        return null;
    }

    /////////////////////// ECMP

    private void doECMP(Collection<Flow> flows, Map<Link, Collection<Flow>> ECMPFlows, Map<Link, Long> removeTrafficSize,
                        Map<Link, Long> trafficChangeMap) throws Exception {
        //do link update
        for (Map.Entry<Link, Collection<Flow>> linkFlowAddEntry : ECMPFlows.entrySet()) {
            //remove flow traffic
            final Link link = linkFlowAddEntry.getKey();
            final List<Flow> flows1 = link.getFlows();
            if (removeTrafficSize.get(link) != null) {
                flows1.removeAll(flows);
            }
            //add flows that should be added
            flows1.addAll(linkFlowAddEntry.getValue());
            link.setUsedCapacity(link.getUsedCapacity() + trafficChangeMap.get(link));
        }
    }

    private void checkECMP(Topology topology, Map<Link, Collection<Flow>> ECMPFlows, Map<Link, Long> ECMPFlowsSize,
                           AbstractLink bigOutLink, Collection<Flow> toDoECMP, Switch newHost) {
        for (Flow flow : toDoECMP) {

            final Switch inputSwitch = ((DummySwitch) bigOutLink.getEnd()).getRealSwitch();
            final Collection<Link> path;
            if (inputSwitch.equals(newHost)){
                //it is comming from the newhost so route to the destination
                path = topology.getPath(inputSwitch, flow.getDestination(), flow);
            }else{
                path = topology.getPath(inputSwitch, newHost, flow);
            }
            //get just two first links
            int i = 0;
            for (Link link : path) {
                if (i >= 2) {
                    break;
                }
                ECMPFlows.get(link).add(flow);
                ECMPFlowsSize.put(link, ECMPFlowsSize.get(link) + flow.getTraffic());
                i++;
            }
        }
    }

    ///////////////////////////////////////// Reverse ECMP

    private ReverseECMPCandidate selectReverseECMPCandidate(Partition partition, Collection<Flow> flows, MoveCandidate moveCandidate,
                                                            Switch oldHost, Partition optionalRemove) throws Exception {
        /*//for each node in the big switch
        //check memory feasibility
        // reverse ECMP: need to move traffic to links of just one core
        BigSwitch bigSwitch = (BigSwitch) moveCandidate.getLink().getEnd(); //in replace we cannot just use this link end

        //traffic for partition on outlink and traffic for partition on inlink
        Map<AbstractLink, Collection<Flow>> bigLinkTraffic = new HashMap<AbstractLink, Collection<Flow>>();
        Map<AbstractLink, Long> bigLinkTrafficSize = new HashMap<AbstractLink, Long>();
        findTrafficOnBigLinks(flows, bigSwitch, bigLinkTraffic, bigLinkTrafficSize);

        Rule oldForwardingRule = partition.getForwardingRule(oldHost);
        //bin packing with two constraints memory and link capacity
        //optimize for no aggregation!

        Set<Switch> seen = new HashSet<Switch>();
        for (Switch newEndCandidate : linksToFrom.keySet()) {
            if (newEndCandidate.getMemoryCapacity() - newEndCandidate.getUsedCapacity() > partition.getSize()) {
                seen.add(newEndCandidate);
                final List<Rule> newHostFineRules = new ArrayList<Rule>(newEndCandidate.getFineRules());
                newHostFineRules.remove(oldForwardingRule);
                if (optionalRemove != null) {
                    newHostFineRules.removeAll(optionalRemove.getRules());//no need to add forwarding rule as the big swtich
                    //cannot be the source of traffic
                }
                newHostFineRules.addAll(partition.getRules());
                List<Rule> newHostAggregatedRules = newEndCandidate.aggregate(newHostFineRules);
                if (newHostAggregatedRules.size() <= newEndCandidate.getMemoryCapacity()) {
                    ReverseECMPFeasibility reverseECMPFeasibility = new ReverseECMPFeasibility(flows, moveCandidate,
                            bigSwitch, bigLinkTraffic, bigLinkTrafficSize, newEndCandidate).invoke();
                    boolean ReverseECMPLinkFeasibility = reverseECMPFeasibility.isReverseECMPLinkFeasibility();
                    if (ReverseECMPLinkFeasibility) {
                        return new ReverseECMPCandidate(newEndCandidate, newHostFineRules, newHostAggregatedRules,
                                reverseECMPFeasibility.getNewTrafficOnLinkMap(), reverseECMPFeasibility.getTrafficChangeOnLinkMap());
                    }
                }
            }
        }*/
        return null;
    }

    private void findTrafficOnBigLinks(Collection<Flow> flows, BigSwitch bigSwitch, Map<AbstractLink, Collection<Flow>> bigLinkTraffic, Map<AbstractLink, Long> bigLinkTrafficSize) {
        for (AbstractLink abstractLink : bigSwitch.getLinksTo().values()) {
            Collection<Flow> flows2 = new LinkedList<Flow>(abstractLink.getFlows());
            flows2.retainAll(flows);
            bigLinkTraffic.put(abstractLink, flows2);
            long sum = 0;
            for (Flow flow : flows2) {
                sum += flow.getTraffic();
            }
            bigLinkTrafficSize.put(abstractLink, sum);
            final AbstractLink otherSideLink = abstractLink.getOtherSide();
            flows2 = new LinkedList<Flow>(otherSideLink.getFlows());
            flows2.retainAll(flows);
            bigLinkTraffic.put(otherSideLink, flows2);
            sum = 0;
            for (Flow flow : flows2) {
                sum += flow.getTraffic();
            }
            bigLinkTrafficSize.put(otherSideLink, sum);
        }
    }

    private void doReverseECMP(Collection<Flow> flows, BigSwitch bigSwitch, Map<Link, Collection<Flow>> newTrafficOnLinkMap,
                               Map<Link, Long> trafficChangeOnLinkMap) throws Exception {

        //do move on links
        //remove traffic on all links
        for (AbstractLink abstractLink : bigSwitch.getLinksTo().values()) {
            for (Link realLink : ((BigLink) abstractLink).linksToFrom.values()) {
                {
                    realLink.getFlows().removeAll(flows);
                    final Collection<Flow> newFlowsOn = newTrafficOnLinkMap.get(realLink);
                    if (newFlowsOn != null) {
                        realLink.getFlows().addAll(newFlowsOn);
                        realLink.setUsedCapacity(realLink.getUsedCapacity() + trafficChangeOnLinkMap.get(realLink));
                    } else {
                        //update usedCapacity
                        long sum = 0;
                        for (Flow flow : realLink.getFlows()) {
                            sum += flow.getTraffic();
                        }
                        realLink.setUsedCapacity(sum);
                    }
                }
                {
                    final Link otherSide = realLink.getOtherSide();
                    otherSide.getFlows().removeAll(flows);
                    final Collection<Flow> newFlowsOn2 = newTrafficOnLinkMap.get(otherSide);
                    if (newFlowsOn2 != null) {
                        otherSide.getFlows().addAll(newFlowsOn2);
                        otherSide.setUsedCapacity(otherSide.getUsedCapacity() + trafficChangeOnLinkMap.get(otherSide));
                    } else {
                        //update usedCapacity
                        long sum = 0;
                        for (Flow flow : otherSide.getFlows()) {
                            sum += flow.getTraffic();
                        }
                        otherSide.setUsedCapacity(sum);
                    }
                }

            }
        }
    }

    private long getFlowsOfPartitionOnLink(Collection<Flow> flows, Collection<Flow> flowsOnCandidateLink) {
        flowsOnCandidateLink.retainAll(flows);
        long sizeOfFlowsOnCandidateLink = 0;
        for (Flow flow : flowsOnCandidateLink) {
            sizeOfFlowsOnCandidateLink += flow.getTraffic();
        }
        return sizeOfFlowsOnCandidateLink;
    }

    /////////////////////////////////////// REPLACE
    public Switch[] replace(Partition partition1, Partition partition2, Collection<AbstractSwitch> partitionSources1,
                            Collection<AbstractSwitch> partitionSources2, Switch switch1, Switch switch2,
                            Collection<Flow> flows1, Collection<Flow> flows2, ReplaceCandidate replaceCandidate) throws Exception {

       /* //first check the one that should be moved to the dummy switch
        if (startIsBig) {
            BigSwitch bigSwitch = (BigSwitch) getStart();
            //partition 1 wants to go to dummyswitch
            //check memory
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
                //memory is OK check the links
                ECMPFeasibilityResult ECMPFeasibilityResult = new ECMPFeasibilityResult(flows1,
                        replaceCandidate.getMoveCandidate1(), switch1, topology).invoke();
                boolean ECMPFeasibility = ECMPFeasibilityResult.isEcmpFeasible();
                Map<Link, Collection<Flow>> ECMPFlows = ECMPFeasibilityResult.getEcmpFlows();
                Map<Link, Long> removeTrafficSize = ECMPFeasibilityResult.getRemoveTrafficSize();
                Map<Link, Long> trafficChangeMap = ECMPFeasibilityResult.getTrafficChangeMap();
                if (ECMPFeasibility) {
                    //now check the other side that needs ReverseECMP both memory and link
                    final ReverseECMPCandidate reverseECMPCandidate = selectReverseECMPCandidate(partition2, flows2,
                            replaceCandidate.getMoveCandidate2(), switch2, partition1);

                    if (reverseECMPCandidate != null) {
                        //do memory update
                        final Switch newHostPartition2 = reverseECMPCandidate.getNewEndCandidate();
                        newHostPartition2.setAggregatedRules(reverseECMPCandidate.getNewHostAggregatedRules());
                        newHostPartition2.setFineRules(reverseECMPCandidate.getNewHostFineRules());

                        //switch1 and newhostPartition2 an be different
                        if (!switch1.equals(newHostPartition2)) {
                            switch1.getFineRules().removeAll(partition1.getRules());//switch1 should be a big switch so it cannot be a source of traffic and does not need a forwarding rule for this partition
                            switch1.reagregate();
                        }

                        switch2.setAggregatedRules(switch2NewAggRules);
                        switch2.setFineRules(switch2NewFineRules);

                        Set<Switch> modifiedSwitches = new HashSet<Switch>();
                        DummyLink.updateOtherSwitches(partition1, partitionSources1, switch1, switch2, modifiedSwitches);
                        DummyLink.updateOtherSwitches(partition2, partitionSources2, switch2, newHostPartition2, modifiedSwitches);

                        for (Switch modifiedSwitch : modifiedSwitches) {
                            modifiedSwitch.reagregate();
                        }

                        doECMP(flows1, ECMPFlows, removeTrafficSize, trafficChangeMap);
                        //do reverse ECMP
                        doReverseECMP(flows2, (BigSwitch) getStart(), reverseECMPCandidate.getNewTrafficOnLinkMap(),
                                reverseECMPCandidate.getTrafficChangeOnLinkMap());
                        bigSwitch.resetAllBigLinks();
                        return new Switch[]{switch2, newHostPartition2};
                    }
                }
            }
        } else {
            return getOtherSide().replace(partition2, partition1, partitionSources2, partitionSources1, switch2, switch1,
                    flows2, flows1, replaceCandidate.reverse());
        }*/

        return null;
    }

    private class ECMPFeasibilityResult {
        private Collection<Flow> flows;
        private MoveCandidate moveCandidate;
        private Switch oldHost;
        private Topology topology;
        private Map<Link, Long> removeTrafficSize;
        private Map<Link, Collection<Flow>> ecmpFlows;
        private boolean ecmpFeasibility;
        private Map<Link, Long> trafficChangeMap;

        public ECMPFeasibilityResult(Collection<Flow> flows, MoveCandidate moveCandidate, Switch oldHost, Topology topology) {
            this.flows = flows;
            this.moveCandidate = moveCandidate;
            this.oldHost = oldHost;
            this.topology = topology;
        }

        public Map<Link, Long> getRemoveTrafficSize() {
            return removeTrafficSize;
        }

        public Map<Link, Collection<Flow>> getEcmpFlows() {
            return ecmpFlows;
        }

        public boolean isEcmpFeasible() {
            return ecmpFeasibility;
        }

        public Map<Link, Long> getTrafficChangeMap() {
            return trafficChangeMap;
        }

        public ECMPFeasibilityResult invoke() {
            //need to run ECMP
            BigSwitch bigSwitch = (BigSwitch) getStart();
            //find traffic of partition on all real links to know the size
            //just the links to/from the oldhost should have traffic
            removeTrafficSize = new HashMap<Link, Long>();
            for (AbstractLink bigOutLink : bigSwitch.getLinksTo().values()) {
                final Link linkToOldHost = bigOutLink.getLink(oldHost);
                final List<Flow> trafficOnLinkToOldHost = new LinkedList<Flow>(linkToOldHost.getFlows());
                final long sizeTrafficOnLinkToOldHost = getFlowsOfPartitionOnLink(flows, trafficOnLinkToOldHost);
                removeTrafficSize.put(linkToOldHost, -sizeTrafficOnLinkToOldHost);

                final Link otherLinkToOldHost = linkToOldHost.getOtherSide();
                final List<Flow> trafficOnOtherLinkToOldHost = new LinkedList<Flow>(otherLinkToOldHost.getFlows());
                final long sizeTrafficOnOtherLinkToOldHost = getFlowsOfPartitionOnLink(flows, trafficOnOtherLinkToOldHost);
                removeTrafficSize.put(otherLinkToOldHost, -sizeTrafficOnOtherLinkToOldHost);
            }

            ecmpFlows = new HashMap<Link, Collection<Flow>>();
            Map<Link, Long> ECMPFlowsSize = new HashMap<Link, Long>();
            //fill data structures
            for (AbstractLink abstractLink : bigSwitch.getLinksTo().values()) {
                for (Link link : ((BigLink) abstractLink).linksToFrom.values()) {
                    ecmpFlows.put(link, new LinkedList<Flow>());
                    ECMPFlowsSize.put(link, 0l);
                    ecmpFlows.put(link.getOtherSide(), new LinkedList<Flow>());
                    ECMPFlowsSize.put(link.getOtherSide(), 0l);
                }
            }

            //traffic for partition on outlink and traffic for partition on inlink
            Map<AbstractLink, Collection<Flow>> bigLinkTraffic = new HashMap<AbstractLink, Collection<Flow>>();
            Map<AbstractLink, Long> bigLinkTrafficSize = new HashMap<AbstractLink, Long>();
            findTrafficOnBigLinks(flows, bigSwitch, bigLinkTraffic, bigLinkTrafficSize);

            //for each incoming flow to the big switch
            final Switch newHost = ((DummySwitch) moveCandidate.getLink().getEnd()).getRealSwitch();
            for (AbstractLink bigOutLink : bigSwitch.getLinksTo().values()) {
                if (bigOutLink.equals(moveCandidate.getLink())) {
                    //the link has changed traffic need to use movecandidate and ECMP traffic from the newhost of partition
                    final Collection<Flow> toDoECMP = moveCandidate.getNewFlowsOtherSide();
                    checkECMP(topology, ecmpFlows, ECMPFlowsSize, bigOutLink, toDoECMP, newHost);
                } else {
                    //other links don't have traffic chnage so just ECMP traffic from source
                    checkECMP(topology, ecmpFlows, ECMPFlowsSize, bigOutLink, bigLinkTraffic.get(bigOutLink.getOtherSide()), newHost);
                }
            }

            //if it is ok on all links
            ecmpFeasibility = true;
            trafficChangeMap = new HashMap<Link, Long>();
            for (Map.Entry<Link, Long> linkECMPFlowSizeAddEntry : ECMPFlowsSize.entrySet()) {
                Link link = linkECMPFlowSizeAddEntry.getKey();
                final Long trafficAdd = linkECMPFlowSizeAddEntry.getValue();
                final Long trafficRemoval = removeTrafficSize.get(link);
                long trafficChange = trafficAdd;
                if (trafficRemoval != null) {
                    trafficChange += trafficRemoval;
                }
                trafficChangeMap.put(link, trafficChange);
                if (!(link.getUsedCapacity() + trafficChange <= link.getCapacity())) {
                    ecmpFeasibility = false;
                    break;
                }
            }
            return this;
        }
    }

    private class ReverseECMPFeasibility {
        private Collection<Flow> flows;
        private MoveCandidate moveCandidate;
        private BigSwitch bigSwitch;
        private Map<AbstractLink, Collection<Flow>> bigLinkTraffic;
        private Map<AbstractLink, Long> bigLinkTrafficSize;
        private Switch newEndCandidate;
        private boolean reverseECMPLinkFeasibility;
        private Map<Link, Collection<Flow>> newTrafficOnLinkMap;
        private Map<Link, Long> trafficChangeOnLinkMap;

        public ReverseECMPFeasibility(Collection<Flow> flows, MoveCandidate moveCandidate, BigSwitch bigSwitch,
                                      Map<AbstractLink, Collection<Flow>> bigLinkTraffic,
                                      Map<AbstractLink, Long> bigLinkTrafficSize, Switch newEndCandidate) {
            this.flows = flows;
            this.moveCandidate = moveCandidate;
            this.bigSwitch = bigSwitch;
            this.bigLinkTraffic = bigLinkTraffic;
            this.bigLinkTrafficSize = bigLinkTrafficSize;
            this.newEndCandidate = newEndCandidate;
        }

        public boolean isReverseECMPLinkFeasibility() {
            return reverseECMPLinkFeasibility;
        }

        public Map<Link, Collection<Flow>> getNewTrafficOnLinkMap() {
            return newTrafficOnLinkMap;
        }

        public Map<Link, Long> getTrafficChangeOnLinkMap() {
            return trafficChangeOnLinkMap;
        }

        public ReverseECMPFeasibility invoke() {
            //check other ECMP links that now go to be consolidated on fewer number of links as they cannot be multipathed anymoore
            reverseECMPLinkFeasibility = true;
            final Collection<AbstractLink> allOutLinks = bigSwitch.getLinksTo().values();
            newTrafficOnLinkMap = new HashMap<Link, Collection<Flow>>();
            trafficChangeOnLinkMap = new HashMap<Link, Long>();
            for (AbstractLink outLink : allOutLinks) {

                final Link realOutLink = outLink.getLink(newEndCandidate);
                final List<Flow> flowsOfPartitionOnLink = new LinkedList<Flow>(realOutLink.getFlows());
                final long sizeOfFlowsOfPartitionOnLink = getFlowsOfPartitionOnLink(flows, flowsOfPartitionOnLink);
                final Link realOutLinkOtherSide = realOutLink.getOtherSide();
                final List<Flow> flowsOfPartitionOnOtherLink = new LinkedList<Flow>(realOutLinkOtherSide.getFlows());
                final long sizeOfFlowsOfPartitionOnOtherLink = getFlowsOfPartitionOnLink(flows, flowsOfPartitionOnOtherLink);
                long change = -sizeOfFlowsOfPartitionOnLink + bigLinkTrafficSize.get(outLink);
                long changeOtherLink = -sizeOfFlowsOfPartitionOnOtherLink + bigLinkTrafficSize.get(outLink.getOtherSide());
                if (outLink.equals(moveCandidate.getLink().getOtherSide())) {//movcandidate.link is always inlink in reverse ecmp
                    //the traffic on this abstractlink has been changed need to use moveCandidate
                    change += moveCandidate.getOtherSideTrafficChange();
                    changeOtherLink += moveCandidate.getTrafficChange();
                    if (realOutLink.getUsedCapacity() + change <= realOutLink.getCapacity()
                            && realOutLinkOtherSide.getUsedCapacity() + changeOtherLink <= realOutLinkOtherSide.getCapacity()) {
                        newTrafficOnLinkMap.put(realOutLink, moveCandidate.getNewFlowsOtherSide());
                        trafficChangeOnLinkMap.put(realOutLink, change);
                        newTrafficOnLinkMap.put(realOutLinkOtherSide, moveCandidate.getNewFlows());
                        trafficChangeOnLinkMap.put(realOutLinkOtherSide, changeOtherLink);
                    } else {
                        reverseECMPLinkFeasibility = false;
                        break;
                    }
                } else {
                    if (realOutLink.getUsedCapacity() + change <= realOutLink.getCapacity()
                            && realOutLinkOtherSide.getUsedCapacity() + changeOtherLink <= realOutLinkOtherSide.getCapacity()) {
                        newTrafficOnLinkMap.put(realOutLink, bigLinkTraffic.get(outLink));
                        trafficChangeOnLinkMap.put(realOutLink, change);
                        newTrafficOnLinkMap.put(realOutLinkOtherSide, bigLinkTraffic.get(outLink.getOtherSide()));
                        trafficChangeOnLinkMap.put(realOutLinkOtherSide, changeOtherLink);
                    } else {
                        reverseECMPLinkFeasibility = false;
                        break;
                    }
                }
            }
            return this;
        }
    }

    private class ReverseECMPCandidate {
        Switch newEndCandidate;
        List<Rule> newHostFineRules;
        List<Rule> newHostAggregatedRules;
        Map<Link, Collection<Flow>> newTrafficOnLinkMap;
        Map<Link, Long> trafficChangeOnLinkMap;

        private ReverseECMPCandidate(Switch newEndCandidate, List<Rule> newHostFineRules, List<Rule> newHostAggregatedRules,
                                     Map<Link, Collection<Flow>> newTrafficOnLinkMap, Map<Link, Long> trafficChangeOnLinkMap) {
            this.newEndCandidate = newEndCandidate;
            this.newHostFineRules = newHostFineRules;
            this.newHostAggregatedRules = newHostAggregatedRules;
            this.newTrafficOnLinkMap = newTrafficOnLinkMap;
            this.trafficChangeOnLinkMap = trafficChangeOnLinkMap;
        }

        public Switch getNewEndCandidate() {
            return newEndCandidate;
        }

        public List<Rule> getNewHostFineRules() {
            return newHostFineRules;
        }

        public List<Rule> getNewHostAggregatedRules() {
            return newHostAggregatedRules;
        }

        public Map<Link, Collection<Flow>> getNewTrafficOnLinkMap() {
            return newTrafficOnLinkMap;
        }

        public Map<Link, Long> getTrafficChangeOnLinkMap() {
            return trafficChangeOnLinkMap;
        }
    }
}