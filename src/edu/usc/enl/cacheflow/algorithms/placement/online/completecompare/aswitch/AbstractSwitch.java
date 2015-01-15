package edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch;

import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate.AbstractCandidate;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate.MoveCandidate;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate.ReplaceCandidate;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.link.AbstractLink;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.link.BigLink;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.link.DummyLink;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.Topology;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/21/12
 * Time: 10:04 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractSwitch implements Comparable<AbstractSwitch> {
    private final Map<Partition, Map<AbstractLink, MoveCandidate>> partitionBenefit = new HashMap<Partition, Map<AbstractLink, MoveCandidate>>();
    private final Map<Partition, List<AbstractCandidate>> partitionCandidates = new HashMap<Partition, List<AbstractCandidate>>();
    public static final long THRESHOLD = 0;
    private String id;
    protected Map<AbstractSwitch, AbstractLink> linksTo = new HashMap<AbstractSwitch, AbstractLink>();

    protected AbstractSwitch(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        return compareTo((AbstractSwitch) obj) == 0;
    }

    public Map<AbstractSwitch, AbstractLink> getLinksTo() {
        return linksTo;
    }

    public void addLinkTo(AbstractSwitch end, Link link, Topology topology) {
        if (this instanceof DummySwitch && end instanceof DummySwitch) {
            linksTo.put(end, new DummyLink(this, end, link));
        } else {
            BigLink bigLink = (BigLink) linksTo.get(end);
            if (bigLink == null) {
                bigLink = new BigLink(this, end, topology);
                linksTo.put(end, bigLink);
            }
            bigLink.addLink(link);
        }
    }

    public boolean hasPartition(Partition partition) {
        return partitionBenefit.containsKey(partition);
    }

    public void fillBenefitsForPartition(Collection<Flow> thisPartitionTraffic, Partition partition, Collection<Flow> denyFlows) {
        Map<AbstractLink, MoveCandidate> benefits = new HashMap<AbstractLink, MoveCandidate>();
        for (AbstractLink link : linksTo.values()) {
            //compute benefit
            benefits.put(link, computeBenefit(link, partition, thisPartitionTraffic, denyFlows));
        }
        partitionBenefit.put(partition, benefits);
    }

    public Map<Partition, MoveCandidate> getBenefitsOnLink(AbstractLink link) {
        Map<Partition, MoveCandidate> output = new HashMap<Partition, MoveCandidate>();
        for (Map.Entry<Partition, Map<AbstractLink, MoveCandidate>> partitionMapEntry : partitionBenefit.entrySet()) {
            output.put(partitionMapEntry.getKey(), partitionMapEntry.getValue().get(link));
        }
        return output;
    }

    public void createCandidatesFor(Partition partition, boolean oneSide) {
        final Map<AbstractLink, MoveCandidate> linkLongMap = partitionBenefit.get(partition);
        List<AbstractCandidate> candidates = new LinkedList<AbstractCandidate>();
        for (Map.Entry<AbstractLink, MoveCandidate> linkBenefitEntry : linkLongMap.entrySet()) {
            AbstractLink link = linkBenefitEntry.getKey();
            if (oneSide && link.getStart().compareTo(link.getEnd()) < 0) {
                continue;
            }
            final MoveCandidate candidate = linkBenefitEntry.getValue();
            final Long benefit = candidate.getBenefit();
            //movement benefit
            if (benefit > THRESHOLD) {
                candidates.add(candidate);
            }

            //replacement benefit
            final Map<Partition, MoveCandidate> benefits = link.getEnd().getBenefitsOnLink(link.getOtherSide());
            for (Map.Entry<Partition, MoveCandidate> partitionBenefitEntry : benefits.entrySet()) {
                final MoveCandidate replaceCandidate = partitionBenefitEntry.getValue();
                final Long otherBenefit = replaceCandidate.getBenefit();
                if (benefit + otherBenefit > 0) {
                    candidates.add(new ReplaceCandidate(candidate, replaceCandidate));
                }
            }
        }
        if (candidates.size() > 0) {
            partitionCandidates.put(partition, candidates);
        }
    }

    public AbstractCandidate getBestCandidateForPartition(Partition partition) {
        final List<AbstractCandidate> candidates = partitionCandidates.get(partition);
        if (candidates == null) {
            return null;
        }
        AbstractCandidate maxCandidate = null;
        int index = 0;
        int maxIndex = 0;
        for (AbstractCandidate candidate : candidates) {
            if (maxCandidate == null || maxCandidate.getBenefit() < candidate.getBenefit()) {
                maxCandidate = candidate;
                maxIndex = index;
            }
            index++;
        }
        if (maxCandidate != null) {
            candidates.remove(maxIndex);
        }
        return maxCandidate;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private MoveCandidate computeBenefit(AbstractLink link, Partition partition, Collection<Flow> partitionTraffic,
                                         Collection<Flow> denyFlows) {
        boolean print = false;
        /*if (partition.hashCode() == 1300270133) {
            System.out.println(link);
            print = true;
        }*/

        //f5=other income
        //f6=other outcome
        //f3=outcome
        //f4=income
        //link=f3
        //otherLink=f4

        // unrolled for performance gain
        // Collection<Flow> f3Flows = new LinkedList<Flow>(link.getFlows());
        //f3Flows.retainAll(partitionTraffic);
        Collection<Flow> f3Flows = new HashSet<Flow>();
        for (Flow flow : link.getFlows()) {
            if (partitionTraffic.contains(flow)) {
                f3Flows.add(flow);
            }
        }

        //Collection<Flow> f4Flows = new LinkedList<Flow>(link.getOtherSide().getFlows());
        //f4Flows.retainAll(partitionTraffic);
        Collection<Flow> f4Flows = new HashSet<Flow>();
        for (Flow flow : link.getOtherSide().getFlows()) {
            if (partitionTraffic.contains(flow)) {
                f4Flows.add(flow);
            }
        }

        Collection<Flow> f5Flows = new HashSet<Flow>(partitionTraffic);
        f5Flows.removeAll(f4Flows);

        Collection<Flow> f6Flows = new HashSet<Flow>(partitionTraffic);
        f6Flows.removeAll(f3Flows);
        f6Flows.removeAll(denyFlows);

        long oldF3Traffic = 0;
        for (Flow f3Flow : f3Flows) {
            oldF3Traffic += f3Flow.getTraffic();
        }

        long oldF4Traffic = 0;
        for (Flow f4Flow : f4Flows) {
            oldF4Traffic += f4Flow.getTraffic();
        }

        long newF3Traffic = 0;
        for (Flow f5Flow : f5Flows) {
            newF3Traffic += f5Flow.getTraffic();
        }

        long newF4Traffic = 0;
        for (Flow f6Flow : f6Flows) {
            newF4Traffic += f6Flow.getTraffic();
        }
        if (print) {
            System.out.println("f3flows.size " + f3Flows.size());
            System.out.println("f4flows.size " + f4Flows.size());
            System.out.println("f5flows.size " + f5Flows.size());
            System.out.println("f6flows.size " + f6Flows.size());

        }

        return new MoveCandidate(link, partition, f5Flows, f6Flows, oldF3Traffic, oldF4Traffic, newF3Traffic, newF4Traffic);
    }

    public void removePartition(Partition partition) {
        partitionCandidates.remove(partition);
        partitionBenefit.remove(partition);
    }

    public void addPartition(Partition partition, Collection<Flow> partitionTraffic, Collection<Flow> denyFlows) {
        fillBenefitsForPartition(partitionTraffic, partition, denyFlows);
        createCandidatesFor(partition, false);
    }

    public AbstractLink getLinkTo(AbstractSwitch neighbor) {
        return linksTo.get(neighbor);
    }

    public int compareTo(AbstractSwitch o) {
        return this.id.compareTo(o.id);
    }

    public void createCandidates(boolean oneSide) {
        for (Map.Entry<AbstractSwitch, AbstractLink> switchLinkEntry : linksTo.entrySet()) {
            AbstractLink link = switchLinkEntry.getValue();
            if (oneSide && link.getStart().compareTo(link.getEnd()) < 0) {
                continue;
            }
            final Map<Partition, MoveCandidate> benefits = link.getEnd().getBenefitsOnLink(link.getOtherSide());
            for (Map.Entry<Partition, Map<AbstractLink, MoveCandidate>> partitionMapEntry : partitionBenefit.entrySet()) {
                final Partition partition = partitionMapEntry.getKey();
                final MoveCandidate moveCandidate = partitionMapEntry.getValue().get(link);
                final Long benefit = moveCandidate.getBenefit();

                List<AbstractCandidate> candidates = partitionCandidates.get(partition);
                if (candidates == null) {
                    candidates = new LinkedList<AbstractCandidate>();
                }
                //movement benefit
                if (benefit > THRESHOLD) {
                    candidates.add(moveCandidate);
                }
                //replacement benefit
                for (Map.Entry<Partition, MoveCandidate> partitionBenefitEntry : benefits.entrySet()) {
                    final MoveCandidate replaceCandidate = partitionBenefitEntry.getValue();
                    final Long otherBenefit = replaceCandidate.getBenefit();
                    if (benefit + otherBenefit > 0) {
                        candidates.add(new ReplaceCandidate(moveCandidate, replaceCandidate));
                    }
                }
                if (candidates.size() > 0) {
                    partitionCandidates.put(partition, candidates);
                }
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////


    /*public static class MoveState {
        private Switch host;
        private List<Rule> aggregatedRules;
        private List<Rule> fineRules;
        private Link link;

        public MoveState(Switch host, List<Rule> aggregatedRules, List<Rule> fineRules, Link link) {
            this.host = host;
            this.aggregatedRules = aggregatedRules;
            this.fineRules = fineRules;
            this.link = link;
        }

        public Switch getHost() {
            return host;
        }

        public List<Rule> getAggregatedRules() {
            return aggregatedRules;
        }

        public List<Rule> getFineRules() {
            return fineRules;
        }

        public Link getLink() {
            return link;
        }
    }*/
}

