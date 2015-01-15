package edu.usc.enl.cacheflow.algorithms.placement.online;

/*
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 2/12/12
 * Time: 1:19 PM
 * To change this template use File | Settings | File Templates.
*/


public class NeighborReplacement {
    /*private Map<Partition, Set<Flow>> partitionTraffic;
    private Map<Switch, List<Partition>> switchPartitionMap;
    private Map<Partition, Set<Switch>> partitionSources = new HashMap<Partition, Set<Switch>>();
    public final Map<Switch, SwitchOverhead> switchOverheads = new HashMap<Switch, SwitchOverhead>();

    public Map<Switch, List<Partition>> replace(Map<Switch, List<Partition>> switchPartitionMap, Map<Partition, Set<Flow>> partitionTraffic
    ) {
        this.partitionTraffic = partitionTraffic;
        this.switchPartitionMap = switchPartitionMap;
        fillPartitionSourceMap(partitionTraffic);


        //for each partition compute metric
        //for each partition on hosts, compute their traffic on each input link and select those partitions not belong to where they are
        List<Pair> toBeMovedPartitions = new ArrayList<Pair>();
        for (Map.Entry<Switch, List<Partition>> switchListEntry : switchPartitionMap.entrySet()) {
            Switch host = switchListEntry.getKey();
            SwitchOverhead switchOverhead = new SwitchOverhead(host);
            final List<Partition> partitions = switchListEntry.getValue();
            for (Partition p : partitions) {
                switchOverhead.put(p, getPartitionTrafficOnSwitchPorts(host, partitionTraffic.get(p)));
            }
            switchOverheads.put(host, switchOverhead);
            // SELECT ALGORITHM
            toBeMovedPartitions.addAll(switchOverhead.getAlphaSignificantCandidates(1));
        }

        //MOVE
        greedyMove(toBeMovedPartitions);

        //also can use backtrack
        return switchPartitionMap;
    }

    private void fillPartitionSourceMap(Map<Partition, Set<Flow>> partitionTraffic) {
        for (Map.Entry<Partition, Set<Flow>> partitionFlowList : partitionTraffic.entrySet()) {
            Set<Switch> partitionSource = new HashSet<Switch>();
            for (Flow flow : partitionFlowList.getValue()) {
                partitionSource.add(flow.getSource());
            }
            partitionSources.put(partitionFlowList.getKey(), partitionSource);
        }
    }

    private void greedyMove(List<Pair> toBeMovedPartitions) {
        //for each partition calculate the benefit of movement
        //Checking feasibility here is not useful as they may become feasible or infeasible later.
        try {
            //sort
            PriorityQueue<Action> actions = new PriorityQueue<Action>(createActions(toBeMovedPartitions));

            //needs to be auto sorted because a new action may be generated in the middle

            for (Action action : actions) {
                //do action
                action.doAction();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Action> createActions(List<Pair> toBeMovedPartitions) {
        List<Action> actions = new ArrayList<Action>(toBeMovedPartitions.size());
        for (Pair toBeMovedPartition : toBeMovedPartitions) {
            //switch space
            //check both replace and move

            MoveAction moveAction = createMoveAction(toBeMovedPartition);
            if (moveAction != null) {
                actions.add(moveAction);
            }

            ReplaceAction replaceAction = createReplaceAction(toBeMovedPartition);
            if (replaceAction != null) {
                actions.add(replaceAction);
            }

            //keep that these actions are corresponding and if one is done the other cannot be done
            if (moveAction != null && replaceAction != null) {
                moveAction.setOtherChoice(replaceAction);
                replaceAction.setOtherChoice(moveAction);
            }
        }
        return actions;
    }

    private ReplaceAction createReplaceAction(Pair toBeMovedPartition) {
        ReplaceAction replaceAction = null;
        final Pair replaceCandidatePair = findReplaceCandidate(toBeMovedPartition);
        if (replaceCandidatePair != null) {
            double replaceBenefit = toBeMovedPartition.getReplaceBenefit(replaceCandidatePair);
            if (replaceBenefit > 0) {
                replaceAction = new ReplaceAction(replaceBenefit, toBeMovedPartition, replaceCandidatePair);
            }
        }
        return replaceAction;
    }

    private Pair findReplaceCandidate(Pair toBeMovedPartition) {
        //replace
        final Switch newHost = toBeMovedPartition.link.getStart();
        return switchOverheads.get(newHost).getMostOverheadPartitionOnLink(
                toBeMovedPartition.link.getOtherSide());
    }

    private MoveAction createMoveAction(Pair toBeMovedPartition) {
        //move
        MoveAction moveAction = null;
        final double moveBenefit = toBeMovedPartition.getMoveBenefit();
        if (moveBenefit > 0) {
            moveAction = new MoveAction(moveBenefit, toBeMovedPartition);
        }
        return moveAction;
    }

    private Map<Link, Double> getPartitionTrafficOnSwitchPorts(Switch host, Set<Flow> flows) {
        Map<Link, Double> linkTraffic = new HashMap<Link, Double>();
        if (flows.size() > 0) {
            for (Link link : host.getLinks()) {

                link = link.getOtherSide();
                List<Flow> traffic = new ArrayList<Flow>(link.getFlows());
                traffic.retainAll(flows);//keep this partition flows
                double sum = 0;
                for (Flow flow : traffic) {//add traffic
                    sum += flow.getTraffic();
                }
                linkTraffic.put(link, sum);

            }
        }
        return linkTraffic;
    }

    *//*private Map<Link, Double> calculateLinkOverhead(Map<Link, Long> linkTraffics) {
        //compute mean and std
        double mean = 0;
        for (Long v : linkTraffics.values()) {
            mean += v;
        }
        mean /= linkTraffics.size();
        double std = 0;
        for (Long v : linkTraffics.values()) {
            std += Math.pow(v - mean, 2);
        }
        std /= linkTraffics.size();
        //get links that have larger traffic than mean+threshold*std
        Map<Link, Double> output = new HashMap<Link, Double>();
        for (Map.Entry<Link, Long> linkLongEntry : linkTraffics.entrySet()) {
            Link link = linkLongEntry.getKey();
            Long v = linkLongEntry.getValue();
            if (v > (mean + threshold * std)) {
                output.put(link, ());
            }
        }
        return output;
    }*//*


    private abstract class Action implements Comparable<Action> {
        private Action otherChoice;
        private double benefit;
        private boolean enable = true;
        protected Pair toBeMovedPartition;

        protected Action(double benefit, Pair toBeMovedPartition) {
            this.benefit = benefit;
            this.toBeMovedPartition = toBeMovedPartition;
        }

        public int compareTo(Action o) {
            return (int) (benefit - o.benefit);
        }

        public void doAction() {
            if (!enable) {
                return;
            }
            try {
                if (doSuccessfully() && otherChoice != null) {
                    otherChoice.disable();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        protected abstract boolean doSuccessfully() throws Exception;


        public void disable() {
            enable = false;
        }

        public void setOtherChoice(Action otherChoice) {
            this.otherChoice = otherChoice;
        }

        protected void updateFlows(Pair pair) {
            final Link otherSideLink = pair.link.getOtherSide();
            // Don't need to run routing again as it is important to just have path
            final List<Flow> oldLinkFlows = pair.link.getFlows();
            final Set<Flow> partitionFlows = partitionTraffic.get(pair.partition);
            Set<Flow> otherTrafficFlows = new HashSet<Flow>(partitionFlows);
            otherTrafficFlows.removeAll(oldLinkFlows);

            //update oldlink
            for (Flow flow : partitionFlows) {
                oldLinkFlows.remove(flow);
            }
            pair.link.setUsedCapacity((long) (pair.link.getUsedCapacity() - pair.traffic));

            //update new link
            final List<Flow> newLinkFlows = otherSideLink.getFlows();
            newLinkFlows.addAll(otherTrafficFlows);
            otherSideLink.setUsedCapacity((long) (otherSideLink.getUsedCapacity() + pair.otherTraffic));
        }
    }

    private class MoveAction extends Action {


        protected MoveAction(double benefit, Pair toBeMovedPartition) {
            super(benefit, toBeMovedPartition);
        }

        @Override
        protected boolean doSuccessfully() throws Exception {
            final Switch oldHost = toBeMovedPartition.host;
            if (!switchPartitionMap.get(oldHost).contains(toBeMovedPartition.partition)) {
                //the scheduled partition is not there anymore!
                return false;
            }


            final Switch newHost = toBeMovedPartition.getNewHost();
            final List<Rule> newHostFineRules = new ArrayList<Rule>(newHost.getFineRules());
            final Rule oldForwardingRule = toBeMovedPartition.partition.getForwardingRule(oldHost);
            newHostFineRules.remove(oldForwardingRule);
            newHostFineRules.addAll(toBeMovedPartition.partition.getRules());
            List<Rule> newHostAggregatedRules = newHost.aggregate(newHostFineRules);
            boolean moveSpaceFeasibility = newHostAggregatedRules.size() <= newHost.getMemoryCapacity();
            //TODO check all sources of traffics, change the forwarding rule and reagregate for feasibility
            //link feasibility
            // to link must support additional pair.othertraffic
            final Link otherSideLink = toBeMovedPartition.link.getOtherSide();
            boolean moveLinkFeasibility = otherSideLink.getUsedCapacity() +
                    toBeMovedPartition.otherTraffic <= toBeMovedPartition.link.getCapacity();


            if (moveSpaceFeasibility && moveLinkFeasibility) {
                {
                    //do the update
                    switchPartitionMap.get(oldHost).remove(toBeMovedPartition.partition);
                    switchPartitionMap.get(newHost).add(toBeMovedPartition.partition);

                    newHost.setAggregatedRules(newHostAggregatedRules);
                    newHost.setFineRules(newHostFineRules);
                    //newHost.reagregate(); no need to do this

                    oldHost.getFineRules().removeAll(toBeMovedPartition.partition.getRules());
                    boolean oldHostSeen = false;

                    final Rule newForwardingRule = toBeMovedPartition.partition.getForwardingRule(newHost);
                    for (Switch source : partitionSources.get(toBeMovedPartition.partition)) {
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
                    }
                }
                updateFlows(toBeMovedPartition);
            }

            return moveSpaceFeasibility && moveLinkFeasibility;
        }
    }

    private class ReplaceAction extends Action {
        Pair replaceCandidatePair;

        public ReplaceAction(double benefit, Pair toBeMovedPartition, Pair replaceCandidatePair) {
            super(benefit, toBeMovedPartition);
            this.replaceCandidatePair = replaceCandidatePair;
        }

        @Override
        protected boolean doSuccessfully() throws Exception {
            //check if the tobemoved and replacement are still there
            //if to be moved is not there abort
            final Switch oldHost = toBeMovedPartition.host;
            final Switch newHost = replaceCandidatePair.host;
            if (!switchPartitionMap.get(oldHost).contains(toBeMovedPartition.partition)) {
                return false;
            } else {
                if (!switchPartitionMap.get(newHost).contains(replaceCandidatePair.partition)) {
                    //TODO find new replacement
                    return false;
                }
            }


            //space feasibility

            final Rule tobeMovedForwardingRule = toBeMovedPartition.partition.getForwardingRule(oldHost);
            final Rule replaceForwardingRule = replaceCandidatePair.partition.getForwardingRule(newHost);

            //check old host
            List<Rule> oldHostFineRules = new ArrayList<Rule>(oldHost.getFineRules());
            oldHostFineRules.removeAll(toBeMovedPartition.partition.getRules());
            oldHostFineRules.remove(replaceForwardingRule);
            oldHostFineRules.addAll(replaceCandidatePair.partition.getRules());
            if (partitionSources.get(toBeMovedPartition.partition).contains(oldHost)) {
                //need to add a forwarding rule to the oldhost rules
                oldHostFineRules.add(tobeMovedForwardingRule);
            }
            final List<Rule> oldHostAggregateRules = oldHost.aggregate(oldHostFineRules);
            int newSize1 = oldHostAggregateRules.size();
            if (newSize1 > oldHost.getMemoryCapacity()) {
                //TODO find new replacement
                return false;
            }

            List<Rule> newHostFineRuleList = new ArrayList<Rule>(newHost.getFineRules());
            newHostFineRuleList.removeAll(replaceCandidatePair.partition.getRules());
            newHostFineRuleList.remove(tobeMovedForwardingRule);
            newHostFineRuleList.addAll(toBeMovedPartition.partition.getRules());
            if (partitionSources.get(replaceCandidatePair.partition).contains(newHost)) {
                newHostFineRuleList.add(replaceForwardingRule);
            }
            final List<Rule> newHostAggregateRules = newHost.aggregate(newHostFineRuleList);
            int newSize2 = newHostAggregateRules.size();
            if (newSize2 > newHost.getMemoryCapacity()) {
                //TODO find new replacement
                return false;
            }
            //link feasibility
            // from link must support replacepair.othertraffic - pair.traffic
            // to link must support pair.othertraffic - replacepair.traffic
            final double fromLinkNewTraffic = toBeMovedPartition.link.getUsedCapacity() + replaceCandidatePair.otherTraffic - toBeMovedPartition.traffic;
            final double toLinkNewTraffic = replaceCandidatePair.link.getUsedCapacity() + toBeMovedPartition.otherTraffic - replaceCandidatePair.traffic;
            boolean replaceLinkFeasibility = fromLinkNewTraffic <= toBeMovedPartition.link.getCapacity() && toLinkNewTraffic <= replaceCandidatePair.link.getCapacity();

            if (!replaceLinkFeasibility) {
                //TODO find new replacement

                return false;
            }

            {
                //Commit changes
                {
                    oldHost.setFineRules(oldHostFineRules);
                    oldHost.setAggregatedRules(oldHostAggregateRules);
                    newHost.setFineRules(newHostFineRuleList);
                    newHost.setAggregatedRules(newHostAggregateRules);

                    final Rule newToBeMovedForwardingRule = toBeMovedPartition.partition.getForwardingRule(newHost);
                    for (Switch source : partitionSources.get(toBeMovedPartition.partition)) {
                        if (source.equals(oldHost) || source.equals(newHost)) {
                            continue;
                        }
                        final List<Rule> fineRules = source.getFineRules();
                        fineRules.remove(tobeMovedForwardingRule);
                        fineRules.add(newToBeMovedForwardingRule);
                        source.setFineRules(fineRules);
                        source.reagregate();
                    }

                    final Rule newReplaceForwardingRule = replaceCandidatePair.partition.getForwardingRule(newHost);
                    for (Switch source : partitionSources.get(replaceCandidatePair.partition)) {
                        if (source.equals(oldHost) || source.equals(newHost)) {
                            continue;
                        }
                        final List<Rule> fineRules = source.getFineRules();
                        fineRules.remove(replaceForwardingRule);
                        fineRules.add(newReplaceForwardingRule);
                        source.setFineRules(fineRules);
                        source.reagregate();
                    }
                }

                updateFlows(toBeMovedPartition);
                updateFlows(replaceCandidatePair);
            }

            return true;
        }
    }


    *//**
     * for each host, each partition has how much overhead on each link
     *//*
    private class SwitchOverhead {
        private Map<Partition, Map<Link, Double>> partitionOverhead = new HashMap<Partition, Map<Link, Double>>();
        private Switch host;

        public SwitchOverhead(Switch host) {
            this.host = host;
        }

        public void put(Partition partition, Map<Link, Double> overhead) {
            partitionOverhead.put(partition, overhead);
        }

        //maximum overhead maximizes x-(sum-x)


        public Pair getMostOverheadPartitionOnLink(Link fromLink) {
            //get from link
            if (fromLink == null) {
                throw new RuntimeException("return link not found");
            }
            double max = 0;
            Partition maxPartition = null;
            for (Map.Entry<Partition, Map<Link, Double>> partitionMapEntry : partitionOverhead.entrySet()) {
                final Double v = partitionMapEntry.getValue().get(fromLink);
                if (v != null && (v > max || maxPartition == null)) {
                    max = v;
                    maxPartition = partitionMapEntry.getKey();
                }
            }

            if (maxPartition == null) {
                if (partitionOverhead.size() > 0) {
                    //find a random partition
                    maxPartition = partitionOverhead.keySet().iterator().next();
                } else {
                    return null;
                }

            }

            //calculate other traffic for maximum
            double sum = 0;
            for (Double v : partitionOverhead.get(maxPartition).values()) {
                sum += v;
            }

            return new Pair(host, maxPartition, fromLink, max, sum - max);
        }

        *//**
         * @param alpha
         * @return a list of partitions on this host that there is a link that more than half of its traffic is
         *         coming from it.
         *//*
        public List<Pair> getAlphaSignificantCandidates(double alpha) {
            List<Pair> output = new ArrayList<Pair>();
            //for each partition
            for (Map.Entry<Partition, Map<Link, Double>> partitionMapEntry : partitionOverhead.entrySet()) {
                Partition partition = partitionMapEntry.getKey();
                Map<Link, Double> overhead = partitionMapEntry.getValue();
                //find maximum
                double max = 0;
                Link maxLink = null;
                double sum = 0;
                for (Map.Entry<Link, Double> linkOverheadEntry : overhead.entrySet()) {
                    final Double v = linkOverheadEntry.getValue();
                    sum += v;
                    if (max < v || maxLink == null) {
                        max = v;
                        maxLink = linkOverheadEntry.getKey();
                    }
                }
                sum -= max;
                if (sum < alpha * max) {
                    output.add(new Pair(host, partition, maxLink, max, sum));
                }
            }
            return output;
        }

        public List<Pair> getAlphaSTDCandidates(double alpha) {
            List<Pair> output = new ArrayList<Pair>();
            for (Map.Entry<Partition, Map<Link, Double>> partitionMapEntry : partitionOverhead.entrySet()) {
                Map<Link, Double> linkOverhead = partitionMapEntry.getValue();

                //compute mean and std
                double mean = 0;
                double max = 0;
                Link maxLink = null;
                for (Map.Entry<Link, Double> linkDoubleEntry : linkOverhead.entrySet()) {
                    Double v = linkDoubleEntry.getValue();
                    if (max < v || maxLink == null) {
                        maxLink = linkDoubleEntry.getKey();
                        max = v;
                    }
                    mean += v;
                }
                mean /= partitionOverhead.size();
                double std = 0;
                for (Double v : linkOverhead.values()) {
                    std += Math.pow(v - mean, 2);
                }
                std /= partitionOverhead.size();
                std = Math.sqrt(std);
                //if max link is larger than mean+threshold* std
                if (max > mean + alpha * std) {
                    output.add(new Pair(host, partitionMapEntry.getKey(), maxLink, max, mean * partitionOverhead.size()));
                }
            }
            return output;
        }
    }

    private class Pair {
        Switch host;
        Partition partition;
        Link link;
        double traffic;
        double otherTraffic;

        private Pair(Switch host, Partition partition, Link link, double traffic, double otherTraffic) {
            this.host = host;
            this.partition = partition;
            this.link = link;
            this.traffic = traffic;
            this.otherTraffic = otherTraffic;
        }

        private double getMoveBenefit() {
            return traffic - otherTraffic;
        }

        private double getReplaceBenefit(Pair replaceCandidate) {
            return getMoveBenefit() + replaceCandidate.getMoveBenefit();
        }

        public Switch getNewHost() {
            return link.getStart();
        }
    }*/
}
