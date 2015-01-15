package edu.usc.enl.cacheflow.algorithms.placement.online.completecompare;

import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.AbstractSwitch;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate.AbstractCandidate;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.candidate.MoveCandidate;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.model.topology.FatTree;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/17/12
 * Time: 12:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class NewNeighborReplacement {
    public Map<Partition, Collection<Flow>> partitionTraffic;
    public BigSwitchTopology newTopology;

    public Map<Partition, Switch> replace(Map<Partition, Switch> switchPartitionMap,
                                          Map<Partition, Map<Rule, Collection<Flow>>> matchedTraffic,
                                          Topology topology) {
        newTopology = new BigSwitchTopology((FatTree) topology);
        final Map<Partition, Collection<Flow>> denyFlows = new HashMap<Partition, Collection<Flow>>();
        partitionTraffic = divideTraffic(matchedTraffic, denyFlows);
        Map<Partition, Set<AbstractSwitch>> partitionSources = fillPartitionSourceMap();

        //compute benefit of movement for each partition on each link
        for (Map.Entry<Partition, Switch> partitionSwitchEntry : switchPartitionMap.entrySet()) {
            final Partition partition = partitionSwitchEntry.getKey();
            AbstractSwitch host = newTopology.get(partitionSwitchEntry.getValue());
            host.fillBenefitsForPartition(partitionTraffic.get(partition), partition, denyFlows.get(partition));
        }

        //find all candidates for each partition
        for (AbstractSwitch abstractSwitch : newTopology.getSwitches()) {
            abstractSwitch.createCandidates(true);
        }

        //find the best candidates
        PriorityQueue<AbstractCandidate> candidates = new PriorityQueue<AbstractCandidate>();
        for (Map.Entry<Partition, Switch> partitionSwitchEntry : switchPartitionMap.entrySet()) {
            final Partition partition = partitionSwitchEntry.getKey();
            AbstractSwitch host = newTopology.get(partitionSwitchEntry.getValue());
            final AbstractCandidate bestCandidateForPartition = host.getBestCandidateForPartition(partition);
            if (bestCandidateForPartition != null) {
                candidates.add(bestCandidateForPartition);
            }
        }

        //run movement
        try {
            int i = 0;
            final List<AbstractCandidate> newCandidates = new LinkedList<AbstractCandidate>();
            while (candidates.size() > 0) {
                final AbstractCandidate candidate = candidates.poll();
               // System.out.println(i + " try " + candidate);
                /*{
                    for (Partition partition : switchPartitionMap.keySet()) {
                        if (partition.hashCode() == -1307437218) {
                            System.out.println(switchPartitionMap.get(partition)+ " "+partitionTraffic.get(partition).size());
                            for (Link link : topology.getLinks()) {
                                final List<Flow> flows = new LinkedList<Flow>(link.getFlows());
                                final Collection<Flow> partitionFlows = partitionTraffic.get(partition);
                                flows.retainAll(partitionFlows);
                                System.out.println(link+" "+flows.size());
                            }
                            *//*Switch agg1 = topology.getSwitchMap().get("Agg_1");
                            Switch core2 = topology.getSwitchMap().get("Core_2");
                            Switch core1 = topology.getSwitchMap().get("Core_1");
                            
                            for (Link link : agg1.getLinks()) {
                                if (link.getEnd().equals(core1)) {
                                    final List<Flow> flows = new LinkedList<Flow>(link.getFlows());
                                    final Collection<Flow> partitionFlows = partitionTraffic.get(partition);
                                    flows.retainAll(partitionFlows);
                                    System.out.println(flows.size());
                                }
                                if (link.getEnd().equals(core2)) {
                                    final List<Flow> flows = new LinkedList<Flow>(link.getFlows());
                                    final Collection<Flow> partitionFlows = partitionTraffic.get(partition);
                                    flows.retainAll(partitionFlows);
                                    System.out.println(flows.size());
                                }
                            }*//*
                            break;
                        }
                    }
                }
                if (i == 8) {
                    System.out.println();
                }*/
                boolean result = candidate.run(switchPartitionMap, partitionSources,
                        partitionTraffic, denyFlows, newCandidates
                );
                if (result) {
                    //System.out.println(candidate.getBenefit());
                    long sum = 0;
                    for (Link link : topology.getLinks()) {
                        sum += link.getUsedCapacity();
                        /*if (link.getUsedCapacity() != link.getFlows().size()) {
                            System.out.println(link);
                        }*/
                    }
                    if (candidate instanceof MoveCandidate){
                        System.out.println(i+","+candidate.getBenefit() + ",," + sum);
                    }else{
                        System.out.println(i+",,"+candidate.getBenefit() + "," + sum);
                    }
                }

                if (newCandidates.size() > 0) {
                    i++;

                    for (AbstractCandidate abstractCandidate : newCandidates) {
                        if (abstractCandidate != null) {
                            candidates.add(abstractCandidate);
                            //System.out.println("got " + abstractCandidate);
                        }
                    }
                    newCandidates.clear();
                }

                if (i > 100) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return switchPartitionMap;
    }

    private Map<Partition, Set<AbstractSwitch>> fillPartitionSourceMap() {
        Map<Partition, Set<AbstractSwitch>> partitionSources = new HashMap<Partition, Set<AbstractSwitch>>();
        for (Map.Entry<Partition, Collection<Flow>> partitionMapEntry : partitionTraffic.entrySet()) {
            Set<AbstractSwitch> partitionSource = new HashSet<AbstractSwitch>();
            for (Flow flow : partitionMapEntry.getValue()) {
                partitionSource.add(newTopology.get(flow.getSource()));
            }
            partitionSources.put(partitionMapEntry.getKey(), partitionSource);
        }
        return partitionSources;
    }

    private Map<Partition, Collection<Flow>> divideTraffic(Map<Partition, Map<Rule, Collection<Flow>>> matchedTraffic,
                                                           Map<Partition, Collection<Flow>> denyFlows) {
        Map<Partition, Collection<Flow>> output = new HashMap<Partition, Collection<Flow>>();
        for (Map.Entry<Partition, Map<Rule, Collection<Flow>>> partitionMapEntry : matchedTraffic.entrySet()) {
            Partition partition = partitionMapEntry.getKey();
            final Collection<Flow> flowList = new HashSet<Flow>();
            final Collection<Flow> denyList = new HashSet<Flow>();
            for (Map.Entry<Rule, Collection<Flow>> ruleFlowsEntry : partitionMapEntry.getValue().entrySet()) {
                flowList.addAll(ruleFlowsEntry.getValue());
                if (ruleFlowsEntry.getKey().getAction() instanceof DenyAction) {
                    denyList.addAll(ruleFlowsEntry.getValue());
                }
            }
            denyFlows.put(partition, denyList);
            output.put(partition, flowList);
        }
        return output;
    }

}
