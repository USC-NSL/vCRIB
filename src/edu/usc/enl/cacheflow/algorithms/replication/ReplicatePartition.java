package edu.usc.enl.cacheflow.algorithms.replication;

import edu.usc.enl.cacheflow.algorithms.replication.switchselectionpartition.SwitchReplicateSelectionPartition;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.CollectionPool;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 7/9/12
 * Time: 8:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class ReplicatePartition extends Replicator {
    private final SwitchReplicateSelectionPartition switchSelection;
    private final CollectionPool<Collection<Switch>> tempSwitchesPool;

    public ReplicatePartition(SwitchReplicateSelectionPartition switchSelection,
                              Topology topology,
                              Map<Partition, Long> minTraffics, Map<Partition, Collection<Switch>> partitionSources,
                              Map<Partition, Map<Switch, Rule>> forwardingRules, CollectionPool<Collection<Switch>> tempSwitchesPool) {
        super(minTraffics, topology,  partitionSources,  forwardingRules);
        this.switchSelection = switchSelection;
        this.tempSwitchesPool = tempSwitchesPool;
    }

    @Override
    public String toString() {
        return "Replicate Partition";
    }

    public Map<Partition, Map<Switch, Switch>> postPlace(Set<Switch> availableSwitches, Assignment assignment, PrintWriter replicateTrendWriter) {
        Collection<Partition> partitions = partitionSources.keySet();


        Map<Partition, Set<Switch>> partitionReplicas = new HashMap<Partition, Set<Switch>>(partitions.size(), 1);
        fillInitialPartitionReplicas(partitions, assignment, partitionReplicas);

        Map<Partition, Map<Switch, Switch>> partitionSourceReplica = new HashMap<Partition, Map<Switch, Switch>>(partitions.size(), 1);
        fillInitialPartitionSourceReplica(partitionSources, partitionReplicas, partitionSourceReplica);

        long currentTrafficSum = 0;
        final Map<Partition, Long> overhead = new HashMap<Partition, Long>();
        for (Partition partition : partitions) {
            Long minTraffic = minTraffics.get(partition);
            long ov = computeInitialOverhead(partition, partitionSourceReplica.get(partition), minTraffics.get(partition));
            overhead.put(partition, ov);
            currentTrafficSum += ov + minTraffic;
        }

        double currentResourceUsage = 0;
        for (Switch aSwitch : topology.getSwitches()) {
            currentResourceUsage += aSwitch.getUsedResources(aSwitch.getState());
        }
        replicateTrendWriter.println(currentTrafficSum + "," + currentResourceUsage + ",");

        /////////////////////////////////////////////

        PriorityQueue<Partition> partitionsOverheadSorted = new PriorityQueue<Partition>(partitions.size(), new Comparator<Partition>() {
            public int compare(Partition o1, Partition o2) {
                return -(int) (overhead.get(o1) - overhead.get(o2));
            }
        });


        switchSelection.init(topology);
        partitionsOverheadSorted.addAll(partitions);

        Long start = System.currentTimeMillis();
        int i = 0;
        int maxSteps = -1;
        while (partitionsOverheadSorted.size() > 0) {
            i++;
            //try to migrateFrom head of queue
            Partition partition = partitionsOverheadSorted.poll();
            //sort switches
            Set<Switch> oldReplicas = partitionReplicas.get(partition);
            List<Switch> sortedSwitches = switchSelection.sortSwitches(availableSwitches, partition,
                    partitionSources.get(partition), oldReplicas);
            System.out.print(i + ": remained: " + partitionsOverheadSorted.size() + " - currently has: " + oldReplicas.size() +
                    " replicas, sort time:" + (System.currentTimeMillis() - start) / 1000.0);
            for (Switch candidateSwitch : sortedSwitches) {
                if (switchSelection.getTrafficIf(candidateSwitch) < overhead.get(partition) + minTraffics.get(partition)) {
                    System.out.print("\n    " + partition.hashCode() + " -> " + candidateSwitch);
                    //check feasibility
                    double oldResourceUsage = candidateSwitch.getUsedResources(candidateSwitch.getState());
                    if (isFeasible(partition, candidateSwitch)) {
                        oldReplicas.add(candidateSwitch);
                        partitionSourceReplica.put(partition, switchSelection.getSelectedReplicaIf(candidateSwitch));
                        long oldOverhead = overhead.get(partition);
                        long newOverhead = switchSelection.getTrafficIf(candidateSwitch) - minTraffics.get(partition);
                        overhead.put(partition, newOverhead);
                        System.out.print("overhead reduced by " + (oldOverhead - newOverhead));
                        currentResourceUsage += candidateSwitch.getUsedResources(candidateSwitch.getState()) - oldResourceUsage;
                        currentTrafficSum += newOverhead - oldOverhead;
                        replicateTrendWriter.println(currentTrafficSum + "," + currentResourceUsage + "," + candidateSwitch);
                        if (newOverhead > 0) {
                            partitionsOverheadSorted.add(partition);
                        }

                        pruneSwitches(availableSwitches);
                        break;
                    }
                }
            }
            System.out.println();
            if (maxSteps > 0 && i > maxSteps) {
                break;
            }
        }
        updateForwardingRules(partitionSourceReplica, forwardingRules);


        return partitionSourceReplica;
    }

    @Override
    public Statistics getStats(Map<String, Object> parameters) {
        return new Statistics();
    }

    private void fillInitialPartitionSourceReplica(Map<Partition, Collection<Switch>> partitionSources,
                                                   Map<Partition, Set<Switch>> partitionReplicas,
                                                   Map<Partition, Map<Switch, Switch>> partitionSourceReplica) {
        for (Map.Entry<Partition, Collection<Switch>> entry : partitionSources.entrySet()) {
            Map<Switch, Switch> sourceReplica = new HashMap<Switch, Switch>(entry.getValue().size());
            partitionSourceReplica.put(entry.getKey(), sourceReplica);
            Switch firstReplica = partitionReplicas.get(entry.getKey()).iterator().next();
            for (Switch source : entry.getValue()) {
                sourceReplica.put(source, firstReplica);
            }
        }
    }

    private long computeInitialOverhead(Partition partition, Map<Switch, Switch> sourceReplica, Long minOverhead) {
        Switch replica = sourceReplica.values().iterator().next();//only first replica
        long sum = topology.getTrafficForHosting(partition,replica);
        return sum - minOverhead;
    }

    private boolean isFeasible(Partition p, Switch s) {
        final Map<Switch, Switch> sourceReplica = switchSelection.getSelectedReplicaIf(s);
        //final Map<Rule, List<Flow>> flows = ruleFlowMap.get(p);
        final CollectionPool.TempCollection<Collection<Switch>> tempSwitches = tempSwitchesPool.getTempCollection();
        for (Map.Entry<Switch, Switch> entry : sourceReplica.entrySet()) {
            if (entry.getValue().equals(s)){
                tempSwitches.getData().add(entry.getKey());
            }
        }

        // check the host of partition
        try {
            topology.getHelper(s).isAddFeasible(s, p,tempSwitches.getData() , true, true);
        } catch (Switch.InfeasibleStateException e) {
            e.printStackTrace();
            return false;
        }finally{
            tempSwitches.release();
        }

        return true;

    }

    private Map<Partition, Set<Switch>> fillInitialPartitionReplicas(Collection<Partition> partitions, Assignment assignment,
                                                                     Map<Partition, Set<Switch>> partitionReplicas) {
        Map<Partition, Switch> placement = assignment.getPlacement();
        for (Partition partition : partitions) {
            HashSet<Switch> oldReplicas = new HashSet<Switch>();
            partitionReplicas.put(partition, oldReplicas);
            oldReplicas.add(placement.get(partition));
        }
        return partitionReplicas;
    }

}
