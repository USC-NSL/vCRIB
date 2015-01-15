package edu.usc.enl.cacheflow.algorithms.replication;

import edu.usc.enl.cacheflow.algorithms.replication.switchselectioncluster.SwitchReplicateSelectionCluster;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.rule.Cluster;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.Action;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchhelper.SwitchHelper;
import edu.usc.enl.cacheflow.model.topology.switchmodel.OVSSwitch;
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
public class ReplicateCluster extends Replicator {

    private final SwitchReplicateSelectionCluster switchSelection;
    private Collection<Cluster> clusters;
    private final CollectionPool<Collection<Switch>> tempSwitchesPool;

    public ReplicateCluster(SwitchReplicateSelectionCluster switchSelection,
                            Topology topology,
                            Collection<Cluster> clusters, Map<Partition, Long> minTraffics,
                            Map<Partition, Collection<Switch>> partitionSources,
                            Map<Partition, Map<Switch, Rule>> forwardingRules, CollectionPool<Collection<Switch>> tempSwitchesPool) {
        super(minTraffics, topology, partitionSources, forwardingRules);
        this.switchSelection = switchSelection;
        this.clusters = clusters;
        this.tempSwitchesPool = tempSwitchesPool;
    }

    @Override
    public String toString() {
        return "ReplicateCluster";
    }

    public Map<Partition, Map<Switch, Switch>> postPlace(Set<Switch> availableSwitches, Assignment assignment, PrintWriter replicateTrendWriter) {

        Map<Cluster, Long> minClusterTraffic = new HashMap<Cluster, Long>(clusters.size());
        for (Cluster cluster : clusters) {
            long sum = 0;
            for (Partition partition : cluster.getPartitions()) {
                sum += minTraffics.get(partition);
            }
            minClusterTraffic.put(cluster, sum);
        }

        Map<Cluster, Set<Switch>> clusterSources = fillClusterSources(clusters, partitionSources);

        Map<Cluster, Set<Switch>> clusterReplicas = new HashMap<Cluster, Set<Switch>>(clusters.size(), 1);
        fillInitialClusterReplicas(clusters, assignment, clusterReplicas);

        Map<Cluster, Map<Switch, Switch>> clusterSourceReplica = new HashMap<Cluster, Map<Switch, Switch>>(clusters.size(), 1);
        fillInitialClusterSourceReplica(clusterSources, clusterReplicas, clusterSourceReplica);

        long currentTrafficSum = 0;
        final Map<Cluster, Long> overhead = new HashMap<Cluster, Long>();
        for (Cluster cluster : clusters) {
            Long minov = minClusterTraffic.get(cluster);
            long ov = computeInitialOverhead(cluster, clusterSourceReplica.get(cluster), minov, topology );
            overhead.put(cluster, ov);
            currentTrafficSum += ov + minov;
        }
        double currentResourceUsage = 0;
        for (Switch aSwitch : topology.getSwitches()) {
            currentResourceUsage += aSwitch.getUsedResources(aSwitch.getState());
        }
        replicateTrendWriter.println(currentTrafficSum + "," + currentResourceUsage + ",");

        /////////////////////////////////////////////

        PriorityQueue<Cluster> clustersOverheadSorted = new PriorityQueue<Cluster>(clusters.size(), new Comparator<Cluster>() {
            public int compare(Cluster o1, Cluster o2) {
                return -(int) (overhead.get(o1) - overhead.get(o2));
            }
        });


        switchSelection.init(topology, availableSwitches);
        clustersOverheadSorted.addAll(clusters);

        Long start = System.currentTimeMillis();
        int i = 0;
        int maxSteps = -1;
        while (clustersOverheadSorted.size() > 0) {
            i++;
            //try to migrateFrom head of queue
            Cluster cluster = clustersOverheadSorted.poll();
            //sort switches
            Set<Switch> oldReplicas = clusterReplicas.get(cluster);
            List<Switch> sortedSwitches = switchSelection.sortSwitches(availableSwitches, cluster,
                    clusterSources.get(cluster), oldReplicas);
            System.out.print(i + ": remained: " + clustersOverheadSorted.size() + " - currently has: " + oldReplicas.size() +
                    " replicas, sort time:" + ((System.currentTimeMillis() - start) / 1000.0));
            for (Switch candidateSwitch : sortedSwitches) {
                if (switchSelection.getTrafficIf(candidateSwitch) < overhead.get(cluster) + minClusterTraffic.get(cluster)) {
                    //because of no useful switches and default max value we set
                    System.out.print("\n    " + cluster.hashCode() + " -> " + candidateSwitch);
                    //check feasibility
                    double oldNewHostResourceUsage = candidateSwitch.getUsedResources(candidateSwitch.getState());
                    if (isFeasible(cluster, candidateSwitch)) {
                        oldReplicas.add(candidateSwitch);
                        clusterSourceReplica.put(cluster, switchSelection.getSelectedReplicaIf(candidateSwitch));
                        long oldOverhead = overhead.get(cluster);
                        long newOverhead = switchSelection.getTrafficIf(candidateSwitch) - minClusterTraffic.get(cluster);

                        overhead.put(cluster, newOverhead);
                        System.out.print(" overhead reduced by " + (oldOverhead - newOverhead));
                        currentResourceUsage += candidateSwitch.getUsedResources(candidateSwitch.getState()) - oldNewHostResourceUsage;
                        currentTrafficSum += newOverhead - oldOverhead;
                        replicateTrendWriter.println(currentTrafficSum + "," + currentResourceUsage + "," + candidateSwitch);
                        replicateTrendWriter.flush();
                        if (newOverhead > 0) {
                            clustersOverheadSorted.add(cluster);
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

        Map<Partition, Map<Switch, Switch>> partitionSourceReplica = new HashMap<Partition, Map<Switch, Switch>>(partitionSources.size());
        for (Cluster cluster : clusters) {
            for (Partition partition : cluster.getPartitions()) {
                Map<Switch, Switch> sourceReplicaForPartition = new HashMap<Switch, Switch>();
                partitionSourceReplica.put(partition, sourceReplicaForPartition);
                Map<Switch, Switch> sourceReplica = clusterSourceReplica.get(cluster);
                for (Switch source : partitionSources.get(partition)) {
                    sourceReplicaForPartition.put(source, sourceReplica.get(source));
                }
            }
        }
        updateForwardingRules(partitionSourceReplica, forwardingRules);
        return partitionSourceReplica;
    }

    @Override
    public Statistics getStats(Map<String, Object> parameters) {
        return new Statistics();
    }


    protected static void fillInitialClusterSourceReplica(Map<Cluster, Set<Switch>> clusterSources, Map<Cluster, Set<Switch>> clusterReplicas
            , Map<Cluster, Map<Switch, Switch>> clusterSourceReplica) {
        for (Map.Entry<Cluster, Set<Switch>> entry : clusterSources.entrySet()) {
            Map<Switch, Switch> sourceReplica = new HashMap<Switch, Switch>(entry.getValue().size());
            clusterSourceReplica.put(entry.getKey(), sourceReplica);
            Switch firstReplica = clusterReplicas.get(entry.getKey()).iterator().next();
            for (Switch source : entry.getValue()) {
                sourceReplica.put(source, firstReplica);
            }
        }
    }

    protected static long computeInitialOverhead(Cluster cluster, Map<Switch, Switch> sourceReplica, Long minClusterOverhead,
                                                 Topology topology) {
        long sum = 0;
        for (Partition partition : cluster.getPartitions()) {
            Switch replica = sourceReplica.values().iterator().next();//only first replica
            sum += topology.getTrafficForHosting(partition, replica);
        }
        return sum - minClusterOverhead;
    }

    private boolean isFeasible(Cluster c, Switch s) {
        final Map<Switch, Switch> sourceReplica = switchSelection.getSelectedReplicaIf(s);
        final CollectionPool.TempCollection<Collection<Switch>> tempSwitches = tempSwitchesPool.getTempCollection();
        for (Map.Entry<Switch, Switch> entry : sourceReplica.entrySet()) {
            if (entry.getValue().equals(s)) {
                tempSwitches.getData().add(entry.getKey());
            }
        }
        final Collection<Partition> partitions = c.getPartitions();
        final Iterator<Partition> itr = partitions.iterator();
        final Switch.FeasibleState oldState = s.getState();
        try {
            if (itr.hasNext()) {
                final SwitchHelper<Switch> helper = topology.getHelper(s);
                Partition partition = itr.next();
                Switch.FeasibleState feasible = helper.isAddFeasible(s, partition, forwardingRules.get(partition).keySet(), true, false);
                s.setState(feasible);
                while (itr.hasNext()) {
                    partition = itr.next();
                    feasible = helper.isAddFeasible(s, partition, forwardingRules.get(partition).keySet(), true, true);
                }
            }
        } catch (Switch.InfeasibleStateException e) {
            s.setState(oldState);
            return false;
        } finally {
            tempSwitches.release();
        }

        return true;
    }

    private Map<Cluster, Set<Switch>> fillInitialClusterReplicas(Collection<Cluster> clusters, Assignment assignment,
                                                                 Map<Cluster, Set<Switch>> clusterReplicas) {
        Map<Partition, Switch> placement = assignment.getPlacement();
        for (Cluster cluster : clusters) {
            HashSet<Switch> oldReplicas = new HashSet<Switch>();
            clusterReplicas.put(cluster, oldReplicas);
            for (Partition partition : cluster.getPartitions()) {
                oldReplicas.add(placement.get(partition));
            }
        }
        return clusterReplicas;
    }

    private Map<Cluster, Set<Switch>> fillClusterSources(Collection<Cluster> clusters, Map<Partition, Collection<Switch>> partitionSources) {
        Map<Cluster, Set<Switch>> clusterSources = new HashMap<Cluster, Set<Switch>>();
        for (Cluster cluster : clusters) {
            HashSet<Switch> sources = new HashSet<Switch>();
            clusterSources.put(cluster, sources);
            for (Partition partition : cluster.getPartitions()) {
                sources.addAll(partitionSources.get(partition));
            }
        }
        return clusterSources;
    }


}
