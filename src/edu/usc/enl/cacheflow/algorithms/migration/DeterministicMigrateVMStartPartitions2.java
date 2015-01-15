package edu.usc.enl.cacheflow.algorithms.migration;

import edu.usc.enl.cacheflow.model.topology.switchhelper.SwitchHelper;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.TrafficAwareSwitchSelection;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.OVSSwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/23/12
 * Time: 4:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class DeterministicMigrateVMStartPartitions2 extends AbstractMigrator {
    private final TrafficAwareSwitchSelection switchSelection;

    public DeterministicMigrateVMStartPartitions2(TrafficAwareSwitchSelection switchSelection,
                                                  Topology topology, Map<Partition, Long> minTraffics,
                                                  Map<Switch, Collection<Partition>> sourcePartitions, Map<Partition, Map<Switch, Rule>> forwardingRules) {
        super(topology,   minTraffics, sourcePartitions, forwardingRules);
        this.switchSelection = switchSelection;
    }

    public Map<Partition, Switch> postPlace2(Set<Switch> availableSwitches, Assignment assignment, PrintWriter trendWriter) {
        Collection<Partition> partitions = forwardingRules.keySet();
        List<Switch> allConsiderableSwitches = getAllConsiderableSwitches(partitions);

        long currentTrafficSum = 0;
        Map<Partition, Switch> placement = assignment.getPlacement();
        Map<Switch, Set<Partition>> rPlacement = assignment.getRplacement();
        for (Partition partition : partitions) {
            Long minTraffic = minTraffics.get(partition);
            ///can be received from placement but what if placement does not want to use traffic info?
            long ov = computeInitialOverhead(partition, placement.get(partition), minTraffics.get(partition));
            overhead.put(partition, ov);
            currentTrafficSum += ov + minTraffic;
        }
        double currentResourceUsage = 0;
        for (Switch aSwitch : topology.getSwitches()) {
            currentResourceUsage += aSwitch.getUsedResources(aSwitch.getState());
        }
        trendWriter.println("," + currentResourceUsage + "," + currentTrafficSum + ",");

        ////////////////////////////////////////////////
        TreeSet<QueuePartition> partitionsOverheadSorted = new TreeSet<QueuePartition>();
        for (Partition partition : partitions) {
            final Long pOverhead = overhead.get(partition);
            if (pOverhead > 0) {
                partitionsOverheadSorted.add(new QueuePartition(partition, true, pOverhead));
            }
        }

        switchSelection.init(topology);

        int steps = 0;
        int skipped = 0;
        List<Switch> candidateSwitches = new ArrayList<Switch>(availableSwitches);
        final ArrayList<Switch> tempListOfSwitches = new ArrayList<Switch>(topology.getSwitches().size());
        Iterator<QueuePartition> itr = partitionsOverheadSorted.iterator();
        migrationStart = System.currentTimeMillis();
        while (itr.hasNext()) {
            QueuePartition queuePartition = itr.next();
            if (queuePartition.isActive() && queuePartition.getOverhead() > 0) {
                steps++;
                Partition partition = queuePartition.getPartition();
                Switch oldHost = placement.get(partition);

                List<Switch> sortedSwitches;
                if (queuePartition.isUseMyMap()) {
                    sortedSwitches = queuePartition.getCandidateSwitches(tempListOfSwitches);
                } else {
                    sortedSwitches = switchSelection.sortSwitches(candidateSwitches, placement, partition);
                }

                boolean foundABetterPlace = false;
                long oldOverhead = overhead.get(partition);
                final Long minTraffic = minTraffics.get(partition);
                final long oldTraffic = oldOverhead + minTraffic;
                for (Switch candidateSwitch : sortedSwitches) {
                    boolean shouldRun;
                    final long newTraffic;
                    //boolean usedq = false;
                    if (queuePartition.isUseMyMap()) {
                        shouldRun = queuePartition.isActive(candidateSwitch);
                        newTraffic = queuePartition.getTrafficIf(candidateSwitch);
                        // usedq = true;
                    } else {
                        newTraffic = switchSelection.getTrafficIf(candidateSwitch);
                        shouldRun = newTraffic < oldOverhead;
                    }
                    if (shouldRun) {

                        //System.out.println(  partition.hashCode() + " -> " + candidateSwitch);
                        //these two lines must be before check feasibility
                        double newHostOldResourceUsage = candidateSwitch.getUsedResources(candidateSwitch.getState());
                        double oldHostOldResourceUsage = oldHost.getUsedResources(oldHost.getState());
                        //check feasibility
                        if (isNewFeasible(partition, candidateSwitch,  true)) {
//                            System.out.print(steps + ": remained: " + partitionsOverheadSorted.size() + " skipped: " + skipped + "- currently on: " + oldHost +
//                                    ", sort time:" + (System.currentTimeMillis() - start) / 1000.0);

                            placement.put(partition, candidateSwitch);
                            Set<Partition> newSwitchPartitions = rPlacement.get(candidateSwitch);
                            if (newSwitchPartitions == null) {
                                newSwitchPartitions = new HashSet<Partition>();
                                rPlacement.put(candidateSwitch, newSwitchPartitions);
                            }
                            newSwitchPartitions.add(partition);
                            rPlacement.get(oldHost).remove(partition);
                            //update state of old host!
                            try {
                                isOldHostFeasible(rPlacement.get(oldHost), partition, oldHost, true); //old host must always be feasible to not save partition
                            } catch (Switch.InfeasibleStateException e) {
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }

                            long newOverhead = newTraffic - minTraffic;
                            overhead.put(partition, newOverhead);
//                            System.out.println(" overhead reduced by " + (oldOverhead - newOverhead));
                            currentResourceUsage += candidateSwitch.getUsedResources(candidateSwitch.getState()) - newHostOldResourceUsage;
                            currentResourceUsage += oldHost.getUsedResources(oldHost.getState()) - oldHostOldResourceUsage;
                            currentTrafficSum += newOverhead - oldOverhead;
                            trendWriter.println(candidateSwitch + "," + currentResourceUsage + "," + currentTrafficSum + "," +
                                    (System.currentTimeMillis() - start) / 1000.0);
                            //trendWriter.flush();
                            if (!availableSwitches.contains(oldHost)) {
                                availableSwitches.add(oldHost);
                                candidateSwitches.add(oldHost);
                            }
                            if (!candidateSwitch.canSaveMore()) {
                                availableSwitches.remove(candidateSwitch);
                                candidateSwitches.remove(candidateSwitch);
                            }

                            foundABetterPlace = true;
                            queuePartition.migrate(newOverhead);

                            //re-activate others that depend on the changed switches
                            boolean newHostIsOVSSource = false;
                            if (candidateSwitch instanceof OVSSwitch) {
                                final Collection<Partition> partitionsCandidateIsSourceOf = sourcePartitions.get(candidateSwitch);
                                newHostIsOVSSource = partitionsCandidateIsSourceOf != null &&
                                        partitionsCandidateIsSourceOf.contains(partition);
                            }
                            for (QueuePartition next : partitionsOverheadSorted) {//must go through all active and inactive
                                next.activate(oldHost);
                                if (newHostIsOVSSource) {
                                    next.activate(candidateSwitch);
                                }
                            }

                            itr.remove();

                            if (newOverhead > 0) {
                                partitionsOverheadSorted.add(queuePartition);
                            }
                            itr = partitionsOverheadSorted.iterator();
                            skipped = 0;

                            break;
                        }
                    } else {
                        break;
                    }
                }
                //System.out.println();

                if (!foundABetterPlace) {
                    //set partition inactive
                    if (queuePartition.isUseMyMap()) {
                        queuePartition.reDeActivate(sortedSwitches);
                    } else {
                        // need all switches not only available switches as they may become available later
                        if (sortedSwitches.size() < allConsiderableSwitches.size()) {
                            switchSelection.sortSwitches(allConsiderableSwitches, placement, partition);
                        }
                        queuePartition.deActivate(switchSelection.getTrafficMap(), oldTraffic);
                    }
                }

            } else {
                skipped++;
            }
        }

        return placement;
    }

    @Override
    public String toString() {
        return "Deterministic2 migrateFrom partition";
    }


}
