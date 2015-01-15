package edu.usc.enl.cacheflow.algorithms.migration;

import edu.usc.enl.cacheflow.model.topology.switchhelper.SwitchHelper;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.TrafficAwareSwitchSelection;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
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
public class DeterministicMigrateVMStartPartitions extends AbstractMigrator {

    private final TrafficAwareSwitchSelection switchSelection;

    public DeterministicMigrateVMStartPartitions(TrafficAwareSwitchSelection switchSelection,
                                                 Topology topology,
                                                 Map<Partition, Long> minTraffics,
                                                 Map<Switch, Collection<Partition>> sourcePartitions, Map<Partition, Map<Switch, Rule>> forwardingRules) {
        super(topology, minTraffics, sourcePartitions, forwardingRules);
        this.switchSelection = switchSelection;

    }

    public Map<Partition, Switch> postPlace2(Set<Switch> availableSwitches, Assignment assignment, PrintWriter trendWriter) {
        /*for (Iterator<Switch> iterator = availableSwitches.iterator(); iterator.hasNext(); ) {
            Switch availableSwitch = iterator.next();
            if (!availableSwitch.canSaveMore()) {
                iterator.remove();
            }
        }*/
        switchTypeResourceUsage.clear();

        Map<Partition, Switch> placement = assignment.getPlacement();
        rPlacement = assignment.getRplacement();
        Collection<Partition> partitions = placement.keySet();

        long currentTrafficSum = 0;
        for (Partition partition : partitions) {
            Long minTraffic = minTraffics.get(partition);
            ///can be received from placement but what if placement does not want to use traffic info?
            long ov = computeInitialOverhead(partition, placement.get(partition), minTraffics.get(partition));
            overhead.put(partition, ov);
            currentTrafficSum += ov + minTraffic;
        }
        double currentResourceUsage = 0;
        currentResourceUsage = initCurrentResourceUsage();

        ////////////////////////////////////////////////

        PriorityQueue<Partition> partitionsOverheadSorted = new PriorityQueue<Partition>(partitions.size(), new Comparator<Partition>() {
            public int compare(Partition o1, Partition o2) {
                return -Long.compare(overhead.get(o1), overhead.get(o2));
            }
        });

        switchSelection.init(topology);
        partitionsOverheadSorted.addAll(partitions);
        int i = 0;
        List<Switch> candidateSwitches = new ArrayList<Switch>(topology.getSwitches());
        for (Iterator<Switch> iterator = candidateSwitches.iterator(); iterator.hasNext(); ) {
            Switch next = iterator.next();
            if (next instanceof MemorySwitch && ((MemorySwitch) next).getMemoryCapacity() == 0) {
                iterator.remove();
            }
        }
        trendWriter.println(","+getSwitchTypeResourceUsage() +  currentResourceUsage + "," + currentTrafficSum + "," + (System.currentTimeMillis() - start));
        migrationStart = System.currentTimeMillis();
        while (partitionsOverheadSorted.size() > 0) {
            i++;
            //try to migrateFrom head of queue
            Partition partition = partitionsOverheadSorted.poll();
            //NO NEED TO CHECK OLD HOST FEASIBILITY AS THE OLD HOST CANNOT BE SRC BECAUSE 1) DETERMINISTIC AND 0 OVERHEAD SKIP
            // 2) VMSTART PARTITIONS SO 1 SRC PER PARTITION

            final Long oldOverhead = overhead.get(partition);
            if (oldOverhead == 0) {
                continue;
            }
            Switch oldHost = placement.get(partition);
            List<Switch> sortedSwitches = switchSelection.sortSwitches(candidateSwitches, placement, partition);
//            System.out.print(i + ": remained: " + partitionsOverheadSorted.size() + " - " + partition + " currently on: " + oldHost +
//                    ", sort time:" + (System.currentTimeMillis() - start) / 1000.0);
            final Long minTraffic = minTraffics.get(partition);
            final long oldTraffic = oldOverhead + minTraffic;
            for (Switch candidateSwitch : sortedSwitches) {
                final long newTraffic = switchSelection.getTrafficIf(candidateSwitch);
                if (newTraffic < oldTraffic) {
//                    System.out.print("\n    " + partition.hashCode() + " -> " + candidateSwitch);
                    //this line must be before check feasibility
                    double newHostOldResourceUsage = candidateSwitch.getUsedResources(candidateSwitch.getState());
                    double oldHostOldResourceUsage = oldHost.getUsedResources(oldHost.getState());
                    double newHostOldResourceAbsUsage = candidateSwitch.getUsedAbsoluteResources(candidateSwitch.getState());
                    double oldHostOldResourceAbsUsage = oldHost.getUsedAbsoluteResources(oldHost.getState());
                    //check feasibility
                    Set<Partition> newSwitchPartitions = rPlacement.get(candidateSwitch);
                    if (newSwitchPartitions == null) {
                        newSwitchPartitions = new HashSet<Partition>();
                        rPlacement.put(candidateSwitch, newSwitchPartitions);
                    }
                    newSwitchPartitions.add(partition);
                    if (isNewFeasible(partition, candidateSwitch, true)) {
                        System.out.println(i+": remained" + partitionsOverheadSorted.size()+" "+(System.currentTimeMillis()-migrationStart));
                        placement.put(partition, candidateSwitch);

                        rPlacement.get(oldHost).remove(partition);
                        //update state of old host!
                        try {
                            isOldHostFeasible(rPlacement.get(oldHost), partition, oldHost, true); //old host must always be feasible to not save partition because
                            // only isSource cares about it and if it is isSource deterministic will not move it
                        } catch (Switch.InfeasibleStateException e) {
                            e.printStackTrace();
                        }

                        long newOverhead = newTraffic - minTraffic;
                        overhead.put(partition, newOverhead);
//                        System.out.print(" move to " + candidateSwitch + " overhead reduced by " + (oldOverhead - newOverhead));
                        currentResourceUsage += candidateSwitch.getUsedResources(candidateSwitch.getState()) - newHostOldResourceUsage;
                        currentResourceUsage += oldHost.getUsedResources(oldHost.getState()) - oldHostOldResourceUsage;
                        switchTypeResourceUsage.put(candidateSwitch.getClass(), switchTypeResourceUsage.get(candidateSwitch.getClass()) + candidateSwitch.getUsedAbsoluteResources(candidateSwitch.getState())-newHostOldResourceAbsUsage);
                        switchTypeResourceUsage.put(oldHost.getClass(), switchTypeResourceUsage.get(oldHost.getClass()) + oldHost.getUsedAbsoluteResources(oldHost.getState())-oldHostOldResourceAbsUsage);

                        currentTrafficSum += newOverhead - oldOverhead;
                        trendWriter.println(candidateSwitch + "," + getSwitchTypeResourceUsage() + currentResourceUsage + "," + currentTrafficSum + "," + (System.currentTimeMillis() - migrationStart));
                        /*if (!availableSwitches.contains(oldHost)) { //keep available switches updated to make this contains fast on availableswitches
                            availableSwitches.add(oldHost);
                            candidateSwitches.add(oldHost);
                        }
                        if (!candidateSwitch.canSaveMore()) {
                            availableSwitches.remove(candidateSwitch);
                            candidateSwitches.remove(candidateSwitch);
                        }*/

                        if (newOverhead > 0) {
                            partitionsOverheadSorted.add(partition);
                        }

                        break;
                    } else {
                        newSwitchPartitions.remove(partition);
                    }
                } else {
                    break;
                }
            }
//            System.out.println();
        }
        trendWriter.println( "," + getSwitchTypeResourceUsage() + currentResourceUsage + "," + currentTrafficSum + "," + (System.currentTimeMillis() - migrationStart));

        return placement;
    }




    @Override
    public String toString() {
        return "Deterministic Migrate Partition";
    }
}
