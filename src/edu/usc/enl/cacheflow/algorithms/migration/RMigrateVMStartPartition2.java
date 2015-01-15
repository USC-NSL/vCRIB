package edu.usc.enl.cacheflow.algorithms.migration;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.factory.AssignmentFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.MemorySwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.OVSSwitch;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/23/12
 * Time: 4:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class RMigrateVMStartPartition2 extends AbstractMigrator {
    private final Random random;
    private final int movingAverageWindowSize;
    private final double stopThreshold;
    private final double beta;
    private final RandomMinTrafficSwitchSelection switchSelection;
    private final double chanceRatio;
    private QueuePartitionRandom[] partitionsList;
    private long minBecauseNoCapacityOnHost = 0;
    private final boolean infeasibleSameHost;

    public RMigrateVMStartPartition2(RandomMinTrafficSwitchSelection switchSelection,
                                     Topology topology,
                                     Map<Partition, Long> minTraffics,
                                     Map<Switch, Collection<Partition>> sourcePartitions, Map<Partition, Map<Switch, Rule>> forwardingRules,
                                     Random random,
                                     int movingAverageWindowSize, double stopThreshold, double beta, double chanceRatio,
                                     boolean infeasibleSameHost) {
        super(topology, minTraffics, sourcePartitions, forwardingRules);
        this.switchSelection = switchSelection;
        this.random = random;
        this.movingAverageWindowSize = movingAverageWindowSize;
        this.stopThreshold = stopThreshold;
        this.beta = beta;
        this.infeasibleSameHost = infeasibleSameHost;
        this.chanceRatio = chanceRatio;
    }

    public Map<Partition, Switch> postPlace2(Set<Switch> availableSwitches, Assignment assignment, PrintWriter trendWriter) {
        final Switch memorySwitch = topology.findEdges().get(0);
        boolean noCapacityOnHost = false;
        if (memorySwitch instanceof MemorySwitch) {
            noCapacityOnHost = ((MemorySwitch) memorySwitch).getMemoryCapacity() == 1024;
        }

        final Map<Partition, Switch> placement = assignment.getPlacement();
        final Map<Switch, Set<Partition>> rPlacement = assignment.getRplacement();
        final List<Switch> considerableSwitches = getAllConsiderableSwitches(placement.keySet());

        double currentTrafficSum = 0;
        double currentResourceUsage = 0;
        final int partitionsNum = placement.size();
        partitionsList = new QueuePartitionRandom[partitionsNum];
        double[] cumulativePartitionsOverheads = new double[partitionsNum];
        double[] movingAverageWindow = new double[movingAverageWindowSize];
        Arrays.fill(movingAverageWindow, 0d);
        double movingAverageSum = 0;
        Map<Switch, Long> allSwitchesZeroMap = new HashMap<Switch, Long>(considerableSwitches.size(), 1);
        boolean[] activePartitions = new boolean[partitionsNum];
        Arrays.fill(activePartitions, true);
        long minTrafficSum = 0;
        minBecauseNoCapacityOnHost = 0;
        //////////////  fill datastructures
        {
            int i = 0;
            for (Partition partition : placement.keySet()) {
                Long minTraffic = minTraffics.get(partition);
                minTrafficSum += minTraffic;
                ///can be received from placement but what if placement does not want to use traffic info?
                long ov = computeInitialOverhead(partition, placement.get(partition), minTraffic);
                currentTrafficSum += ov + minTraffic;
                //ovTraffic[1];
                overhead.put(partition, ov);
                partitionsList[i] = new QueuePartitionRandom(partition, true, ov);
                if (noCapacityOnHost) {
                    final Switch tor = forwardingRules.get(partition).keySet().iterator().next().getLinks().get(0).getEnd();
                    minBecauseNoCapacityOnHost += computeInitialOverhead(partition, tor, minTraffic);
                }
                i++;
            }

            try {
                updatePartitionRates(0, cumulativePartitionsOverheads,
                        selectDenom(currentTrafficSum, minTrafficSum, partitionsNum), activePartitions, partitionsList);
            } catch (InfiniteRateException e) {
                e.printStackTrace();
                System.exit(1);
            }

            for (Switch aSwitch : topology.getSwitches()) {
                currentResourceUsage += aSwitch.getUsedResources(aSwitch.getState());
            }

            for (Switch considerableSwitch : considerableSwitches) {
                allSwitchesZeroMap.put(considerableSwitch, 0l);
            }
        }
        switchSelection.init(considerableSwitches, this);

        //////////////////////////////////////////////// RUN

        trendWriter.println("," + currentResourceUsage + "," + currentTrafficSum + ",");

        int step = 0;
        int maxSteps = -1;
        int skipPartitions = 0;
        int activePartitionsNum = partitionsNum;
        migrationStart = System.currentTimeMillis();
        while (true) {
            if (skipPartitions >= activePartitionsNum) {
                break;
            }
            int partitionIndex = Util.randomSelect(cumulativePartitionsOverheads, random);
            if (!activePartitions[partitionIndex]) {
                System.out.println("No feasible partition anymore");
                break;
            }
            QueuePartitionRandom queuePartition = partitionsList[partitionIndex];
            Partition partition = queuePartition.getPartition();
            Switch oldHost = placement.get(partition);

            // check if old host accepts to migrateFrom this partition
            // only important if old host is a isSource
            // as partitions are VM start only one machine could be isSource
            Switch.FeasibleState oldHostFeasible = null;
            try {
                //rPlacement.get(oldHost).remove(partition);
                oldHostFeasible = isOldHostFeasible(rPlacement.get(oldHost), partition, oldHost, false);
                //rPlacement.get(oldHost).add(partition);
            } catch (Switch.InfeasibleStateException e) {
//                System.out.println("old host " + oldHost + " is not OK for moving partition " + partition + " out of it");
                //rPlacement.get(oldHost).add(partition);
                queuePartition.deActivateBySource();//need to be before deactivate
                activePartitions[partitionIndex] = false;
                activePartitionsNum--;
                if (queuePartition.isUseMyMap()) {
                    queuePartition.reDeActivate();
                } else {
                    // need all switches not only available switches as they may become available later
                    queuePartition.deActivate(Collections.<Switch, Long>emptyMap(), Long.MAX_VALUE);
                }
                //not select this partition again
                try {
                    updatePartitionRates( partitionIndex, cumulativePartitionsOverheads,
                            selectDenom(currentTrafficSum, minTrafficSum, partitionsNum), activePartitions, partitionsList);
                } catch (InfiniteRateException e2) {
                    e2.printStackTrace();
                    System.exit(1);
                }

//                System.out.println("skip old host not feasible");
                continue;
            }

            long newOverhead;
            long oldOverhead = overhead.get(partition);

            //pick a random switch
            int candidateSwitchIndex;

            Long minTraffic = minTraffics.get(partition);
            candidateSwitchIndex = switchSelection.getRandomFeasibleSwitch(partition,
                    minTraffic + oldOverhead, oldHost, considerableSwitches, false, null, selectDenom(currentTrafficSum, minTrafficSum, partitionsNum));
            Switch candidateSwitch = candidateSwitchIndex >= 0 ? considerableSwitches.get(candidateSwitchIndex) : null;
            if (candidateSwitchIndex < 0 || oldHost.equals(candidateSwitch)) {
//                if (infeasibleSameHost) {
                queuePartition.notDeActivateByHost();//need to be before deactivate
                activePartitions[partitionIndex] = false;
                activePartitionsNum--;
                if (queuePartition.isUseMyMap()) {
                    queuePartition.reDeActivate();
                } else {
                    // need all switches not only available switches as they may become available later
                    int i = 0;
                    for (Switch considerableSwitche : considerableSwitches) {
                        allSwitchesZeroMap.put(considerableSwitche, switchSelection.getTrafficIf(i));
                        i++;
                    }
                    queuePartition.deActivate(allSwitchesZeroMap, switchSelection.getBeta(),
                            selectDenom(currentTrafficSum, minTrafficSum, partitionsNum), chanceRatio, minTraffic + oldOverhead, oldHost);
                }
                try {
                    updatePartitionRates( partitionIndex, cumulativePartitionsOverheads,
                            selectDenom(currentTrafficSum, minTrafficSum, partitionsNum), activePartitions, partitionsList);
                } catch (InfiniteRateException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
//                } else {
//                    skipPartitions++;
//                }
                //System.out.println("skip migration not feasible");
                continue; //dont' consider in the movingaverage

            } else {
                //System.out.println(partition.hashCode() + " -> " + candidateSwitch);
                //get these numbers before updating
                double newHostOldResourceUsage = candidateSwitch.getUsedResources(candidateSwitch.getState());
                double oldHostOldResourceUsage = oldHost.getUsedResources(oldHost.getState());
                {
                    //update new host;
                    placement.put(partition, candidateSwitch);
                    Set<Partition> newSwitchPartitions = rPlacement.get(candidateSwitch);
                    if (newSwitchPartitions == null) {
                        newSwitchPartitions = new HashSet<Partition>();
                        rPlacement.put(candidateSwitch, newSwitchPartitions);
                    }
                    newSwitchPartitions.add(partition);
                    if (!isNewFeasible(partition, candidateSwitch, true)) { // I know it is feasible just update new host
                        throw new RuntimeException("not feasible candidate switch!");
                    }
                    rPlacement.get(oldHost).remove(partition);
                    //update old host
                    oldHost.setState(oldHostFeasible);

                }

                newOverhead = switchSelection.getTrafficIf(candidateSwitchIndex) - minTraffic;

                {
                    //updatestats;
//                    System.out.println(step + ": " + partition + " on: " + oldHost +
//                            ", sort time:" + (System.currentTimeMillis() - start) / 1000.0 + " to " + candidateSwitch +
//                            " overhead reduced by " + (oldOverhead - newOverhead));
                    currentResourceUsage += candidateSwitch.getUsedResources(candidateSwitch.getState()) - newHostOldResourceUsage;
                    currentResourceUsage += oldHost.getUsedResources(oldHost.getState()) - oldHostOldResourceUsage;
                    currentTrafficSum += newOverhead - oldOverhead;
                    trendWriter.println(candidateSwitch + "," + currentResourceUsage + "," + currentTrafficSum + "," + (System.currentTimeMillis() - start) / 1000.0);
                    //trendWriter.flush();
                }
                {
                    overhead.put(partition, newOverhead);
                    queuePartition.migrate(newOverhead);

                    //re-activate others that depend on the changed switches
                    boolean newHostIsOVSSource = false;
                    if (candidateSwitch instanceof OVSSwitch) {
                        final Collection<Partition> partitionsCandidateIsSourceOf = sourcePartitions.get(candidateSwitch);
                        newHostIsOVSSource = partitionsCandidateIsSourceOf != null &&
                                partitionsCandidateIsSourceOf.contains(partition);
                    }

                    int index = 0;
                    int minIndexUpdated = partitionIndex;
                    for (QueuePartitionRandom next : partitionsList) {//must go through all active and inactive
                        boolean nextIsActive = next.isActive();
                        next.activate(oldHost);
                        if (next.isDeActivateByHost() && placement.get(next.getPartition()).equals(oldHost)) {
                            next.activateByHost();
                        }
                        if (newHostIsOVSSource) {
                            next.activate(candidateSwitch);
                            if (next.isDeActivateByHost() && placement.get(next.getPartition()).equals(candidateSwitch)) {
                                next.activateByHost();
                            }
                        }
                        if (!nextIsActive && next.isActive()) {
                            activePartitions[index] = true;
                            activePartitionsNum++;
                            minIndexUpdated = minIndexUpdated > index ? index : minIndexUpdated;
                            //System.out.println(next + " got feasible ");
                        }
                        index++;
                    }
                    activePartitions[partitionIndex] = true;

                    try {
                        updatePartitionRates(minIndexUpdated, cumulativePartitionsOverheads,
                                selectDenom(currentTrafficSum, minTrafficSum, partitionsNum), activePartitions, partitionsList);
                    } catch (InfiniteRateException e) {
                        e.printStackTrace();
                        System.exit(1);
                    }

                }

            }

            ///////////////////////////////////////////////////////////
            //check feasibility
            /* {
                Map<Switch, Double> usage = new HashMap<>();
                for (Switch aSwitch : topology.getSwitches()) {
                    usage.put(aSwitch, aSwitch.getUsedResources(aSwitch.getState()));
                }
                topology.reset();//!!!
                try {
                    new AssignmentFactory.LoadPlacer(false, forwardingRules, assignment, sourcePartitions).place(topology, placement.keySet());
                    for (Switch aSwitch : topology.getSwitches()) {
                        final double newResourceUsage = aSwitch.getUsedResources(aSwitch.getState());
                        if (newResourceUsage != usage.get(aSwitch)) {
                            System.out.println();
                        }
                    }
                } catch (NoAssignmentFoundException e) {
                    e.printStackTrace();
                }
            }*/
            ///////////////////////////////////////////////////////////

            if (oldOverhead == newOverhead) {
                skipPartitions++;
                continue;
            }
            skipPartitions = 0;
            //check to finish
            if (maxSteps > 0 && step > maxSteps) {
                break;
            }
            movingAverageSum += oldOverhead - newOverhead;
            movingAverageSum -= movingAverageWindow[step % movingAverageWindowSize];
            movingAverageWindow[step % movingAverageWindowSize] = oldOverhead - newOverhead;
            if (step >= movingAverageWindowSize && movingAverageSum / movingAverageWindowSize < stopThreshold) {
                break;
            }

            step++;
        }

//        if (!checkPlacementSimilarity(placement,rPlacement)){
//            System.out.println();
//        }

//        for (Map.Entry<Switch, Collection<Partition>> entry : sourcePartitions.entrySet()) {
//            for (Partition partition : entry.getValue()) {
//                if (placement.get(partition).equals(entry.getKey()) &&//its on isSource
//                        entry.getKey().getState().getRules().contains(forwardingRules.get(partition).get(entry.getKey()))) {// and isSource contains forwarding rule
//                    System.out.println();
//                }
//            }
//        }

        return placement;
    }


    private double selectDenom(double currentTrafficSum, long minTrafficSum, int partitionsNum) {
        return 1.0 * (currentTrafficSum - minTrafficSum - minBecauseNoCapacityOnHost) / partitionsNum;
    }


    private QueuePartition getMaxActiveOverhead(QueuePartition[] partitions, boolean[] activePartitions) {
        QueuePartition maxPartition = null;
        int i = 0;
        for (QueuePartition queuePartition : partitions) {
            if ((maxPartition == null || maxPartition.getOverhead() < queuePartition.getOverhead()) && activePartitions[i]) {
                maxPartition = queuePartition;
            }
            i++;
        }
        return maxPartition;
    }

    private boolean checkPlacementSimilarity(Map<Partition, Switch> placement, Map<Switch, Set<Partition>> rPlacement) {
        for (Map.Entry<Switch, Set<Partition>> entry : rPlacement.entrySet()) {
            for (Partition partition : entry.getValue()) {
                if (!placement.get(partition).equals(entry.getKey())) {
                    return false;
                }
            }
        }
        for (Map.Entry<Partition, Switch> entry : placement.entrySet()) {
            if (!rPlacement.get(entry.getValue()).contains(entry.getKey())) {
                return false;
            }
        }
        return true;
    }

    public static double computeRate(double beta, double averagePartitionTraffic, double value) {
        return Math.exp(beta * value / averagePartitionTraffic);
    }

    public void updatePartitionRates(int start, double[] cumulativePartitionsOverheads, double averagePartitionTraffic,
                                     boolean[] activePartitions, QueuePartition[] queuePartitions
    ) throws InfiniteRateException {
        QueuePartition maxOverhead = getMaxActiveOverhead(partitionsList, activePartitions);
        double decrease = 0;
        if (maxOverhead != null) {
            decrease = maxOverhead.getOverhead();
        }
        double value;
        for (int i = start; i < cumulativePartitionsOverheads.length; i++) {
            if (activePartitions[i]) {
                value = computeRate(beta, averagePartitionTraffic, queuePartitions[i].getOverhead() - decrease);
            } else {
                value = 0;
            }
            cumulativePartitionsOverheads[i] = (i == 0 ? 0 : cumulativePartitionsOverheads[i - 1]) + value;
        }
        if (Double.isInfinite(cumulativePartitionsOverheads[cumulativePartitionsOverheads.length - 1])) {
            throw new InfiniteRateException();
        }
    }


    @Override
    public String toString() {
        return "Random Migrate Partition";
    }

    private class InfiniteRateException extends Exception {

    }
}
