package edu.usc.enl.cacheflow.algorithms.migration.rmigration3;

import edu.usc.enl.cacheflow.algorithms.migration.AbstractMigrator;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/23/12
 * Time: 4:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class RMigrateVMStartPartition3 extends AbstractMigrator {
    private final int movingAverageWindowSize;
    private static final boolean printInfoTiming = false;
    private static final boolean printInfo = false;
    private final double alphaDownSteps;
    private final double stopThreshold;
    private final int initDownSteps;
    private final int threadNum;
    private final Map<Switch, Integer> switchIndexMap;
    private final long budget;
    private final long detBudget = 1000;


    private List<Switch> considerableSwitches;
    private List<QueuePartitionThread> queuePartitionThreads;
    private long[] maxValueForSwitch;
    private AllQueuePartition[] maxValueForSwitchPartition;

    public RMigrateVMStartPartition3(Topology topology,
                                     Map<Partition, Long> minTraffics,
                                     Map<Switch, Collection<Partition>> sourcePartitions, Map<Partition, Map<Switch, Rule>> forwardingRules,
                                     int threadNum, int movingAverageWindowSize, double stopThreshold, int initDownSteps, double alphaDownSteps, long budget) {
        super(topology, minTraffics, sourcePartitions, forwardingRules);
        this.movingAverageWindowSize = movingAverageWindowSize;
        this.stopThreshold = stopThreshold;
        this.budget = budget;
        switchIndexMap = new HashMap<>(topology.getSwitches().size(), 1);
        this.threadNum = threadNum;
        this.initDownSteps = initDownSteps;
        this.alphaDownSteps = alphaDownSteps;

    }

    public Map<Partition, Switch> postPlace2(Set<Switch> availableSwitches, Assignment assignment, PrintWriter trendWriter) {

        double avgDownSteps = initDownSteps;
        LinkedList<ThreadJob> jobQueue = new LinkedList<>();
        queuePartitionThreads = new ArrayList<>(threadNum);
        for (int i = 0; i < threadNum; i++) {
            final QueuePartitionThread thread = new QueuePartitionThread(jobQueue);
            queuePartitionThreads.add(thread);
            thread.start();
        }
        ///////////////////////////////////////////////////////////init datastructures
        final Map<Partition, Switch> placement = assignment.getPlacement();

        final Map<Switch, Set<Partition>> rPlacement = assignment.getRplacement();
        considerableSwitches = getAllConsiderableSwitches(placement.keySet());
        final int partitionsNum = placement.size();
        final Map<Partition, AllQueuePartition> partitionAllQueuePartitionMap = new HashMap<>(partitionsNum, 1);

        int batchThreadNumForPartitions = Math.min(partitionsNum, threadNum);
        int batchPartitionsPerThread = (int) Math.ceil(1.0 * partitionsNum / batchThreadNumForPartitions);
        int switchesNum = considerableSwitches.size();
        int batchThreadNumForSwitches = Math.min(switchesNum, threadNum);
        int batchSwitchesPerThread = (int) Math.ceil(1.0 * switchesNum / batchThreadNumForSwitches);

        final Map<Partition, Switch> bestPlacement = new HashMap<>(placement.size(), 1);
        long bestPlacementScore = 0;
        final Map<Switch, Switch.FeasibleState> bestState = new HashMap<>(considerableSwitches.size(), 1);
        final Set<Switch> changedSwitchesSinceBest = new HashSet<>(considerableSwitches);
        final Set<Partition> changedPartitionAssignmentsSinceBest = new HashSet<>(placement.keySet());

        long currentTrafficSum = 0;
        double currentResourceUsage = 0;

        double[] movingAverageWindow = new double[movingAverageWindowSize];
        Arrays.fill(movingAverageWindow, 0d);
        double movingAverageSum = 0;
        double[] switchRates = new double[switchesNum];
        int step = 0;

        //fill switch index
        switchIndexMap.clear();
        {
            int i = 0;
            for (Switch considerableSwitch : considerableSwitches) {
                switchIndexMap.put(considerableSwitch, i++);
            }
        }

        //////////////////////////////////////////////create allqueuepartitions
        final List<AllQueuePartition> allQueuePartitions = new ArrayList<>(partitionsNum);
        {
            partitionAllQueuePartitionMap.clear();
            int index = 0;
            for (Partition partition : placement.keySet()) {
                final AllQueuePartition allQueuePartition = new AllQueuePartition(
                        switchIndexMap.get(placement.get(partition)), partition, index, forwardingRules.get(partition).keySet().iterator().next());
                allQueuePartitions.add(allQueuePartition);
                partitionAllQueuePartitionMap.put(partition, allQueuePartition);
                index++;
            }
        }
        //fill the overheads
        {
            initThreads();
            synchronized (jobQueue) {
                jobQueue.add(new FillAndSortTraffic(allQueuePartitions.iterator(), false));
                jobQueue.add(new FillAllFeasible(allQueuePartitions.iterator(), false));
                jobQueue.add(new InitOldHostFeasibility(allQueuePartitions.iterator(), false));
                jobQueue.notifyAll();
            }
            //this computes traffic which is needed for overhead and rates
        }

        currentResourceUsage = initCurrentResourceUsage();
        ///////////////////////////////////////////////////////////////////// Run migration
        //prepare randomselection datastructure
        double[] cumulativePartitionsRates = new double[partitionsNum];
        Arrays.fill(cumulativePartitionsRates, 0);
        Util.IntegerWrapper updatedQueuePartitionMinIndex = new Util.IntegerWrapper(0);

        migrationStart = System.currentTimeMillis();
        long stepStart = migrationStart;

        joinQueuePartitionThreads();//traffic data is ready now

        boolean[] bestFeasibleChangedPartitions = new boolean[partitionsNum];
        Arrays.fill(bestFeasibleChangedPartitions, false);

        //fill initial stats, need traffics to be computed before
        for (AllQueuePartition allQueuePartition : allQueuePartitions) {
            long oldHostTraffic = allQueuePartition.getTraffic(allQueuePartition.getOldHostIndex());
            currentTrafficSum += oldHostTraffic;
        }
        bestPlacementScore = currentTrafficSum;

        maxValueForSwitch = new long[switchesNum];
        maxValueForSwitchPartition = new AllQueuePartition[switchesNum];

        {
            initThreads();
            synchronized (jobQueue) {
                jobQueue.add(new UpdateMaxValueForSwitchesJob(considerableSwitches.iterator(), allQueuePartitions, false));//needs traffic, TODO

                /* for (int i = 0; i < batchThreadNumForPartitions; i++) {
                    jobQueue.add(new ComputeRatesJob(allQueuePartitions.subList(i * batchPartitionsPerThread, (i + 1) * batchPartitionsPerThread).iterator(), 0, 0, true));
                }*/
                //jobQueue.add(new UpdateFeasibilityBoundaryJob(allQueuePartitions.iterator()));
                jobQueue.notifyAll();
            }

            joinQueuePartitionThreads();
            updatedQueuePartitionMinIndex.setValue(0);
        }

        bestPlacementScore = updateBestPlacement(placement, bestPlacement, bestState, currentTrafficSum, changedPartitionAssignmentsSinceBest, changedSwitchesSinceBest);
        trendWriter.println("," + getSwitchTypeResourceUsage() + currentResourceUsage + "," + currentTrafficSum + "," + (System.currentTimeMillis() - start));
        double stepsFromLastDecrease = avgDownSteps;

        migrationStart = System.currentTimeMillis();
        while (true) {


            long t = 0;

//            if (step == 0) {
//                migrationStart = System.currentTimeMillis();
//                trendWriter.println("," + currentResourceUsage + "," + currentTrafficSum + "," + (migrationStart - start));
//            }

            stepStart = System.currentTimeMillis();

            int qPartitionIndex = -1;
            double maxQPartitionRate = 0;
            double usableAvgDownSteps = avgDownSteps * (1 - alphaDownSteps) + (stepsFromLastDecrease + 1) * alphaDownSteps;
            {
                Arrays.fill(cumulativePartitionsRates, Double.NEGATIVE_INFINITY);
                double maxPositiveGain = 0;
                for (int i2 = 0, allQueuePartitionsSize = allQueuePartitions.size(); i2 < allQueuePartitionsSize; i2++) {
                    AllQueuePartition allQueuePartition = allQueuePartitions.get(i2);
                    if (allQueuePartition.bestFeasibleSwitchSortedIndex < switchesNum) {
                        int bestSwitchIndex = allQueuePartition.convertSortedIndexToSwitchIndex(allQueuePartition.bestFeasibleSwitchSortedIndex);
                        if (allQueuePartition.isOldHostFeasible2()) {
                            double maxRate = allQueuePartition.getRateForSwitch(allQueuePartition.getTraffic(allQueuePartition.oldHostIndex),
                                    bestSwitchIndex, usableAvgDownSteps);
                            cumulativePartitionsRates[i2] = maxRate;
                            long gain = allQueuePartition.traffic[allQueuePartition.oldHostIndex] - allQueuePartition.traffic[bestSwitchIndex];
                            if (gain > maxPositiveGain) {
                                maxPositiveGain = gain;
                            }
                        }

                    }
                }

                //find the queuepartition with maximum rate to the best candidate. This rate also includes the penalty/gain for new/old host
                for (int i1 = 0, cumulativePartitionsRatesLength = cumulativePartitionsRates.length; i1 < cumulativePartitionsRatesLength; i1++) {
                    double cumulativePartitionsRate = cumulativePartitionsRates[i1];
                    AllQueuePartition allQueuePartition = allQueuePartitions.get(i1);
                    if (allQueuePartition.isOldHostFeasible2() && (qPartitionIndex < 0 || maxQPartitionRate < cumulativePartitionsRate)) {
                        maxQPartitionRate = cumulativePartitionsRate;
                        qPartitionIndex = i1;
                    }
                }


                if (qPartitionIndex < 0 || maxQPartitionRate == Double.NEGATIVE_INFINITY) {
                    System.out.println("No best feasible for any partition");
                    break;
                }
            }
            if (printInfoTiming) {
                t = System.currentTimeMillis();
                System.out.println("Update CDF: M" + (t - migrationStart) + "  R" + (t - stepStart));
            }

            final AllQueuePartition queuePartition = allQueuePartitions.get(qPartitionIndex);
            final int oldHostIndex = queuePartition.getOldHostIndex();
            final Switch oldHost = considerableSwitches.get(oldHostIndex);


            Switch source = queuePartition.source;

            //prepare to select a target switch
            {
                //let's get serious and update those feasibilities that are not valid
                initThreads();
                synchronized (jobQueue) {
                    for (int i = 0; i < batchThreadNumForSwitches; i++) {
                        jobQueue.add(new ValidateFeasibilityForPartition(considerableSwitches.subList(i * batchSwitchesPerThread,
                                Math.min((i + 1) * batchSwitchesPerThread, considerableSwitches.size())).iterator(), true, queuePartition));
                    }
                    //jobQueue.add(new UpdateFeasibilityBoundaryJob(allQueuePartitions.iterator()));
                    jobQueue.notifyAll();
                }

                joinQueuePartitionThreads();//max rate wants validity
                queuePartition.computeRates(0, 0, usableAvgDownSteps);
            }

            int targetSwitchIndex = queuePartition.maxRateIndex;
            if (targetSwitchIndex < 0) {
                System.out.println("no other feasible. why this partition is selected? check if best was feasbile. " + step + " " + qPartitionIndex);
                throw new RuntimeException();
            }
            Switch targetSwitch = considerableSwitches.get(targetSwitchIndex);

            ///
            if (printInfo || step % 1000 == 0) {
                t = System.currentTimeMillis();
                System.out.println("Selection: M" + (t - migrationStart) + "  R" + (t - stepStart));
                System.out.print(step + ":moving avg " + movingAverageSum / movingAverageWindowSize + ": " + queuePartition +
                        ", to " + targetSwitch + " avgDownSteps was " + avgDownSteps);
            }
            //take old statistics of the switches
            double newHostOldResourceUsage = targetSwitch.getUsedResources(targetSwitch.getState());
            double oldHostOldResourceUsage = oldHost.getUsedResources(oldHost.getState());
            double newHostOldResourceAbsUsage = targetSwitch.getUsedAbsoluteResources(targetSwitch.getState());
            double oldHostOldResourceAbsUsage = oldHost.getUsedAbsoluteResources(oldHost.getState());
            long trafficChange = queuePartition.getTraffic(targetSwitchIndex) - queuePartition.getTraffic(oldHostIndex);

            final Set<Partition> hostedOnOldHost = rPlacement.get(oldHost);
            Set<Partition> hostedOnTargetHost = rPlacement.get(targetSwitch);
            //do migration
            try {
                hostedOnTargetHost = migrateOnSwitchAndPlacement(placement, rPlacement, queuePartition, oldHost, targetSwitchIndex,
                        targetSwitch, hostedOnOldHost, hostedOnTargetHost);
            } catch (Exception e) {
                System.out.println(step+": " + queuePartition +
                        ", to " + targetSwitch + "("+targetSwitchIndex+") " + " avgDownSteps was " + avgDownSteps);
                throw e;
            }

            if (printInfoTiming) {
                t = System.currentTimeMillis();
                System.out.println("Apply: M" + (t - migrationStart) + "  R" + (t - stepStart));
            }

            {
                //update queuepartitions

                //my rates depend on old host now that old host is changed I need to recompute them
                //final Iterator<AllQueuePartition> singletonItr = Collections.singleton(queuePartition).iterator();
                //queuePartition.computeRates(avgOverhead, maxCurrentOverhead);

                //reset mini index to CURRENT PARTITION as it rates will change
                //updatedQueuePartitionMinIndex.setValue(qPartitionIndex);

                //init threads
                initThreads();
                synchronized (jobQueue) {

                    //update migrated partition

                    // all rates will be recomputed so no bother to update rates
                    //jobQueue.offer(new ComputeRatesJob(singletonItr, 0, 0, false));
                    //its true that I didn't update feasibility of old and target host to true and false
                    //but this will be updated later when all partitions update those
                    //so this may do a little unnecessary tasks but
                    // hey this is not necessary, as target was feasible and WAS BEFORE BOUNDARY that I selected
                    //now it become infeasible when others update and certainly will update its boundary
                    //jobQueue.offer(new UpdateFeasibilityBoundaryJob(singletonItr));

                    //update partitions hosted on the old and target machine
                    boolean oldHostWasSource = source.equals(oldHost);
                    boolean targetHostIsSource = source.equals(targetSwitch);
                    jobQueue.offer(new UpdateOldHostQueuePartitionJob(new AllQueuePartitionIterator(hostedOnOldHost.iterator(), partitionAllQueuePartitionMap), hostedOnOldHost, oldHostWasSource, true, queuePartition));
                    jobQueue.offer(new UpdateOldHostQueuePartitionJob(new AllQueuePartitionIterator(hostedOnTargetHost.iterator(), partitionAllQueuePartitionMap), hostedOnTargetHost, targetHostIsSource, false, queuePartition));

                    //

                    //every other partition must update their is newfeasible
                    for (int i = 0; i < batchThreadNumForPartitions; i++) {
                        jobQueue.offer(new UpdateNewHostQueuePartitionJobTogether(allQueuePartitions.subList(i * batchPartitionsPerThread,
                                Math.min((i + 1) * batchPartitionsPerThread, allQueuePartitions.size())).iterator(),
                                targetSwitchIndex, oldHostWasSource, oldHostIndex, queuePartition, true, bestFeasibleChangedPartitions));
                    }
                    /*for (int i = 0; i < batchThreadNumForPartitions; i++) {
                        jobQueue.offer(new UpdateNewHostQueuePartitionJobTogether(allQueuePartitions.subList(i * batchPartitionsPerThread, (i + 1) * batchPartitionsPerThread).iterator(),
                                targetSwitchIndex, targetHostIsSource, false, queuePartition, false, bestFeasibleChangedPartitions));
                    }*/
                    jobQueue.notifyAll();

//                    UpdateNewHostQueuePartitionJob job1 = new UpdateNewHostQueuePartitionJob(allQueuePartitions.iterator(), updatedQueuePartitionMinIndex,
//                            oldHostIndex, oldHostWasSource, true, queuePartition, false, bestFeasibleChangedPartitions);
//                    while (job1.itr.hasNext()) {
//                        job1.run(job1.itr.next());
//
//                    }
//
//                    UpdateNewHostQueuePartitionJob job2 = new UpdateNewHostQueuePartitionJob(allQueuePartitions.iterator(), updatedQueuePartitionMinIndex,
//                            targetSwitchIndex, targetHostIsSource, false, queuePartition, false, bestFeasibleChangedPartitions);
//                    while (job2.itr.hasNext()) {
//                        job2.run(job2.itr.next());
//
//                    }

                }


                final double targetHostResourceChange = targetSwitch.getUsedResources(targetSwitch.getState()) - newHostOldResourceUsage;
                final double oldHostResourceChange = oldHost.getUsedResources(oldHost.getState()) - oldHostOldResourceUsage;
                {
                    //updatestats;
                    if (printInfo || step % 1000 == 0) {
                        System.out.println(" sort time:" + ((System.currentTimeMillis() - migrationStart) / 1000.0) + " overhead reduced by " + (-trafficChange));
                    }
                    currentResourceUsage += targetHostResourceChange;
                    currentResourceUsage += oldHostResourceChange;
                    currentTrafficSum += trafficChange;

                    /*if (trafficChange < 0) {
                        avgDownSteps = (1 - alphaDownSteps) * stepsFromLastDecrease + alphaDownSteps * avgDownSteps;
                        //System.out.println("avgDownSteps " + avgDownSteps);
                        stepsFromLastDecrease = 0;
                    } else {
                        stepsFromLastDecrease++;
                    }*/

                    switchTypeResourceUsage.put(targetSwitch.getClass(), switchTypeResourceUsage.get(targetSwitch.getClass()) + targetSwitch.getUsedAbsoluteResources(targetSwitch.getState()) - newHostOldResourceAbsUsage);
                    switchTypeResourceUsage.put(oldHost.getClass(), switchTypeResourceUsage.get(oldHost.getClass()) + oldHost.getUsedAbsoluteResources(oldHost.getState()) - oldHostOldResourceAbsUsage);
                    trendWriter.println(targetSwitch + "," + getSwitchTypeResourceUsage() + currentResourceUsage + "," + currentTrafficSum + "," + (System.currentTimeMillis() - migrationStart));
                    //trendWriter.flush();
                }

                changedSwitchesSinceBest.add(oldHost);
                changedSwitchesSinceBest.add(targetSwitch);
                changedPartitionAssignmentsSinceBest.add(queuePartition.getPartition());
                //update best //must be after updating currenttraficsum
                if (bestPlacementScore > currentTrafficSum) {
                    bestPlacementScore = updateBestPlacement(placement, bestPlacement, bestState, currentTrafficSum,
                            changedPartitionAssignmentsSinceBest, changedSwitchesSinceBest);
                }
                bestFeasibleChangedPartitions[queuePartition.index] = true;
                joinQueuePartitionThreads();

                if (printInfoTiming) {
                    t = System.currentTimeMillis();
                    System.out.println("Update feasibilities: M" + (t - migrationStart) + "  R" + (t - stepStart));
                }


                //now compute maximums
                AllQueuePartition maxForOldHost = maxValueForSwitchPartition[oldHostIndex];
                {

                    initThreads();
                    synchronized (jobQueue) {


                        //for those best feasible changed, for every switch they where max find max again
                        jobQueue.offer(new UpdateMaxForChangedBestPartitions(allQueuePartitions.iterator(), false, bestFeasibleChangedPartitions, allQueuePartitions));

                        /*for (AllQueuePartition bestFeasibleChangedPartition : allQueuePartitions) {
                            if (bestFeasibleChangedPartitions[bestFeasibleChangedPartition.index]) {
                                for (int switchIndex = 0; switchIndex < maxValueForSwitchPartition.length; switchIndex++) {
                                    if (bestFeasibleChangedPartition.equals(maxValueForSwitchPartition[switchIndex])) {
                                        updateMaxValueForSwitch(allQueuePartitions, switchIndex);
                                    }
                                }
                            }
                        }*/
                        jobQueue.notifyAll();
                    }
                    joinQueuePartitionThreads();

                    if (printInfoTiming) {
                        t = System.currentTimeMillis();
                        System.out.println("Update maxes1: M" + (t - migrationStart) + "  R" + (t - stepStart));
                    }
                    synchronized (jobQueue) {
                        initThreads();
                        jobQueue.offer(new UpdateMaxForChangedSwitches(allQueuePartitions.iterator(), false, bestFeasibleChangedPartitions, allQueuePartitions));
                        //for partitions best feasible changed, update max for every switch


                        /*for (int switchIndex = 0; switchIndex < maxValueForSwitchPartition.length; switchIndex++) {
                            for (AllQueuePartition bestFeasibleChangedPartition : allQueuePartitions) {
                                if (bestFeasibleChangedPartitions[bestFeasibleChangedPartition.index]) {
                                    updateMaxValueForSwitchForPartition(switchIndex, bestFeasibleChangedPartition);
                                }
                            }
                        }*/

                        jobQueue.notifyAll();
                    }
                    joinQueuePartitionThreads();
                    Arrays.fill(bestFeasibleChangedPartitions, false);
                    if (printInfoTiming) {
                        t = System.currentTimeMillis();
                        System.out.println("Update maxes2: M" + (t - migrationStart) + "  R" + (t - stepStart));
                    }
                }
                {
                    //update the alpha
                    if (maxValueForSwitchPartition[oldHostIndex] != maxForOldHost && maxValueForSwitchPartition[oldHostIndex] != queuePartition) {
                        avgDownSteps = (1 - alphaDownSteps) * stepsFromLastDecrease + alphaDownSteps * avgDownSteps;
                        System.out.println("avgDownSteps " + avgDownSteps);
                        stepsFromLastDecrease = 0;
                    } else if (maxValueForSwitchPartition[oldHostIndex] == maxForOldHost) {
                        stepsFromLastDecrease++;
                    }
                }

            }

            movingAverageSum += -trafficChange;
            movingAverageSum -= movingAverageWindow[step % movingAverageWindowSize];
            movingAverageWindow[step % movingAverageWindowSize] = -trafficChange;
            if (step >= movingAverageWindowSize && movingAverageSum / movingAverageWindowSize < stopThreshold) {
                System.out.println("moving average stop");
                break;
            }

            if (budget > 0 && (System.currentTimeMillis() - migrationStart > budget - detBudget)) {
                System.out.println("timeout");
                break;
            }


            if (printInfoTiming) {
                t = System.currentTimeMillis();
                System.out.println("Finish: M" + (t - migrationStart) + "  R" + (t - stepStart));
            }

            step++;


        }

        ////////////////////////////////////////////////////////////////////////////////////////////

        final Map<Switch, Set<Partition>> bestRPlacement = Assignment.generateRPlacement(bestPlacement);


        for (Switch considerableSwitch : considerableSwitches) {
            considerableSwitch.setState(bestState.get(considerableSwitch));
        }

        for (AllQueuePartition allQueuePartition : allQueuePartitions) {
            allQueuePartition.oldHostIndex = switchIndexMap.get(bestPlacement.get(allQueuePartition.getPartition()));
        }


        currentTrafficSum = bestPlacementScore;
        currentResourceUsage = initCurrentResourceUsage();
        trendWriter.println("," + getSwitchTypeResourceUsage() + currentResourceUsage + "," + currentTrafficSum + "," + (System.currentTimeMillis() - migrationStart));

        currentTrafficSum = greedyUpdate(bestPlacement, bestRPlacement, step, allQueuePartitions, stepStart, currentResourceUsage, currentTrafficSum, trendWriter);
        System.out.println("deterministic reduce traffic from " + bestPlacementScore + " to " + currentTrafficSum + " " + (bestPlacementScore - currentTrafficSum));

        initThreads();
        synchronized (jobQueue) {
            jobQueue.offer(new ExitThreadJob(allQueuePartitions.iterator()));
            jobQueue.notifyAll();
        }
        joinQueuePartitionThreads();

        return bestPlacement;
    }


    private long updateBestPlacement(Map<Partition, Switch> placement, Map<Partition, Switch> bestPlacement,
                                     Map<Switch, Switch.FeasibleState> bestState, long currentTrafficSum,
                                     Set<Partition> changedPartitionAssignmentsSinceBest, Set<Switch> changedSwitchesSinceBest) {
        for (Partition partition : changedPartitionAssignmentsSinceBest) {
            bestPlacement.put(partition, placement.get(partition));
        }
        for (Switch aSwitch : changedSwitchesSinceBest) {
            bestState.put(aSwitch, aSwitch.getState().clone());
        }
        changedPartitionAssignmentsSinceBest.clear();
        changedSwitchesSinceBest.clear();
        return currentTrafficSum;
    }

    private long greedyUpdate(Map<Partition, Switch> placement, Map<Switch, Set<Partition>> rPlacement, int step,
                              List<AllQueuePartition> allQueuePartitions, long stepStart, double currentResourceUsage,
                              long currentTrafficSum, PrintWriter trendWriter) {
        //now do very greedy
        final Map<AllQueuePartition, Long> maxOverhead = new HashMap<>(allQueuePartitions.size());
        PriorityQueue<AllQueuePartition> partitionsOverheadSorted = new PriorityQueue<AllQueuePartition>(allQueuePartitions.size(), new Comparator<AllQueuePartition>() {
            public int compare(AllQueuePartition o1, AllQueuePartition o2) {
                return -Long.compare(maxOverhead.get(o1), maxOverhead.get(o2));
            }
        });
        {
            for (AllQueuePartition allQueuePartition : allQueuePartitions) {
                long currentOverhead = allQueuePartition.getTraffic(allQueuePartition.oldHostIndex) - minTraffics.get(allQueuePartition.getPartition());
                maxOverhead.put(allQueuePartition, currentOverhead);
            }
        }

        partitionsOverheadSorted.addAll(allQueuePartitions);

        while (partitionsOverheadSorted.size() > 0 && (budget == 0 || System.currentTimeMillis() - migrationStart < budget)) {
            AllQueuePartition queuePartition = partitionsOverheadSorted.poll();
            if (maxOverhead.get(queuePartition) <= 0) {
                break;
            }
            Partition partition = queuePartition.partition;
            long oldTraffic = queuePartition.getTraffic(queuePartition.oldHostIndex);
            Switch oldHost = considerableSwitches.get(queuePartition.oldHostIndex);
            final Long oldOverhead = maxOverhead.get(queuePartition);
            for (Switch candidateSwitch : queuePartition.sortedSwitches) {
                if (printInfoTiming) {
                    System.out.println(partitionsOverheadSorted.size() + " move " + partition + " from " + oldHost + " to " + candidateSwitch);
                }
                final long newTraffic = queuePartition.getTraffic(switchIndexMap.get(candidateSwitch));
                if (newTraffic < oldTraffic) {
//                    int f = FeasiblePlacer.flowsNum(topology.getSwitchMap().get("Edge_12_12_05"), rPlacement.get(topology.getSwitchMap().get("Edge_12_12_05")), topology);
//                    if (f!= ((OVSSwitch.OVSState) topology.getSwitchMap().get("Edge_12_12_05").getState()).getNewFlows()){
//                        System.out.println(" mistake in new flows ");
//                    }
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
                        if (printInfoTiming) {
                            System.out.println("successful");
                        }
                        placement.put(partition, candidateSwitch);
                        queuePartition.oldHostIndex = switchIndexMap.get(candidateSwitch);
                        rPlacement.get(oldHost).remove(partition);
                        //update state of old host!
                        try {
                            isOldHostFeasible(rPlacement.get(oldHost), partition, oldHost, true); //old host must always be feasible to not save partition because
                            // only isSource cares about it and if it is isSource deterministic will not move it
                        } catch (Switch.InfeasibleStateException e) {
                            e.printStackTrace();
                        }

                        long newOverhead = newTraffic - minTraffics.get(partition);
                        maxOverhead.put(queuePartition, newOverhead);
//                        System.out.print(" move to " + candidateSwitch + " overhead reduced by " + (oldOverhead - newOverhead));
                        currentResourceUsage += candidateSwitch.getUsedResources(candidateSwitch.getState()) - newHostOldResourceUsage;
                        currentResourceUsage += oldHost.getUsedResources(oldHost.getState()) - oldHostOldResourceUsage;
                        currentTrafficSum += newOverhead - oldOverhead;
                        switchTypeResourceUsage.put(candidateSwitch.getClass(), switchTypeResourceUsage.get(candidateSwitch.getClass()) + candidateSwitch.getUsedAbsoluteResources(candidateSwitch.getState()) - newHostOldResourceAbsUsage);
                        switchTypeResourceUsage.put(oldHost.getClass(), switchTypeResourceUsage.get(oldHost.getClass()) + oldHost.getUsedAbsoluteResources(oldHost.getState()) - oldHostOldResourceAbsUsage);
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
                            partitionsOverheadSorted.add(queuePartition);
                        }

                        break;
                    } else {
                        newSwitchPartitions.remove(partition);
                    }
                } else {
                    break;
                }
            }
        }
        return currentTrafficSum;
    }

    private Set<Partition> migrateOnSwitchAndPlacement(Map<Partition, Switch> placement, Map<Switch, Set<Partition>> rPlacement,
                                                       AllQueuePartition queuePartition, Switch oldHost, int targetSwitchIndex,
                                                       Switch targetSwitch, Set<Partition> hostedOnOldHost, Set<Partition> hostedOnTargetHost) {
        Partition partition = queuePartition.getPartition();
        placement.put(partition, targetSwitch);
        if (hostedOnTargetHost == null) {
            hostedOnTargetHost = new HashSet<Partition>();
            rPlacement.put(targetSwitch, hostedOnTargetHost);
        }
        hostedOnTargetHost.add(partition);
        if (!isNewFeasible(partition, targetSwitch, true)) { // I know it is feasible just update new host
            throw new RuntimeException("not feasible candidate switch!");
        }
        hostedOnOldHost.remove(partition);
        //update old host
        try {
            isOldHostFeasible(hostedOnOldHost, partition, oldHost, true);
        } catch (Switch.InfeasibleStateException e) {
            System.out.println(queuePartition.isOldHostFeasible2());
            e.printStackTrace();
            System.exit(1);
        }

        queuePartition.migrate(targetSwitchIndex);
        return hostedOnTargetHost;
    }

    private void updateMaxValueForSwitch(Collection<AllQueuePartition> allQueuePartitions, int switchIndex) {
        maxValueForSwitch[switchIndex] = 0;
        maxValueForSwitchPartition[switchIndex] = null;
        for (AllQueuePartition allQueuePartition : allQueuePartitions) {
            if (allQueuePartition.bestFeasibleSwitchSortedIndex < considerableSwitches.size()) {
                updateMaxValueForSwitchForPartition(switchIndex, allQueuePartition);
            }
        }
    }

    private void updateMaxValueForSwitchForPartition(int switchIndex, AllQueuePartition allQueuePartition) {
        long v1 = 0;
        try {
            v1 = Math.max(0, allQueuePartition.getBestFeasibleSwitchTraffic() - allQueuePartition.traffic[switchIndex]);
        } catch (NoBestFeasibleSwitchException e) {
            //I don't have any feasible
            //System.out.println();
        }
        long v2 = Math.max(0, allQueuePartition.traffic[allQueuePartition.oldHostIndex] - allQueuePartition.traffic[switchIndex]);
        long v = Math.min(v1, v2);
        if (maxValueForSwitch[switchIndex] < v) {
            synchronized (maxValueForSwitch) {
                if (maxValueForSwitch[switchIndex] < v) {
                    maxValueForSwitchPartition[switchIndex] = allQueuePartition;
                    maxValueForSwitch[switchIndex] = v;
                }
            }
        }
    }

    private void initThreads() {
        for (QueuePartitionThread queuePartitionThread : queuePartitionThreads) {
            queuePartitionThread.setFinished(false);
        }
    }


    @Override
    public String toString() {
        return "Random Migrate Partition3";
    }

    private void joinQueuePartitionThreads() {
        try {
            for (QueuePartitionThread queuePartitionThread : queuePartitionThreads) {
                synchronized (queuePartitionThread) {
                    if (!queuePartitionThread.isFinished()) {
//                        System.out.println("start waiting for " + queuePartitionThread);
                        queuePartitionThread.wait();
//                        System.out.println("finish waiting for " + queuePartitionThread);
                    } else {
//                        System.out.println(queuePartitionThread + " was finished");
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private abstract class ThreadJob<T> {
        public Iterator<? extends T> itr;
        private final boolean batch;

        public ThreadJob(Iterator<? extends T> itr, boolean batch) {
            this.itr = itr;
            this.batch = batch;
        }

        public abstract <R extends T> void run(R t);

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }

        public boolean isBatch() {
            return batch;
        }
    }


    private class QueuePartitionThread extends Thread {
        private final LinkedList<ThreadJob> jobQueue;
        private boolean finished = false;

        private QueuePartitionThread(LinkedList<ThreadJob> jobQueue) {

            this.jobQueue = jobQueue;
        }

        public synchronized boolean isFinished() {
            return finished;
        }

        public synchronized void setFinished(boolean finished) {
            this.finished = finished;
        }

		//TODO: Fix this. It keeeps finished=true after coming out of wait. the join may pass this thread while they are still working
        @Override
        public void run() {
            while (true) {
                ThreadJob take = null;
                Object next = null;
                synchronized (jobQueue) {
                    try {
                        while (next == null) {
                            if (jobQueue.isEmpty()) {
                                //System.out.println(this + " set its finish to true and notify");
                                synchronized (this) {
                                    finished = true;
                                    take = null;
                                    this.notify();
                                }
                                //System.out.println(this + " is going to wait");
                                jobQueue.wait();
                                continue;//check the empty again as others may used up the queue
                            }
                            take = jobQueue.peek();
                            if (take instanceof ExitThreadJob) {
                                synchronized (this) {
//                                    System.out.println(this + " finished");
                                    finished = true;
                                    take = null;
                                    this.notify();
                                }
                                return;
                            }
                            synchronized (this) {
                                finished = false;
                            }
                            if (take.isBatch()) {
                                jobQueue.remove();
                                next = take.itr.next();
                            } else {

                                if (take.itr.hasNext()) {
                                    next = take.itr.next();
                                } else {
                                    ThreadJob removed = jobQueue.remove();
                                    if (printInfoTiming) {
                                        System.out.println(this + " " + removed + " " + jobQueue.size() + " time= " + (System.currentTimeMillis() - migrationStart));
                                    }
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return;
                    }
                }
                if (take.isBatch()) {
                    take.run(next);
                    while (take.itr.hasNext()) {
                        take.run(take.itr.next());
                    }
                    if (printInfoTiming) {
                        System.out.println(this + " " + take + " " + jobQueue.size() + " time= " + (System.currentTimeMillis() - migrationStart));
                    }
                } else {
                    take.run(next);
                }
            }
        }
    }

    private class UpdateMaxForChangedSwitches extends QueuePartitionJob {
        private final boolean[] bestFeasibleChangedPartitions;
        private final List<AllQueuePartition> allQueuePartitions;

        protected UpdateMaxForChangedSwitches(Iterator<? extends AllQueuePartition> itr, boolean batch,
                                              boolean[] bestFeasibleChangedPartitions, List<AllQueuePartition> allQueuePartitions) {
            super(itr, batch);
            this.bestFeasibleChangedPartitions = bestFeasibleChangedPartitions;
            this.allQueuePartitions = allQueuePartitions;
        }


        @Override
        public <R extends AllQueuePartition> void run(R bestFeasibleChangedPartition) {
            if (bestFeasibleChangedPartitions[bestFeasibleChangedPartition.index]) {
                for (int switchIndex = 0; switchIndex < maxValueForSwitchPartition.length; switchIndex++) {
                    updateMaxValueForSwitchForPartition(switchIndex, bestFeasibleChangedPartition);
                }
            }
        }
    }

    private class UpdateMaxForChangedBestPartitions extends QueuePartitionJob {
        private final boolean[] bestFeasibleChangedPartitions;
        private final List<AllQueuePartition> allQueuePartitions;

        protected UpdateMaxForChangedBestPartitions(Iterator<? extends AllQueuePartition> itr, boolean batch,
                                                    boolean[] bestFeasibleChangedPartitions, List<AllQueuePartition> allQueuePartitions) {
            super(itr, batch);
            this.bestFeasibleChangedPartitions = bestFeasibleChangedPartitions;
            this.allQueuePartitions = allQueuePartitions;
        }

        @Override
        public <R extends AllQueuePartition> void run(R bestFeasibleChangedPartition) {
            if (bestFeasibleChangedPartitions[bestFeasibleChangedPartition.index]) {
                for (int switchIndex = 0; switchIndex < maxValueForSwitchPartition.length; switchIndex++) {
                    if (bestFeasibleChangedPartition.equals(maxValueForSwitchPartition[switchIndex])) {
                        updateMaxValueForSwitch(allQueuePartitions, switchIndex);
                    }
                }
            }
        }
    }

    private class FillAndSortTraffic extends QueuePartitionJob {

        private FillAndSortTraffic(Iterator<AllQueuePartition> itr, boolean batch) {
            super(itr, batch);
        }

        @Override
        public <R extends AllQueuePartition> void run(final R next) {
            {
                //compute traffic
                int switchIndex = 0;
                for (Switch candidateSwitch : considerableSwitches) {
                    long traffic = topology.getTrafficForHosting(next.getPartition(), candidateSwitch);
                    next.traffic[switchIndex] = traffic;
                    switchIndex++;
                }
            }
            {
                //compute traffic
                long sum = 0;
                for (Collection<Flow> flows : topology.ruleFlowMap.get(next.getPartition()).values()) {
                    for (Flow flow : flows) {
                        sum += flow.getTraffic();
                    }
                }
                next.inputTraffic = sum;
            }

            considerableSwitches.toArray(next.sortedSwitches);//I'm sure it will fit in
            {
                //sort switches
                Arrays.sort(next.sortedSwitches, new Comparator<Switch>() {
                    @Override
                    public int compare(Switch o1, Switch o2) {
                        return Long.compare(next.traffic[switchIndexMap.get(o1)], next.traffic[switchIndexMap.get(o2)]);
                    }
                });
            }

            {
                //insert sortedIndexes in the map map
                int sortedSwitchIndex = 0;
                for (Switch aSwitch : next.sortedSwitches) {
                    next.sortedSwitchesIndexMap.put(aSwitch, sortedSwitchIndex++);
                }
            }
        }
    }

    private class UpdateNewHostQueuePartitionJobTogether extends QueuePartitionJob {

        private final int oldHostSwitchIndex;
        private final int targetSwitchIndex;
        final boolean isSource;
        final AllQueuePartition migratedQueuePartition;

        final boolean[] bestChangedQueue;

        public UpdateNewHostQueuePartitionJobTogether(Iterator<AllQueuePartition> itr, int targetSwitchIndex, boolean source,
                                                      int oldHostSwitchIndex, AllQueuePartition migratedQueuePartition, boolean batch,
                                                      boolean[] bestChangedQueue) {
            super(itr, batch);
            this.targetSwitchIndex = targetSwitchIndex;
            this.oldHostSwitchIndex = oldHostSwitchIndex;
            this.isSource = source;
            this.migratedQueuePartition = migratedQueuePartition;
            this.bestChangedQueue = bestChangedQueue;
        }

        @Override
        public <R extends AllQueuePartition> void run(R next) {
            Switch oldHost = considerableSwitches.get(oldHostSwitchIndex);
            Integer oldHostSortedIndex = next.sortedSwitchesIndexMap.get(oldHost);
            Switch targetHost = considerableSwitches.get(targetSwitchIndex);
            Integer targetSortedIndex = next.sortedSwitchesIndexMap.get(targetHost);
            boolean mustUpdateBestFeasible = false;
            if (oldHostSortedIndex < targetSortedIndex) { //want to run the smallest index first
                mustUpdateBestFeasible = processOldHost(next, oldHostSortedIndex);
                mustUpdateBestFeasible = processTargetHost(next, targetSortedIndex) || mustUpdateBestFeasible;//must be this way to run processTargetHost
            } else {
                mustUpdateBestFeasible = processTargetHost(next, targetSortedIndex);
                mustUpdateBestFeasible = processOldHost(next, oldHostSortedIndex) || mustUpdateBestFeasible;
            }

            if (mustUpdateBestFeasible) {
                next.updateBestFeasible();
            }
        }

        private <R extends AllQueuePartition> boolean processTargetHost(R next, Integer targetSortedIndex) {
            if (isSource || next.feasibleSwitches[targetSwitchIndex]) {
                return update(next, targetSortedIndex, targetSwitchIndex);
            }
            return false;
        }

        private <R extends AllQueuePartition> boolean processOldHost(R next, Integer oldHostSortedIndex) {
            if (isSource || !next.feasibleSwitches[oldHostSwitchIndex]) {//don't do optimization
                return update(next, oldHostSortedIndex, oldHostSwitchIndex);
            }
            return false;
        }

        private <R extends AllQueuePartition> boolean update(R next, Integer oldHostSortedIndex,
                                                             int switchIndex) {
            boolean changed = false;
            boolean mustUpdateBestFeasible = false;
            if (migratedQueuePartition.equals(next)) {
                //it is done even after best because it is cheap
                next.feasibleSwitches[switchIndex] = switchIndex == oldHostSwitchIndex;
                next.feasibilityValueValid[switchIndex] = true;
                changed = true;
            }

            if (oldHostSortedIndex <= next.bestFeasibleSwitchSortedIndex) {
                //I care
                if (!changed) {//must check, it could have been checked because I am the partition that is migrated in this step
                    changed = next.checkNewFeasible(switchIndex);
                    next.feasibilityValueValid[switchIndex] = true;
                }
                if (changed) {
                    bestChangedQueue[next.index] = true;
                    if (oldHostSortedIndex == next.bestFeasibleSwitchSortedIndex) {
                        next.bestFeasibleSwitchSortedIndex = considerableSwitches.size();
                        mustUpdateBestFeasible = true;
                    } else {
                        next.bestFeasibleSwitchSortedIndex = oldHostSortedIndex;
                    }
                }
            } else {
                // I don't care skip
                next.feasibilityValueValid[switchIndex] = false;
            }
            return mustUpdateBestFeasible;
        }
    }

    /*
    private class UpdateNewHostQueuePartitionJob extends QueuePartitionJob {

        private final int switchIndex;
        final boolean isOldHost;
        final boolean isSource;
        final AllQueuePartition migratedQueuePartition;
        final BlockingQueue<AllQueuePartition> bestChangedQueue;

        protected UpdateNewHostQueuePartitionJob(Iterator<AllQueuePartition> itr, int switchIndex, boolean source,
                                                 boolean isOldHost, AllQueuePartition migratedQueuePartition, boolean batch,
                                                 BlockingQueue<AllQueuePartition> bestChangedQueue) {
            super(itr, batch);
            this.switchIndex = switchIndex;
            this.isSource = source;
            this.isOldHost = isOldHost;
            this.migratedQueuePartition = migratedQueuePartition;
            this.bestChangedQueue = bestChangedQueue;
        }

        @Override
        public <R extends AllQueuePartition> void run(R next) {
            if (next.index == 15248) {
                System.out.println();
            }
            if (isSource || (isOldHost && !next.feasibleSwitches[switchIndex]) || (!isOldHost && next.feasibleSwitches[switchIndex])) {
                final Switch candidateSwitch = considerableSwitches.get(switchIndex);
                Integer sortedSwitchIndex = next.sortedSwitchesIndexMap.get(candidateSwitch);
                boolean changed = false;
                if (migratedQueuePartition.equals(next)) {
                    //it is done even after best because it is cheap
                    next.feasibleSwitches[switchIndex] = isOldHost;
                    next.feasibilityValueValid[switchIndex] = true;
                    changed = true;
                }

                if (sortedSwitchIndex <= next.bestFeasibleSwitchSortedIndex) {
                    //I care
                    if (!changed) {
                        changed = next.checkNewFeasible(switchIndex);
                        next.feasibilityValueValid[switchIndex] = true;
                    }
                    if (changed) {
                        if (sortedSwitchIndex == next.bestFeasibleSwitchSortedIndex) {
                            //next.updateBestFeasible(); wait all updates are done
                            bestChangedQueue.add(next);
                        } else {
                            next.bestFeasibleSwitchSortedIndex = sortedSwitchIndex;
                            bestChangedQueue.add(next);
                        }
                    }
                } else {
                    // I don't care skip
                    next.feasibilityValueValid[switchIndex] = false;
                }

            }
        }
    }*/

    /*private class UpdateFeasibilityBoundaryJob extends QueuePartitionJob {

        protected UpdateFeasibilityBoundaryJob(Iterator<AllQueuePartition> itr, boolean batch) {
            super(itr, batch);
        }

        @Override
        public <R extends AllQueuePartition> void run(R next) {
            next.boundary = next.updateFeasibilityBoundary(next.boundary, boundaryThreshold);
        }
    }*/

    private class UpdateOldHostQueuePartitionJob extends QueuePartitionJob {
        final Set<Partition> hostedPartitions;
        final boolean isOldHost;
        final boolean isSource;
        final AllQueuePartition migratedQueuePartition;

        protected UpdateOldHostQueuePartitionJob(Iterator<? extends AllQueuePartition> itr, Set<Partition> hostedPartitions,
                                                 boolean source, boolean oldHost, AllQueuePartition migratedQueuePartition) {
            super(itr, false);
            this.hostedPartitions = hostedPartitions;
            isOldHost = oldHost;
            isSource = source;
            this.migratedQueuePartition = migratedQueuePartition;
        }

        @Override
        public <R extends AllQueuePartition> void run(R next) {
            /* if (print) {
                System.out.print((isSource || (isOldHost && !next.isOldHostFeasible2() || (!isOldHost && next.isOldHostFeasible2()))) + " for " + next);
            }*/
            if (isSource || (isOldHost && !next.isOldHostFeasible2() || (!isOldHost && next.isOldHostFeasible2()))) {
                if (!isOldHost && next.equals(migratedQueuePartition)) {
                    next.setOldHostFeasible(true); //I just migrated here
                    //minimum of updatedQueuePartitionMinIndex is my current index no need to update
                } else {
                    final Switch oldHost = considerableSwitches.get(next.oldHostIndex);
                    next.checkOldHost(oldHost, hostedPartitions);
                }
                //System.out.println(" " + next.isOldHostFeasible2());
            }
        }
    }

    private abstract class QueuePartitionJob extends ThreadJob<AllQueuePartition> {

        protected QueuePartitionJob(Iterator<? extends AllQueuePartition> itr, boolean batch) {
            super(itr, batch);
        }


        public abstract <R extends AllQueuePartition> void run(R next);
    }

    private class AllQueuePartitionIterator implements Iterator<AllQueuePartition> {
        private final Iterator<Partition> itr;
        private final Map<Partition, AllQueuePartition> partitionAllQueuePartitionMap;

        private AllQueuePartitionIterator(Iterator<Partition> itr, Map<Partition, AllQueuePartition> partitionAllQueuePartitionMap) {
            this.itr = itr;
            this.partitionAllQueuePartitionMap = partitionAllQueuePartitionMap;
        }

        @Override
        public boolean hasNext() {
            return itr.hasNext();
        }

        @Override
        public AllQueuePartition next() {
            return partitionAllQueuePartitionMap.get(itr.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }


    public class AllQueuePartition {
        public final boolean[] feasibleSwitches;
        public final boolean[] feasibilityValueValid;
        public final long[] traffic;
        public final Partition partition;
        public final double[] rates;
        public final int index;
        public final Switch source;
        public int boundary;
        public final Map<Switch, Integer> sortedSwitchesIndexMap;
        public final Switch[] sortedSwitches;

        public int oldHostIndex;
        public boolean oldHostFeasible = true;
        public double feasibleNotCurrentRatesSumCache = -1;
        //public double rateSum;
        public int bestFeasibleSwitchSortedIndex;
        public long inputTraffic;
        public double maxRate;
        public int maxRateIndex;

        private AllQueuePartition(int oldHostIndex, Partition partition, int index, Switch source) {
            this.oldHostIndex = oldHostIndex;
            this.partition = partition;
            this.index = index;
            this.source = source;
            feasibleSwitches = new boolean[considerableSwitches.size()];
            traffic = new long[considerableSwitches.size()];
            rates = new double[considerableSwitches.size()];
            sortedSwitchesIndexMap = new HashMap<>(considerableSwitches.size(), 1);
            sortedSwitches = new Switch[considerableSwitches.size()];
            feasibilityValueValid = new boolean[considerableSwitches.size()];
            boundary = 0;
            bestFeasibleSwitchSortedIndex = considerableSwitches.size();
//            Arrays.fill(feasibleSwitches, false);
//            Arrays.fill(feasibilityValueValid, false);
        }

        @Override
        public String toString() {
            if (bestFeasibleSwitchSortedIndex < sortedSwitches.length) {
                int i = convertSortedIndexToSwitchIndex(bestFeasibleSwitchSortedIndex);
                return "AllQueuePartition{" +
                        "index=" + index +
                        ", bestfeasible=" + i +
                        " (" + considerableSwitches.get(i) + ")" +
                        ", partition=" + partition +
                        ", source=" + source +
                        ", oldHostIndex=" + oldHostIndex +
                        " (" + considerableSwitches.get(oldHostIndex) + ")" +
                        ", oldHostFeasible=" + oldHostFeasible +
                        '}';
            } else {
                return "AllQueuePartition{" +
                        "index=" + index +
                        ", bestfeasible=NOTHING" +
                        ", partition=" + partition +
                        ", source=" + source +
                        ", oldHostIndex=" + oldHostIndex +
                        " (" + considerableSwitches.get(oldHostIndex) + ")" +
                        ", oldHostFeasible=" + oldHostFeasible +
                        '}';
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AllQueuePartition that = (AllQueuePartition) o;

            if (index != that.index) return false;

            return true;
        }

        public int convertSortedIndexToSwitchIndex(int sortedIndex) {
            return switchIndexMap.get(sortedSwitches[sortedIndex]);
        }

        @Override
        public int hashCode() {
            return index;
        }

        public void setOldHostFeasible(boolean oldHostFeasible) {
            this.oldHostFeasible = oldHostFeasible;
        }

        public int getOldHostIndex() {
            return oldHostIndex;
        }

        public long getTraffic(int index) {
            return traffic[index];
        }

        public boolean isOldHostFeasible2() {
            return oldHostFeasible;
        }

        public Partition getPartition() {
            return partition;
        }

        public int getMaxRateIndex() {
            return maxRateIndex;
        }

        public void computeRates(double avg, double max, double usableAvgDownSteps) {
            final long oldHostTraffic = traffic[oldHostIndex];
            maxRateIndex = -1;
            for (int switchIndex = 0; switchIndex < traffic.length; switchIndex++) {
                final double value = getRateForSwitch(oldHostTraffic, switchIndex, usableAvgDownSteps);
                rates[switchIndex] = value;
                if (feasibleSwitches[switchIndex] && feasibilityValueValid[switchIndex]) {//TODO
                    if (maxRate < rates[switchIndex] || maxRateIndex < 0) {
                        maxRate = rates[switchIndex];
                        maxRateIndex = switchIndex;
                    }
                }
            }
            feasibleNotCurrentRatesSumCache = -1;
        }

        private double getRateForSwitch(long oldHostTraffic, int switchIndex, double usableAvgDownSteps) {
            return (
                    (oldHostTraffic - traffic[switchIndex])
                            + 1 / usableAvgDownSteps * (maxValueForSwitch[oldHostIndex]
                            - (this.equals(maxValueForSwitchPartition[switchIndex]) ? 0 : maxValueForSwitch[switchIndex]))
            );
        }

        public double getSumNotCurrentFeasible() {
            if (feasibleNotCurrentRatesSumCache < 0) {
                double sum = 0;
                for (int i = 0; i < boundary; i++) {
                    if (feasibleSwitches[i]) {
                        sum += rates[i];
                    }
                }
                feasibleNotCurrentRatesSumCache = sum;
            }
            return feasibleNotCurrentRatesSumCache;
        }

        public double[] fillCumulativeFeasibleNotCurrentRates(double[] switchRates) {
            {// IF BOUNDARY IS > HALF IT IS BETTER TO COPY AND ZERO OUT OF BOUNDARIES NOOOOOOOOO! YOU MUST MAKE UNFEASIBLES TO ZERO TOO!
                // need to check only boundaries because out of that can be feasible but not considered.
                Arrays.fill(switchRates, 0);
                int i = 0;
                for (Switch sortedSwitch : sortedSwitches) {
                    if (i >= boundary) {
                        break;
                    }
                    Integer switchIndex = switchIndexMap.get(sortedSwitch);
                    if (feasibleSwitches[switchIndex]) {
                        switchRates[switchIndex] = rates[switchIndex];
                    }
                    i++;
                }
            }
            for (int i = 0; i < switchRates.length; i++) {
                switchRates[i] += i == 0 ? 0 : switchRates[i - 1];
            }
            return switchRates;
        }

        public double[] fillFeasibleNotCurrentRates(double[] switchRates) {
            {// IF BOUNDARY IS > HALF IT IS BETTER TO COPY AND ZERO OUT OF BOUNDARIES NOOOOOOOOO! YOU MUST MAKE UNFEASIBLES TO ZERO TOO!
                // need to check only boundaries because out of that can be feasible but not considered.
                Arrays.fill(switchRates, 0);
                int i = 0;
                for (Switch sortedSwitch : sortedSwitches) {
                    if (i >= boundary) {
                        break;
                    }
                    Integer switchIndex = switchIndexMap.get(sortedSwitch);
                    if (feasibleSwitches[switchIndex]) {
                        switchRates[switchIndex] = rates[switchIndex];
                    }
                    i++;
                }
            }
            return switchRates;
        }

        public void migrate(int targetSwitchIndex) {
            //just migrated so old host must be feasible to revert this action
            //this will be done when the feasiblity will be updated
            //feasibleNotCurrentRatesSumCache += rates[oldHostIndex] - rates[targetSwitchIndex];


            oldHostIndex = targetSwitchIndex;

            //this will be updated later
            /*final Switch oldHost = considerableSwitches.get(getOldHostIndex());
            final Set<Partition> hostedPartitions = rPlacement.get(oldHost);
            try {
               // hostedPartitions.remove(partition);
                isOldHostFeasible(hostedPartitions, partition, oldHost, false);
                //hostedPartitions.add(partition);
                oldHostFeasible = true;
            } catch (Switch.InfeasibleStateException e) {
                oldHostFeasible = false;
                System.out.println("old host " + oldHost + " is not OK for moving partition " + partition + " out of it");
                //hostedPartitions.add(partition);
                System.out.println("It cannot be infeasible as I added it just now so reverse is OK too");
                System.exit(1);
            }*/
        }

        /* private void initialFill() {
            final Switch oldHost = considerableSwitches.get(getOldHostIndex());
            final Set<Partition> hostedPartitions = rPlacement.get(oldHost);
            checkOldHost(oldHost, hostedPartitions);
            int switchIndex = 0;
            for (Switch candidateSwitch : considerableSwitches) {
                feasibleSwitches[switchIndex] = switchIndex != oldHostIndex && isNewFeasible(partition, candidateSwitch, false);
                long traffic = topology.getTrafficForHosting(partition, candidateSwitch);
                this.traffic[switchIndex] = traffic;
                switchIndex++;
            }
        }*/

        public void updateBestFeasible() {
            int switchSortedIndex = 0;
            for (int i = 0, sortedSwitchesLength = sortedSwitches.length; i < sortedSwitchesLength; i++) {
                Switch sortedSwitch = sortedSwitches[i];
                final boolean feasible;
                Integer switchIndex = switchIndexMap.get(sortedSwitch);
                if (feasibilityValueValid[switchIndex]) {
                    feasible = feasibleSwitches[switchIndex];
                } else {
                    // check feasibility
                    checkNewFeasible(switchIndex);
                    feasible = feasibleSwitches[switchIndex];
                    feasibilityValueValid[switchIndex] = true;
                }
                if (feasible) {
                    bestFeasibleSwitchSortedIndex = switchSortedIndex;
                    return;
                }
                switchSortedIndex++;
            }
            bestFeasibleSwitchSortedIndex = considerableSwitches.size();
        }

        public int updateFeasibilityBoundary(int currentBoundary, double boundaryThreshold) {
            //fill feasibility
            double sumFeasibleRate = 0;
            int switchSortedIndex = 0;
            for (Switch sortedSwitch : sortedSwitches) {

                final Integer switchIndex = switchIndexMap.get(sortedSwitch);
                boolean feasible;
//                try {
                if (switchSortedIndex < currentBoundary) {
                    //I have the value
                    feasible = feasibleSwitches[switchIndex];
                } else if (feasibilityValueValid[switchIndex]) {
                    feasible = feasibleSwitches[switchIndex];
                } else {
                    feasible = switchIndex != getOldHostIndex() && isNewFeasible(getPartition(), sortedSwitch, false);
                }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    feasible=false;
//                }
                feasibleSwitches[switchIndex] = feasible;
                feasibilityValueValid[switchIndex] = true;
                if (feasible) {
                    if (bestFeasibleSwitchSortedIndex > switchSortedIndex) {
                        bestFeasibleSwitchSortedIndex = switchSortedIndex;
                    }
                    sumFeasibleRate += rates[switchIndex];
                } else {
//                    if (switchIndex == oldHostIndex) {
//                        sumFeasibleRate += rates[switchIndex];
//                    } else {
//                    }
                    if (bestFeasibleSwitchSortedIndex == switchSortedIndex) {
                        bestFeasibleSwitchSortedIndex = considerableSwitches.size();
                    }
                    //feasibleRateSumEstimate can become zero because of double precision
                    //in these cases feasibleRateSumEstimate is very small
                    // sumFeasibleRate> 0  should  cover those cases
                }
                switchSortedIndex++;
            }
            /*if (switchSortedIndex < currentBoundary) {
                int tempSortedIndex = switchSortedIndex;
                while (iterator.hasNext() || tempSortedIndex < currentBoundary) {
                    Switch aSwitch = iterator.next();
                    final Integer switchIndex = switchIndexMap.get(aSwitch);
                    feasibleSwitches[switchIndex] = false;
                    tempSortedIndex++;
                }
            }*/
            feasibleNotCurrentRatesSumCache = sumFeasibleRate; //- rates[oldHostIndex];
            return switchSortedIndex + 1;
        }

        /*  public int getFirstFeasibleSwitch(List<Switch> tempArray) {
            tempArray.clear();
            tempArray.addAll(sortedSwitchesIndexMap.get());
            switchIndexMap.get(sortedSwitchesIndexMap);
            Integer switchIndex = 0;
            boolean found = false;
            for (Switch aSwitch : sortedSwitchesIndexMap.keySet()) {
                switchIndex = switchIndexMap.get(aSwitch);
                if (feasibleSwitches[switchIndex] && feasibilityValueValid[switchIndex]) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                switchIndex = oldHostIndex;
            }
            return switchIndex;
        }*/

        public boolean checkNewFeasible(int switchIndex) {
            boolean oldValue = feasibleSwitches[switchIndex];
            feasibleSwitches[switchIndex] = switchIndex != oldHostIndex &&
                    isNewFeasible(partition, considerableSwitches.get(switchIndex), false);
            return oldValue != feasibleSwitches[switchIndex];
        }

        public boolean checkOldHost(Switch oldHost, Set<Partition> hostedPartitions) {
            boolean oldOldHostFeasible = oldHostFeasible;
            if (source.equals(oldHost)) {
                try {
                    isOldHostFeasible(hostedPartitions, partition, oldHost, false);
                    oldHostFeasible = true;
                } catch (Switch.InfeasibleStateException e) {
                    oldHostFeasible = false;
//                    System.out.println("old host " + oldHost + " is not OK for moving partition " + partition + " out of it");
                }
            } else {
                oldHostFeasible = true;
            }
            return oldOldHostFeasible != oldHostFeasible;
        }

        public int getIndex() {
            return index;
        }
/*
        public long getOverhead(int index) {
            return traffic[index] - minTraffics.get(partition);
        }*/

        public long getBestFeasibleSwitchTraffic() throws NoBestFeasibleSwitchException {
            if (bestFeasibleSwitchSortedIndex >= sortedSwitches.length) {
                throw new NoBestFeasibleSwitchException(this);
            }
            return traffic[convertSortedIndexToSwitchIndex(bestFeasibleSwitchSortedIndex)];
        }

        public double getMaxRate() {
            return maxRate;
        }
    }

    private class NoBestFeasibleSwitchException extends Exception {
        private final AllQueuePartition allQueuePartition;

        private NoBestFeasibleSwitchException(AllQueuePartition allQueuePartition) {
            this.allQueuePartition = allQueuePartition;
        }
    }

    /*private class ComputeRatesJob extends QueuePartitionJob {
        private final double max;
        private final double avg;

        public ComputeRatesJob(Iterator<AllQueuePartition> qpIterator, double avg, double max, boolean batch) {
            super(qpIterator, batch);
            this.max = max;
            this.avg = avg;
        }

        @Override
        public <R extends AllQueuePartition> void run(R next) {
            next.computeRates(avg, max);
        }
    }*/

    private class InitOldHostFeasibility extends QueuePartitionJob {

        public InitOldHostFeasibility(Iterator<AllQueuePartition> qpIterator, boolean batch) {
            super(qpIterator, batch);
        }

        @Override
        public <R extends AllQueuePartition> void run(R next) {
            //check oldhost
            final int oldHostIndex = next.getOldHostIndex();
            final Switch oldHost = considerableSwitches.get(oldHostIndex);
            final Set<Partition> hostedPartitions = rPlacement.get(oldHost);
            next.checkOldHost(oldHost, hostedPartitions);
        }
    }

    private class FillAllFeasible extends QueuePartitionJob {
        public FillAllFeasible(Iterator<AllQueuePartition> iterator, boolean batch) {
            super(iterator, batch);
        }

        @Override
        public <R extends AllQueuePartition> void run(R next) {
            Arrays.fill(next.rates, 1);
            next.boundary = next.updateFeasibilityBoundary(0, 2);
        }
    }

    private class ExitThreadJob extends ThreadJob<Object> {


        public ExitThreadJob(Iterator<? extends Object> itr) {
            super(itr, false);
        }

        @Override
        public <R extends Object> void run(R t) {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

/*
private class FindMaxCurrentOverhead {
    private double currentTrafficSum;
    private int step;
    private List<AllQueuePartition> allQueuePartitions;
    private Util.IntegerWrapper updatedQueuePartitionMinIndex;
    private double maxCurrentOverhead;
    private AllQueuePartition maxCurrentOverheadPartition;
    private boolean maxChanged;
    private final FindMaxCurrentOverheadThread[] threads;

    public FindMaxCurrentOverhead(double currentTrafficSum,
                                  int step, List<AllQueuePartition> allQueuePartitions,
                                  Util.IntegerWrapper updatedQueuePartitionMinIndex,
                                  double maxCurrentOverhead, AllQueuePartition maxCurrentOverheadPartition,
                                  boolean maxChanged,
                                  int threadNum) {
        this.currentTrafficSum = currentTrafficSum;
        this.step = step;
        this.allQueuePartitions = allQueuePartitions;
        this.updatedQueuePartitionMinIndex = updatedQueuePartitionMinIndex;
        this.maxCurrentOverhead = maxCurrentOverhead;
        this.maxCurrentOverheadPartition = maxCurrentOverheadPartition;
        this.maxChanged = maxChanged;
        threads = new FindMaxCurrentOverheadThread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            threads[i]=new FindMaxCurrentOverheadThread(this,
                    new FindMaxCurrentOverheadJob());
        }
    }

    public double getCurrentTrafficSum() {
        return currentTrafficSum;
    }

    public double getMaxCurrentOverhead() {
        return maxCurrentOverhead;
    }

    public AllQueuePartition getMaxCurrentOverheadPartition() {
        return maxCurrentOverheadPartition;
    }

    public boolean isMaxChanged() {
        return maxChanged;
    }

    public FindMaxCurrentOverhead invoke() {
        for (FindMaxCurrentOverheadThread thread : threads) {
            ((FindMaxCurrentOverheadJob) thread.job).init(,maxCurrentOverhead,maxCurrentOverheadPartition);
            thread.reInit();
        }
        for (Thread findMinThread1 : findMinThreads) {
            findMinThread1.start();
        }

        if (step == 0) {
            for (int i2 = updatedQueuePartitionMinIndex.getValue(), allQueuePartitionsSize = allQueuePartitions.size(); i2 < allQueuePartitionsSize; i2++) {
                AllQueuePartition allQueuePartition = allQueuePartitions.get(i2);
                long oldHostTraffic = allQueuePartition.getTraffic(allQueuePartition.getOldHostIndex());
                currentTrafficSum += oldHostTraffic;
            }
        }

        try {
            for (Thread findMinThread : findMinThreads) {
                findMinThread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return this;
    }
}

private class FindMaxCurrentOverheadThread extends Thread {
    private final Object waitObject;
    private final QueuePartitionJob job;
    private boolean finishFlag;
    private boolean finished = false;

    private FindMaxCurrentOverheadThread(Object waitObject, QueuePartitionJob job) {
        this.waitObject = waitObject;
        this.job = job;
    }

    public void reInit() {
        finishFlag = false;
    }

    public void setFinishFlag() {
        finishFlag = true;
    }

    public void setFinished(boolean finished) {
        this.finished = finished;
    }

    @Override
    public void run() {
        while (!finishFlag) {
            AllQueuePartition next = null;
            while (next == null) {
                synchronized (job.itr) {
                    if (!job.itr.hasNext()) {
                        synchronized (this) {
                            finished = true;
                            this.notify();
                        }
                        synchronized (waitObject) {
                            try {
                                waitObject.wait();
                            } catch (InterruptedException e) {
                                return;
                            }
                        }
                    } else {
                        next = job.itr.next();
                        synchronized (this) {
                            finished = false;
                        }
                    }
                }

                //process next
                job.run(next);
            }
        }
    }
}

private class FindMaxCurrentOverheadJob extends QueuePartitionJob {
    private double maxCurrentOverhead;
    private AllQueuePartition maxCurrentOverheadPartition;
    private boolean maxChanged;


    private FindMaxCurrentOverheadJob() {
        super(null);
        maxChanged=false;
    }

    public void init(Iterator<AllQueuePartition> itr, double maxCurrentOverhead, AllQueuePartition maxCurrentOverheadPartition){
        this.maxCurrentOverhead = maxCurrentOverhead;
        this.maxCurrentOverheadPartition = maxCurrentOverheadPartition;
    }

    @Override
    public void run(AllQueuePartition allQueuePartition) {
        long oldHostTraffic = allQueuePartition.getTraffic(allQueuePartition.getOldHostIndex());

        try {
            double currentOverhead = oldHostTraffic - allQueuePartition.getBestFeasibleSwitchTraffic();
            if (maxCurrentOverheadPartition == null || maxCurrentOverhead < currentOverhead) {
                maxCurrentOverhead = currentOverhead;
                maxCurrentOverheadPartition = allQueuePartition;
                maxChanged = true;
            }
        } catch (NoBestFeasibleSwitchException e) {
            //do nothing just skip
        }
    }
}*/

    private class UpdateMaxValueForSwitchesJob extends ThreadJob<Switch> {
        private final Collection<AllQueuePartition> allQueuePartitions;

        public UpdateMaxValueForSwitchesJob(Iterator<Switch> itr, Collection<AllQueuePartition> allQueuePartitions, boolean batch) {
            super(itr, batch);
            this.allQueuePartitions = allQueuePartitions;
        }

        @Override
        public <R extends Switch> void run(R aSwitch) {
            updateMaxValueForSwitch(allQueuePartitions, switchIndexMap.get(aSwitch));
        }
    }

    private class ValidateFeasibilityForPartition extends ThreadJob<Switch> {
        private final AllQueuePartition queuePartition;

        public ValidateFeasibilityForPartition(Iterator<? extends Switch> itr, boolean batch, AllQueuePartition queuePartition) {
            super(itr, batch);
            this.queuePartition = queuePartition;
        }

        @Override
        public <R extends Switch> void run(R aSwitch) {
            int switchIndex = switchIndexMap.get(aSwitch);
            if (!queuePartition.feasibilityValueValid[switchIndex]) {
                queuePartition.checkNewFeasible(switchIndex);
                queuePartition.feasibilityValueValid[switchIndex] = true;
            }
        }
    }
}

