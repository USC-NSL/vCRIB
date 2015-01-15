package edu.usc.enl.cacheflow.algorithms.migration.rmigration3;

import edu.usc.enl.cacheflow.algorithms.migration.AbstractMigrator;
import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.factory.AssignmentFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
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
public class RealRandomMigrateVMStartPartitions extends AbstractMigrator {
    private final Random random;
    private final int movingAverageWindowSize;
    private final double stopThreshold;
    private final double beta;
    private List<Switch> considerableSwitches;
    private final double recomputeThreshold;

    private LinkedList<QueuePartitionJob> jobQueue;
    private List<QueuePartitionThread> queuePartitionThreads;
    private final Map<Partition, AllQueuePartition> partitionAllQueuePartitionMap;
    private final Map<Switch, Integer> switchIndexMap;
    private final double boundaryThreshold;
    private static final int DEFAULT_MAX_OVERHEAD = 0;
    private static final boolean printInfo = true;
    private final int threadNum;

    public RealRandomMigrateVMStartPartitions(Topology topology,
                                              Map<Partition, Long> minTraffics,
                                              Map<Switch, Collection<Partition>> sourcePartitions, Map<Partition, Map<Switch, Rule>> forwardingRules,
                                              Random random,
                                              int threadNum, int movingAverageWindowSize, double stopThreshold, double beta, double boundaryThreshold,
                                              double recomputeThreshold) {
        super(topology, minTraffics, sourcePartitions, forwardingRules);
        this.random = random;
        this.movingAverageWindowSize = movingAverageWindowSize;
        this.stopThreshold = stopThreshold;
        this.beta = beta;
        this.boundaryThreshold = boundaryThreshold;
        this.recomputeThreshold = recomputeThreshold;
        switchIndexMap = new HashMap<>(topology.getSwitches().size(), 1);
        this.threadNum = threadNum;

        partitionAllQueuePartitionMap = new HashMap<>(forwardingRules.size(), 1);
    }

    public Map<Partition, Switch> postPlace2(Set<Switch> availableSwitches, Assignment assignment, PrintWriter trendWriter) {
        jobQueue = new LinkedList<>();
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
        final Map<Partition, Switch> bestPlacement = new HashMap<>(placement);
        long bestPlacementScore = 0;
        long currentTrafficSum = 0;
        double currentResourceUsage = 0;
        final int partitionsNum = placement.size();
        double[] movingAverageWindow = new double[movingAverageWindowSize];
        Arrays.fill(movingAverageWindow, 0d);
        double movingAverageSum = 0;
        double[] switchRates = new double[considerableSwitches.size()];
        int step = 0;

        //fill switch index
        switchIndexMap.clear();
        {
            int i = 0;
            for (Switch considerableSwitch : considerableSwitches) {
                switchIndexMap.put(considerableSwitch, i++);
            }
        }

        long maxMinTraffic = -1;
        for (Long aLong : minTraffics.values()) {
            if (maxMinTraffic < aLong) {
                maxMinTraffic = aLong;
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
                jobQueue.add(new FillAndSortTraffic(allQueuePartitions.iterator()));
                jobQueue.add(new FillAllFeasible(allQueuePartitions.iterator()));
                jobQueue.add(new InitOldHostFeasibility(allQueuePartitions.iterator()));
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

        double maxCurrentOverhead = DEFAULT_MAX_OVERHEAD;
        AllQueuePartition maxCurrentOverheadPartition = null;
        boolean maxChanged = false;
        migrationStart = System.currentTimeMillis();
        long stepStart = migrationStart;
        maxCurrentOverheadPartition = null;

        joinQueuePartitionThreads();

        //fill initial stats, need traffics to be computed before
        for (AllQueuePartition allQueuePartition : allQueuePartitions) {
            long oldHostTraffic = allQueuePartition.getTraffic(allQueuePartition.getOldHostIndex());
            currentTrafficSum += oldHostTraffic;
        }
        bestPlacementScore = currentTrafficSum;

        while (true) {

            //compute average of current overhead and init stats
            {
                double tempMaxCurrentOverhead = DEFAULT_MAX_OVERHEAD;
                AllQueuePartition tempMaxCurrentOverheadPartition = null;
                {
                    for (AllQueuePartition allQueuePartition : allQueuePartitions) {
                        long oldHostTraffic = allQueuePartition.getTraffic(allQueuePartition.getOldHostIndex());
                        try {
                            double currentOverhead = oldHostTraffic - allQueuePartition.getBestFeasibleSwitchTraffic();
                            if (tempMaxCurrentOverhead < currentOverhead) {
                                tempMaxCurrentOverhead = currentOverhead;
                                tempMaxCurrentOverheadPartition = allQueuePartition;
                                maxChanged = true;
                            }
                        } catch (NoBestFeasibleSwitchException e) {
                            //do nothing just skip
                        }
                    }
//                    avgOverhead = avgOverhead / allQueuePartitions.size();


                }
                if (tempMaxCurrentOverhead <= DEFAULT_MAX_OVERHEAD) {
                    System.out.println("Reached the best solution");
                    break;
                }

                if (printInfo) {
                    long t = System.currentTimeMillis();
                    System.out.println("Update max: M" + (t - migrationStart) + "  R" + (t - stepStart));
                }

                //now compute rates and feasibility
                {
                    if (maxCurrentOverhead <= DEFAULT_MAX_OVERHEAD || (tempMaxCurrentOverhead / maxCurrentOverhead) <= recomputeThreshold ||
                            (maxCurrentOverhead / tempMaxCurrentOverhead) <= recomputeThreshold) {
                        maxCurrentOverhead = tempMaxCurrentOverhead;
                        maxCurrentOverheadPartition = tempMaxCurrentOverheadPartition;
                        System.out.println("step " + step + ": update rates");
                        initThreads();
                        synchronized (jobQueue) {
                            jobQueue.add(new ComputeRatesJob(allQueuePartitions.iterator(), maxCurrentOverhead));
                            jobQueue.add(new UpdateFeasibilityBoundaryJob(allQueuePartitions.iterator()));
                            jobQueue.notifyAll();
                        }

                        joinQueuePartitionThreads();
                        updatedQueuePartitionMinIndex.setValue(0);
                    }
                }

            }

            long t = 0;
            if (printInfo) {
                t = System.currentTimeMillis();
                System.out.println("Update rates because of max: M" + (t - migrationStart) + "  R" + (t - stepStart));
            }

            if (step == 0) {
                migrationStart = System.currentTimeMillis();
                trendWriter.println("," + getSwitchTypeResourceUsage() + currentResourceUsage + "," + currentTrafficSum + "," + (migrationStart - start));
            }

            stepStart = System.currentTimeMillis();

            //now update oldhostfeasible partitions rates from sum of their feasible not current rates
            {
                //for (int i2 = updatedQueuePartitionMinIndex.getValue(), allQueuePartitionsSize = allQueuePartitions.size(); i2 < allQueuePartitionsSize; i2++) {
                //int lastFeasible = -1;
                for (int i2 = updatedQueuePartitionMinIndex.getValue(), allQueuePartitionsSize = allQueuePartitions.size(); i2 < allQueuePartitionsSize; i2++) {
                    AllQueuePartition allQueuePartition = allQueuePartitions.get(i2);
                    if (allQueuePartition.isOldHostFeasible2()) {
                        cumulativePartitionsRates[i2] = allQueuePartition.getSumNotCurrentFeasible();
                        // lastFeasible = i2;
                    } else {
                        cumulativePartitionsRates[i2] = 0;
                    }
                    cumulativePartitionsRates[i2] += i2 == 0 ? 0 : cumulativePartitionsRates[i2 - 1];
                }
                /*for (int i = 0; i < cumulativePartitionsRates.length; i++) {
                    cumulativePartitionsRates[i] /= cumulativePartitionsRates[cumulativePartitionsRates.length - 1];
                }
                for (int i = cumulativePartitionsRates.length - 1; i >= lastFeasible; i--) {
                    cumulativePartitionsRates[i] = 1;

                }*/

                if (cumulativePartitionsRates[cumulativePartitionsRates.length - 1] == 0) {
                    System.out.println("No partition to move");
                    break;
                }
            }

            if (printInfo) {
                t = System.currentTimeMillis();
                System.out.println("Update CDF: M" + (t - migrationStart) + "  R" + (t - stepStart));
            }

            /*AllQueuePartition maxProbableQueuePartition = null;
            double maxProbability = -1;
            {
                for (int i = 0, cumulativePartitionsRatesLength = cumulativePartitionsRates.length; i < cumulativePartitionsRatesLength; i++) {
                    double v = cumulativePartitionsRates[i] - (i == 0 ? 0 : cumulativePartitionsRates[i - 1]);
                    if (maxProbableQueuePartition == null || v > maxProbability) {
                        maxProbability = v;
                        maxProbableQueuePartition = allQueuePartitions.get(i);
                    }
                    if (v < 0) {
                        System.out.println("negative v");
                        System.exit(1);
                    }
                }
            }
            maxProbability /= cumulativePartitionsRates[cumulativePartitionsRates.length - 1];*/
            //pick a random partition
            final int qPartitionIndex = Util.randomSelect(cumulativePartitionsRates, random);
            AllQueuePartition queuePartition = allQueuePartitions.get(qPartitionIndex);
            final int oldHostIndex = queuePartition.getOldHostIndex();
            final Switch oldHost = considerableSwitches.get(oldHostIndex);
            Partition partition = queuePartition.getPartition();

            Switch source = queuePartition.source;

            //prepare to select a target switch

            queuePartition.fillCumulativeFeasibleNotCurrentRates(switchRates);

            //select the target switch
            final int targetSwitchIndex = Util.randomSelect(switchRates, random);
            if (targetSwitchIndex == considerableSwitches.size()) {
                System.out.println(targetSwitchIndex);
                System.out.println(queuePartition);
                System.out.println(queuePartition.feasibleNotCurrentRatesSumCache);
                queuePartition.updateFeasibilityBoundary(queuePartition.boundary, boundaryThreshold);
                System.out.println(queuePartition.feasibleNotCurrentRatesSumCache);
            }
            Switch targetSwitch = considerableSwitches.get(targetSwitchIndex);


            ///
            if (printInfo || step % 1000 == 0) {
                t = System.currentTimeMillis();
                System.out.println("Selection: M" + (t - migrationStart) + "  R" + (t - stepStart));
                System.out.print(step + ":moving avg " + movingAverageSum / movingAverageWindowSize + ": " + partition + " src=" + source + " on: " + oldHost +
                        ", to " + targetSwitch +
                        " probability was " + (cumulativePartitionsRates[qPartitionIndex] - (qPartitionIndex == 0 ? 0 : cumulativePartitionsRates[qPartitionIndex - 1])) / cumulativePartitionsRates[cumulativePartitionsRates.length - 1]
                        //+ " max probable was " + maxProbableQueuePartition + "(" + maxProbability + ") and average was " + cumulativePartitionsRates[cumulativePartitionsRates.length - 1] / cumulativePartitionsRates.length
                        + " target switch probability was " + (switchRates[targetSwitchIndex] - (targetSwitchIndex == 0 ? 0 : switchRates[targetSwitchIndex - 1])) / switchRates[switchRates.length - 1]
                        + " sum was " + switchRates[switchRates.length - 1]);
            }
            //take old statistics of the switches
            double newHostOldResourceUsage = targetSwitch.getUsedResources(targetSwitch.getState());
            double oldHostOldResourceUsage = oldHost.getUsedResources(oldHost.getState());
            double newHostOldResourceAbsUsage = targetSwitch.getUsedAbsoluteResources(targetSwitch.getState());
            double oldHostOldResourceAbsUsage = oldHost.getUsedAbsoluteResources(oldHost.getState());
            long trafficChange = queuePartition.getTraffic(targetSwitchIndex) - queuePartition.getTraffic(oldHostIndex);

            //do migration
            final Set<Partition> hostedOnOldHost = rPlacement.get(oldHost);
            Set<Partition> hostedOnTargetHost = rPlacement.get(targetSwitch);
            {
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


            }

            if (printInfo) {
                t = System.currentTimeMillis();
                System.out.println("Apply: M" + (t - migrationStart) + "  R" + (t - stepStart));
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
                switchTypeResourceUsage.put(targetSwitch.getClass(), switchTypeResourceUsage.get(targetSwitch.getClass()) + targetSwitch.getUsedAbsoluteResources(targetSwitch.getState()) - newHostOldResourceAbsUsage);
                switchTypeResourceUsage.put(oldHost.getClass(), switchTypeResourceUsage.get(oldHost.getClass()) + oldHost.getUsedAbsoluteResources(oldHost.getState()) - oldHostOldResourceAbsUsage);
                trendWriter.println(targetSwitch + "," + getSwitchTypeResourceUsage() + currentResourceUsage + "," + currentTrafficSum + "," + (System.currentTimeMillis() - migrationStart));
                //trendWriter.flush();
            }

            {
                //update queuepartitions
                queuePartition.migrate(targetSwitchIndex);
                //my rates depend on old host now that old host is changed I need to recompute them
                final Iterator<AllQueuePartition> singletonItr = Collections.singleton(queuePartition).iterator();
                //queuePartition.computeRates(avgOverhead, maxCurrentOverhead);

                //reset mini index to CURRENT PARTITION as it rates will change
                updatedQueuePartitionMinIndex.setValue(qPartitionIndex);

                //init threads
                initThreads();
                synchronized (jobQueue) {

                    //update migrated partition
                    jobQueue.offer(new ComputeRatesJob(singletonItr, maxCurrentOverhead));
                    //its true that I didn't update feasibility of old and target host to true and false
                    //but this will be updated later when all partitions update those
                    //so this may do a little unnecessary tasks but
                    // hey this is not necessary, as target was feasible and WAS BEFORE BOUNDARY that I selected
                    //now it become infeasible when others update and certainly will update its boundary
                    //jobQueue.offer(new UpdateFeasibilityBoundaryJob(singletonItr));

                    //update partitions hosted on the old and target machine
                    boolean oldHostWasSource = source.equals(oldHost);
                    boolean targetHostIsSource = source.equals(targetSwitch);
                    jobQueue.offer(new UpdateOldHostQueuePartitionJob(hostedOnOldHost, updatedQueuePartitionMinIndex, oldHostWasSource, true, queuePartition));
                    jobQueue.offer(new UpdateOldHostQueuePartitionJob(hostedOnTargetHost, updatedQueuePartitionMinIndex, targetHostIsSource, false, queuePartition));


                    //every other partition must update their is newfeasible
                    jobQueue.offer(new UpdateNewHostQueuePartitionJob(allQueuePartitions.iterator(), updatedQueuePartitionMinIndex, oldHostIndex, oldHostWasSource, true, queuePartition));
                    jobQueue.offer(new UpdateNewHostQueuePartitionJob(allQueuePartitions.iterator(), updatedQueuePartitionMinIndex, targetSwitchIndex, targetHostIsSource, false, queuePartition));
                    jobQueue.notifyAll();
                }

                //update best
                if (bestPlacementScore > currentTrafficSum) {
                    bestPlacementScore = currentTrafficSum;
                    bestPlacement.clear();
                    bestPlacement.putAll(placement);
                }

                joinQueuePartitionThreads();
            }
            if (printInfo) {
                t = System.currentTimeMillis();
                System.out.println("Update: M" + (t - migrationStart) + "  R" + (t - stepStart));
            }

            movingAverageSum += -trafficChange;
            movingAverageSum -= movingAverageWindow[step % movingAverageWindowSize];
            movingAverageWindow[step % movingAverageWindowSize] = -trafficChange;
            if (step >= movingAverageWindowSize && movingAverageSum / movingAverageWindowSize < stopThreshold) {
                System.out.println("moving average stop");
                break;
            }

            //take care of maximum
            maxChanged = false;
            //check if it is changed
            if (maxCurrentOverheadPartition.getIndex() >= updatedQueuePartitionMinIndex.getValue()) {
                long oldHostTraffic = maxCurrentOverheadPartition.getTraffic(maxCurrentOverheadPartition.getOldHostIndex());
                try {
                    long currentOverhead = oldHostTraffic - maxCurrentOverheadPartition.getBestFeasibleSwitchTraffic();
                    if (currentOverhead != maxCurrentOverhead) {
                        maxCurrentOverhead = currentOverhead;
                        maxChanged = true;
                    }
                } catch (NoBestFeasibleSwitchException e) {
                    maxChanged = true;
//                    maxCurrentOverheadPartition = null;
//                    maxCurrentOverhead = DEFAULT_MAX_OVERHEAD;
                }
            }

            if (printInfo) {
                t = System.currentTimeMillis();
                System.out.println("Finish: M" + (t - migrationStart) + "  R" + (t - stepStart));
            }

            step++;
        }

        initThreads();
        synchronized (jobQueue) {
            jobQueue.offer(new ExitQueuePartitionJob(allQueuePartitions.iterator()));
            jobQueue.notifyAll();
        }
        joinQueuePartitionThreads();

        topology.reset();
        try {
            new AssignmentFactory.LoadPlacer(false, forwardingRules, new Assignment(bestPlacement), sourcePartitions).place2(topology, bestPlacement.keySet());
        } catch (NoAssignmentFoundException e) {
            e.printStackTrace();
        }

        return bestPlacement;
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


    private class QueuePartitionThread extends Thread {
        private final LinkedList<QueuePartitionJob> jobQueue;
        private boolean finished = false;

        private QueuePartitionThread(LinkedList<QueuePartitionJob> jobQueue) {
            this.jobQueue = jobQueue;
        }

        public synchronized boolean isFinished() {
            return finished;
        }

        public synchronized void setFinished(boolean finished) {
            this.finished = finished;
        }


        @Override
        public void run() {
            while (true) {
                AllQueuePartition next = null;
                QueuePartitionJob take = null;
                synchronized (jobQueue) {
                    try {
                        while (next == null) {
                            if (jobQueue.isEmpty()) {
                                //System.out.println(this + " set its finish to true and notify");
                                synchronized (this) {
                                    finished = true;
                                    this.notify();
                                }
                                //System.out.println(this + " is going to wait");
                                jobQueue.wait();
                                continue;//check the empty again as others may used up the queue
                            }
                            take = jobQueue.peek();
                            if (take instanceof ExitQueuePartitionJob) {
                                synchronized (this) {
//                                    System.out.println(this + " finished");
                                    finished = true;
                                    this.notify();
                                }
                                return;
                            }
                            synchronized (this) {
                                finished = false;
                            }
                            if (take.itr.hasNext()) {
                                next = take.itr.next();
                                //jobQueue.addFirst(take);
                                //jobQueue.notifyAll();
                            } else {
                                QueuePartitionJob removed = jobQueue.remove();
                                if (printInfo) {
                                    System.out.println(this + " " + removed.getClass().getSimpleName() + " " + jobQueue.size() + " time= " + (System.currentTimeMillis() - migrationStart));
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return;
                    }
                }

                take.run(next);
            }
        }
    }

    private class FillAndSortTraffic extends QueuePartitionJob {

        private FillAndSortTraffic(Iterator<AllQueuePartition> itr) {
            super(itr);
        }

        @Override
        public void run(final AllQueuePartition next) {
            {
                //compute traffic
                int switchIndex = 0;
                for (Switch candidateSwitch : considerableSwitches) {
                    long traffic = topology.getTrafficForHosting(next.getPartition(), candidateSwitch);
                    next.traffic[switchIndex] = traffic;
                    switchIndex++;
                }
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

    private class UpdateNewHostQueuePartitionJob extends QueuePartitionJob {

        private final Util.IntegerWrapper updatedQueuePartitionMinIndex;
        private final int switchIndex;
        final boolean isOldHost;
        final boolean isSource;
        final AllQueuePartition migratedQueuePartition;

        protected UpdateNewHostQueuePartitionJob(Iterator<AllQueuePartition> itr, Util.IntegerWrapper updatedQueuePartitionMinIndex, int switchIndex, boolean source,
                                                 boolean isOldHost, AllQueuePartition migratedQueuePartition) {
            super(itr);
            this.updatedQueuePartitionMinIndex = updatedQueuePartitionMinIndex;
            this.switchIndex = switchIndex;
            this.isSource = source;
            this.isOldHost = isOldHost;
            this.migratedQueuePartition = migratedQueuePartition;
        }

        @Override
        public void run(AllQueuePartition next) {
//            if (next.index==165 && switchIndex==1065){
//                System.out.println();
//            }
            if (isSource || (isOldHost && !next.feasibleSwitches[switchIndex]) || (!isOldHost && next.feasibleSwitches[switchIndex])) {
                final Switch candidateSwitch = considerableSwitches.get(switchIndex);
                if (next.sortedSwitchesIndexMap.get(candidateSwitch) < next.boundary) {
                    boolean changed;
                    if (isOldHost && migratedQueuePartition.equals(next)) {
                        next.feasibleSwitches[switchIndex] = true;
                        // I just migrated from there so going back is feasible too
                        //minimum value of updatedQueuePartitionMinIndex is my current index
                        changed = true;
                    } else {
                        changed = next.checkNewFeasible(switchIndex);
                    }
                    if (changed) {
                        //now, that is changed I need to update boundary

                        //this method must does not set the feasibility of additional ones to false, as they are cached but their validity is checked by  feasibilityValueValid.
                        int oldBoundary = next.boundary;
                        next.boundary = next.updateFeasibilityBoundary(oldBoundary, boundaryThreshold);

                        //System.out.println(next.index + " feasibility of " + switchIndex + " changed to " +
                        //       next.feasibleSwitches[switchIndex] + " with changed  boundary form " + oldBoundary + " to " + next.boundary);
                        synchronized (updatedQueuePartitionMinIndex) {
                            updatedQueuePartitionMinIndex.setValue(updatedQueuePartitionMinIndex.getValue() > next.getIndex() ? next.getIndex() : updatedQueuePartitionMinIndex.getValue());
                        }
                    }
                } else {
                    // I don't care skip
                    next.feasibilityValueValid[switchIndex] = false;
                }
            }
        }
    }

    private class UpdateFeasibilityBoundaryJob extends QueuePartitionJob {

        protected UpdateFeasibilityBoundaryJob(Iterator<AllQueuePartition> itr) {
            super(itr);
        }

        @Override
        public void run(AllQueuePartition next) {
            next.boundary = next.updateFeasibilityBoundary(next.boundary, boundaryThreshold);
        }
    }

    private class UpdateOldHostQueuePartitionJob extends QueuePartitionJob {
        final Util.IntegerWrapper updatedQueuePartitionMinIndex;
        final Set<Partition> hostedPartitions;
        final boolean isOldHost;
        final boolean isSource;
        final AllQueuePartition migratedQueuePartition;

        protected UpdateOldHostQueuePartitionJob(Set<Partition> hostedPartitions, Util.IntegerWrapper updatedQueuePartitionMinIndex,
                                                 boolean source, boolean oldHost, AllQueuePartition migratedQueuePartition) {
            super(new AllQueuePartitionIterator(hostedPartitions.iterator()));
            this.updatedQueuePartitionMinIndex = updatedQueuePartitionMinIndex;
            this.hostedPartitions = hostedPartitions;
            isOldHost = oldHost;
            isSource = source;
            this.migratedQueuePartition = migratedQueuePartition;
        }

        @Override
        public void run(AllQueuePartition next) {
            /* if (print) {
                System.out.print((isSource || (isOldHost && !next.isOldHostFeasible2() || (!isOldHost && next.isOldHostFeasible2()))) + " for " + next);
            }*/
            if (isSource || (isOldHost && !next.isOldHostFeasible2() || (!isOldHost && next.isOldHostFeasible2()))) {
                if (!isOldHost && next.equals(migratedQueuePartition)) {
                    next.setOldHostFeasible(true); //I just migrated here
                    //minimum of updatedQueuePartitionMinIndex is my current index no need to update
                } else {
                    final Switch oldHost = considerableSwitches.get(next.oldHostIndex);
                    final boolean changed = next.checkOldHost(oldHost, hostedPartitions);
                    if (changed) {
                        // System.out.print(" changed ");
                        //System.out.println(next.index + " oldhost " + next.getOldHostIndex() + " feasibility changed to " + next.isOldHostFeasible2());
                        synchronized (updatedQueuePartitionMinIndex) {
                            updatedQueuePartitionMinIndex.setValue(updatedQueuePartitionMinIndex.getValue() > next.getIndex() ? next.getIndex() : updatedQueuePartitionMinIndex.getValue());
                        }
                    }
                }
                //System.out.println(" " + next.isOldHostFeasible2());
            }
        }
    }

    private abstract class QueuePartitionJob {
        private final Iterator<AllQueuePartition> itr;

        protected QueuePartitionJob(Iterator<AllQueuePartition> itr) {
            this.itr = itr;
        }

        public abstract void run(AllQueuePartition next);
    }

    private class AllQueuePartitionIterator implements Iterator<AllQueuePartition> {
        private final Iterator<Partition> itr;

        private AllQueuePartitionIterator(Iterator<Partition> itr) {
            this.itr = itr;
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


    private class AllQueuePartition {
        private final boolean[] feasibleSwitches;
        private final boolean[] feasibilityValueValid;
        private final long[] traffic;
        private final Partition partition;
        private final double[] rates;
        private final int index;
        private final Switch source;
        private int boundary;
        private final Map<Switch, Integer> sortedSwitchesIndexMap;
        private final Switch[] sortedSwitches;

        private int oldHostIndex;
        private boolean oldHostFeasible = true;
        private double feasibleNotCurrentRatesSumCache = -1;
        private double rateSum;
        private int bestFeasibleSwitchSortedIndex;

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
            return "AllQueuePartition{" +
                    "index=" + index +
                    ", boundary=" + boundary +
                    ", partition=" + partition +
                    ", source=" + source +
                    ", oldHostIndex=" + oldHostIndex +
                    ", oldHostFeasible=" + oldHostFeasible +
                    '}';
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

        public void computeRates(double max) {
            final long oldHostTraffic = traffic[oldHostIndex];
            rateSum = 0;
            for (int i = 0; i < traffic.length; i++) {
                final double value = Math.exp(beta * (oldHostTraffic - traffic[i]) / max);
                rates[i] = value;
                rateSum += value;
            }
            feasibleNotCurrentRatesSumCache = -1;
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

        public int updateFeasibilityBoundary(int currentBoundary, double boundaryThreshold) {
            /*double rateSum = 0;
            for (double rate : rates) {
                rateSum += rate;
            }*/

            //fill feasibility
            double sumFeasibleRate = 0;
            int switchSortedIndex = 0;
            double feasibleRateSumEstimate = rateSum;
            for (Switch sortedSwitch : sortedSwitches) {

                final Integer switchIndex = switchIndexMap.get(sortedSwitch);
                if (rates[switchIndex] == 0) {
                    //why bother to check feasiblity when we know its rate will be zero
                    break;
                }
                final boolean feasible;
                if (switchSortedIndex < currentBoundary) {
                    //I have the value
                    feasible = feasibleSwitches[switchIndex];
                } else if (feasibilityValueValid[switchIndex]) {
                    feasible = feasibleSwitches[switchIndex];
                } else {
                    feasible = switchIndex != getOldHostIndex() && isNewFeasible(getPartition(), sortedSwitch, false);
                }
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
                    feasibleRateSumEstimate -= rates[switchIndex];
//                    }
                    if (bestFeasibleSwitchSortedIndex == switchSortedIndex) {
                        bestFeasibleSwitchSortedIndex = considerableSwitches.size();
                    }
                    //feasibleRateSumEstimate can become zero because of double precision
                    //in these cases feasibleRateSumEstimate is very small
                    // sumFeasibleRate> 0  should  cover those cases
                }
                if (sumFeasibleRate > 0 && sumFeasibleRate >= boundaryThreshold * feasibleRateSumEstimate) {
                    break;
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
    }

    private class NoBestFeasibleSwitchException extends Exception {
        private final AllQueuePartition allQueuePartition;

        private NoBestFeasibleSwitchException(AllQueuePartition allQueuePartition) {
            this.allQueuePartition = allQueuePartition;
        }
    }

    private class ComputeRatesJob extends QueuePartitionJob {
        private final double max;

        public ComputeRatesJob(Iterator<AllQueuePartition> qpIterator, double max) {
            super(qpIterator);
            this.max = max;
        }

        @Override
        public void run(AllQueuePartition next) {
            next.computeRates(max);
        }
    }

    private class InitOldHostFeasibility extends QueuePartitionJob {

        public InitOldHostFeasibility(Iterator<AllQueuePartition> qpIterator) {
            super(qpIterator);
        }

        @Override
        public void run(AllQueuePartition next) {
            //check oldhost
            final int oldHostIndex = next.getOldHostIndex();
            final Switch oldHost = considerableSwitches.get(oldHostIndex);
            final Set<Partition> hostedPartitions = rPlacement.get(oldHost);
            next.checkOldHost(oldHost, hostedPartitions);
        }
    }

    private class FillAllFeasible extends QueuePartitionJob {
        public FillAllFeasible(Iterator<AllQueuePartition> iterator) {
            super(iterator);
        }

        @Override
        public void run(AllQueuePartition next) {
            Arrays.fill(next.rates, 1);
            next.rateSum = next.rates.length;
            next.updateFeasibilityBoundary(0, 2);
        }
    }

    private class ExitQueuePartitionJob extends QueuePartitionJob {

        protected ExitQueuePartitionJob(Iterator<AllQueuePartition> itr) {
            super(itr);
        }

        @Override
        public void run(AllQueuePartition next) {

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
}

