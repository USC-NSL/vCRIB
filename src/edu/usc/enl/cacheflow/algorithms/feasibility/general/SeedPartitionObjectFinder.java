package edu.usc.enl.cacheflow.algorithms.feasibility.general;

import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/22/12
 * Time: 12:37 AM
 * To change this template use File | Settings | File Templates.
 */
 class SeedPartitionObjectFinder {
    private final List<Switch> considerableSwitches;
    private final Collection<PartitionObject> partitionObjects;
    private final Map<Switch, Set<PartitionObject>> oneChoicePartitions;
    private final FindMinThread[] findMinThreads;
    private PartitionObject partitionObject = null;
    private int partitionObjectMaxSwitchIndex = -1;
    private final Map<Switch,Integer> switchIndexMap;


    public SeedPartitionObjectFinder(List<Switch> considerableSwitches, Collection<PartitionObject> partitionObjects,
                                     Map<Switch, Set<PartitionObject>> oneChoicePartitions, Map<Switch, Integer> switchIndexMap,
                                     int threadNum) {
        this.considerableSwitches = considerableSwitches;
        this.partitionObjects = partitionObjects;
        this.oneChoicePartitions = oneChoicePartitions;
        this.switchIndexMap = switchIndexMap;
        this.findMinThreads = new FindMinThread[threadNum];
    }

    public PartitionObject getPartitionObject() {
        return partitionObject;
    }

    public int getPartitionObjectMaxSwitchIndex() {
        return partitionObjectMaxSwitchIndex;
    }

    public SeedPartitionObjectFinder invoke() throws NoAssignmentFoundException {
        Iterator<PartitionObject> iterator = partitionObjects.iterator();
        for (int i = 0; i < findMinThreads.length; i++) {
            findMinThreads[i] = new FindMinThread(iterator, oneChoicePartitions, considerableSwitches);
        }

        Util.runThreads(findMinThreads);

        //if there is a one choice switch get it
        if (oneChoicePartitions.size() > 0) {
            partitionObject = getAndUpdateOneChoicePartitions();
        } else {
            float partitionObjectSimilarity = Float.MAX_VALUE;
            for (FindMinThread findMinThread : findMinThreads) {
                if (findMinThread.getInfeasiblePartition() != null) {
                    System.out.println("No feasible place for " + findMinThread.getInfeasiblePartition());
                    throw new NoAssignmentFoundException();
                }
                if (partitionObject == null || partitionObjectSimilarity > findMinThread.maxMinValue) {
                    partitionObject = findMinThread.maxPartitionObject;
                    partitionObjectSimilarity = findMinThread.maxMinValue;
                    partitionObjectMaxSwitchIndex = findMinThread.maxMinSwitchIndex;
                }
            }
        }
        return this;
    }

    private PartitionObject getAndUpdateOneChoicePartitions() throws NoAssignmentFoundException {
        final Iterator<Map.Entry<Switch, Set<PartitionObject>>> itr1 = oneChoicePartitions.entrySet().iterator();
        final Map.Entry<Switch, Set<PartitionObject>> firstEntry = itr1.next();
        final Set<PartitionObject> partitionObjects = firstEntry.getValue();
        final Switch host = firstEntry.getKey();
        final Iterator<PartitionObject> itr2 = partitionObjects.iterator();
        final PartitionObject partitionObject = itr2.next();
        itr2.remove();
        if (!itr2.hasNext()) {
            itr1.remove();
        }
        if (!partitionObject.getSwitchFeasible(switchIndexMap.get(host))) {
            System.out.println("One choice partition " + partitionObject + " on " + host + " become infeasible");
            throw new NoAssignmentFoundException();
        }
        return partitionObject;
    }

    private static class FindMinThread extends Thread {
        private int maxMinSwitchIndex = 0;
        private float maxMinValue = Float.MAX_VALUE;
        private final Iterator<PartitionObject> iterator;
        private PartitionObject maxPartitionObject = null;
        private final Map<Switch, Set<PartitionObject>> oneChoicePartitions;
        private PartitionObject infeasiblePartition = null;
        private final List<Switch> considerableSwitches;

        private FindMinThread(Iterator<PartitionObject> iterator, Map<Switch, Set<PartitionObject>> oneChoicePartitions,
                              List<Switch> considerableSwitches) {
            this.iterator = iterator;
            this.oneChoicePartitions = oneChoicePartitions;
            this.considerableSwitches = considerableSwitches;
        }



        @Override
        public void run() {
            while (true) {
                PartitionObject next;
                synchronized (iterator) {
                    if (iterator.hasNext()) {
                        next = iterator.next();
                    } else {
                        break;
                    }
                }
                final int minimumIndex = next.getMinimumAndOnlyFeasibleIndex();
                if (minimumIndex < 0) {
                    infeasiblePartition = next;
                    synchronized (iterator) {
                        //use up the iterator to make others finish soon
                        while (iterator.hasNext()) {
                            iterator.next();
                        }
                    }
                    return;
                }
                final float value = next.getSwitchScore(minimumIndex);
                if (maxPartitionObject == null || maxMinValue > value) {
                    maxMinValue = value;
                    maxPartitionObject = next;
                    maxMinSwitchIndex = minimumIndex;
                }
                //I need this onechoicespartitions list to be complete so don't use up iterator
                if (next.getOnlyFeasibleHost() > 0) {
                    synchronized (oneChoicePartitions) {
                        final Switch onlyFeasibleHost = considerableSwitches.get(next.getOnlyFeasibleHost());

                        Set<PartitionObject> onlyFeasibleCollection = oneChoicePartitions.get(onlyFeasibleHost);
                        if (onlyFeasibleCollection == null) {
                            onlyFeasibleCollection = new HashSet<>();
                            oneChoicePartitions.put(onlyFeasibleHost, onlyFeasibleCollection);
                        }
                        onlyFeasibleCollection.add(next);
                    }
                }
            }
        }

        public PartitionObject getInfeasiblePartition() {
            return infeasiblePartition;
        }

    }

}
