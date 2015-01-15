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
class SeedPartitionObjectFinder2 {
    private final List<Switch> considerableSwitches;
    private final List<PartitionObject> partitionObjects;
    private final FindMinThread[] findMinThreads;
    private PartitionObject partitionObject = null;
    private int partitionObjectMaxSwitchIndex = -1;
    private final FeasiblePlacer2 feasiblePlacer2;


    public SeedPartitionObjectFinder2(List<Switch> considerableSwitches, List<PartitionObject> partitionObjects,
                                      Map<Switch, Integer> switchIndexMap,
                                      int threadNum, FeasiblePlacer2 feasiblePlacer2) {
        this.considerableSwitches = considerableSwitches;
        this.partitionObjects = partitionObjects;
        this.feasiblePlacer2 = feasiblePlacer2;
        this.findMinThreads = new FindMinThread[threadNum];
    }

    public PartitionObject getPartitionObject() {
        return partitionObject;
    }

    public int getPartitionObjectMaxSwitchIndex() {
        return partitionObjectMaxSwitchIndex;
    }

    public SeedPartitionObjectFinder2 invoke() throws NoAssignmentFoundException {
        partitionObject = partitionObjects.get(Util.random.nextInt(partitionObjects.size()));

        Iterator<Switch> iterator = considerableSwitches.iterator();
        for (int i = 0; i < findMinThreads.length; i++) {
            findMinThreads[i] = new FindMinThread(iterator, partitionObject, feasiblePlacer2);
        }

        Util.runThreads(findMinThreads);

        //if there is a one choice switch get it
        int switchIndex = 0;
        partitionObjectMaxSwitchIndex = -1;
        double minScore = 0;
        for (Switch considerableSwitch : considerableSwitches) {
            if (partitionObject.getSwitchFeasible(switchIndex) && (partitionObjectMaxSwitchIndex < 0 || minScore > partitionObject.getSwitchScore(switchIndex))) {
                partitionObjectMaxSwitchIndex = switchIndex;
                minScore = partitionObject.getSwitchScore(switchIndex);
            }
            switchIndex++;
        }
        return this;
    }

    private static class FindMinThread extends Thread {
        private final Iterator<Switch> iterator;
        private final PartitionObject partitionObject1;
        private final FeasiblePlacer2 feasiblePlacer2;

        private FindMinThread(Iterator<Switch> iterator,
                              PartitionObject partitionObject1, FeasiblePlacer2 feasiblePlacer2) {
            this.iterator = iterator;
            this.partitionObject1 = partitionObject1;
            this.feasiblePlacer2 = feasiblePlacer2;
        }


        @Override
        public void run() {
            while (true) {
                Switch aSwitch;
                synchronized (iterator) {
                    if (iterator.hasNext()) {
                        aSwitch = iterator.next();
                    } else {
                        break;
                    }
                }
                double score = Float.MAX_VALUE;
                final Integer index = feasiblePlacer2.getSwitchIndexMap().get(aSwitch);
                try {
                    score = feasiblePlacer2.getScore(partitionObject1, aSwitch, true);
                    partitionObject1.setSwitchFeasible(index, true);
                } catch (Switch.InfeasibleStateException e) {
                    partitionObject1.setSwitchFeasible(index, false);
                }
                partitionObject1.setScore(index, (float) score);
            }
        }
    }

}
