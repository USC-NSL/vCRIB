package edu.usc.enl.cacheflow.algorithms.feasibility.general;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

class UpdateScoreThread extends Thread {
    private final Iterator<PartitionObject> itr;
    private final List<Switch> considerableSwitches;
    private final Switch updatedSwitch;
    private final FeasiblePlacer feasiblePlacer;


    public UpdateScoreThread(Iterator<PartitionObject> itr, List<Switch> considerableSwitches,
                             Switch updatedSwitch, FeasiblePlacer feasiblePlacer) {
        this.itr = itr;
        this.considerableSwitches = considerableSwitches;
        this.feasiblePlacer = feasiblePlacer;
        this.updatedSwitch = updatedSwitch;
    }

    @Override
    public void run() {
        int processed = 0;
        while (true) {
            PartitionObject partitionObject;
            synchronized (itr) {
                if (itr.hasNext()) {
                    partitionObject = itr.next();
                } else {
                    break;
                }
            }

            //if aSwitch is my src need to update all scores!
            if (updatedSwitch == null) {
                //update all scores
                if (partitionObject.isBaseStatesEmpty()) {
                    //find base state
                    for (Map.Entry<Class<? extends Switch>, Switch> entry : feasiblePlacer.getBaseSwitches().entrySet()) {
                        //create the feasible state
                        final Switch baseSwitch = entry.getValue();
                        final Switch.FeasibleState baseState;
                        try {
                            baseState = feasiblePlacer.place(partitionObject, baseSwitch, false);
                        } catch (Switch.InfeasibleStateException e) {
                            e.printStackTrace();
                            throw new RuntimeException("It should not check this but Base " + baseSwitch + "+ is not large enough for " + partitionObject.getPartition());
                        }
                        partitionObject.setBaseState(entry.getKey(), baseState);
                    }
                }
                for (Switch considerableSwitch : considerableSwitches) {
                    update(partitionObject, considerableSwitch);
                }
            } else {
                //update scores for those switches only
                update(partitionObject, updatedSwitch);
            }
            processed++;
        }
    }

    private void update(PartitionObject partitionObject, Switch aSwitch) {
        double score = Float.MAX_VALUE;
        final Integer index = feasiblePlacer.getSwitchIndexMap().get(aSwitch);
        try {
            score = feasiblePlacer.getScore(partitionObject, aSwitch);
            partitionObject.setSwitchFeasible(index, true);
        } catch (Switch.InfeasibleStateException e) {
            partitionObject.setSwitchFeasible(index, false);
        }
        partitionObject.setScore(index, (float) score);
    }


}