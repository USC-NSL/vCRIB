package edu.usc.enl.cacheflow.algorithms.feasibility.general;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

 class PartitionObject //implements Comparable<PartitionObject>
{
    private final Partition partition;
    private final float[] switchScores;
    private final boolean[] switchFeasible;
    private final Switch src;
    private Map<Class<? extends Switch>, Switch.FeasibleState> baseStates;
    private int onlyFeasibleHost = -1;

    public PartitionObject(List<Switch> considerableSwitches, Partition partition, Switch src) {
        this.partition = partition;
        this.src = src;
        switchScores = new float[considerableSwitches.size()];
        switchFeasible = new boolean[considerableSwitches.size()];
        baseStates = new HashMap<>();
    }

    public void setBaseState(Class<? extends Switch> switchClass, Switch.FeasibleState state) {
        baseStates.put(switchClass, state);
    }

    public Switch.FeasibleState getBaseState(Class<? extends Switch> switchClass) {
        return baseStates.get(switchClass);
    }

    /*
        public Switch getMaxSwitch() {
            return maxSwitch;
        }
    */

    public Switch getSrc() {
        return src;
    }

    /*@Override
    public int compareTo(PartitionObject o) {
        if (equals(o)) {
            return 0;
        }
        final int compare = Float.compare(o.maxSwitchSimilarityScore, maxSwitchSimilarityScore);
        if (compare == 0) {
            return partition.getId() - o.partition.getId();
        }
        return compare;//larger is first in sort
    }*/

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionObject that = (PartitionObject) o;

        if (partition != null ? !partition.equals(that.partition) : that.partition != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return partition != null ? partition.hashCode() : 0;
    }

    public int getMinimumAndOnlyFeasibleIndex() {
        int minimumIndex = -1;
        float minimum = 0;
        int i = 0;
        for (float switchSimilarityScore : switchScores) {
            if (switchFeasible[i]) {
                if (minimumIndex < 0) {
                    onlyFeasibleHost = i;
                } else {
                    onlyFeasibleHost = -1;
                }
                if (minimumIndex < 0 || minimum > switchSimilarityScore) {
                    minimum = switchSimilarityScore;
                    minimumIndex = i;
                }
            }
            i++;
        }
        return minimumIndex;
    }

    public int getOnlyFeasibleHost() {
        return onlyFeasibleHost;
    }

    public double getSwitchSimilarity(int index, Switch host) {
        return getBaseScore(host) - switchScores[index];
    }

    public float getSwitchScore(int index) {
        return switchScores[index];
    }

    public void setSwitchFeasible(int index, boolean value) {
        switchFeasible[index] = value;
    }

    public boolean getSwitchFeasible(int index) {
        return switchFeasible[index];
    }

    private double getBaseScore(Switch newHost) {
        return newHost.getUsedResources(baseStates.get(newHost.getClass()));
    }


    /*public void updateMax(Collection<Switch> toCheck, List<Switch> considerableSwitches, Map<Switch, Integer> switchIndexMap) {
        //if min is updated find min again
        if (toCheck == null || toCheck.contains(maxSwitch) || toCheck == considerableSwitches) {
            maxSwitchSimilarityScore = -Float.MAX_VALUE;
            maxSwitch = null;
            int i = 0;
            for (float score : switchScores) {
                if (maxSwitch == null || maxSwitchSimilarityScore < score) {
                    maxSwitch = considerableSwitches.get(i);
                    maxSwitchSimilarityScore = score;
                }
                i++;
            }
        }
        // otherwise compare the updated switches score with min
        else {
            for (Switch aSwitch : toCheck) {
                final float score = switchScores[switchIndexMap.get(aSwitch)];
                if (maxSwitch == null || maxSwitchSimilarityScore > score) {
                    maxSwitch = aSwitch;
                    maxSwitchSimilarityScore = score;
                }
            }
        }
    }*/

    public Partition getPartition() {
        return partition;
    }

    public void setScore(int i, float similarity) {
        switchScores[i] = similarity;
    }

    /*public float getMinScore() {
        return maxSwitchSimilarityScore;
    }*/

    public boolean isBaseStatesEmpty() {
        return baseStates.isEmpty();
    }

    @Override
    public String toString() {
        return "PartitionObject{" +
                "src=" + src +
                ", partition=" + partition +
                '}';
    }

    public static class PartitionIterator implements Iterator<Partition> {
        private final Iterator<PartitionObject> internalItr;

        public PartitionIterator(Iterator<PartitionObject> internalItr) {
            this.internalItr = internalItr;
        }

        @Override
        public boolean hasNext() {
            return internalItr.hasNext();
        }

        @Override
        public Partition next() {
            return internalItr.next().getPartition();
        }

        @Override
        public void remove() {
            internalItr.remove();
        }
    }
}