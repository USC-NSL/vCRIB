package edu.usc.enl.cacheflow.algorithms.placement.switchselectioncluster;

import edu.usc.enl.cacheflow.model.rule.Cluster;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/25/12
 * Time: 8:58 AM
 * To change this template use File | Settings | File Templates.
 */
public class RandomSwitchSelectionCluster extends SwitchSelectionCluster{
    private final Random random;

    public RandomSwitchSelectionCluster(Random random) {
        this.random = random;
    }

    @Override
    public  <T extends List<Switch>> T sortSwitches(T toFill, Map<Cluster, Switch> assignment, Cluster cluster) {
        Collections.shuffle(toFill,random);
        return toFill;
    }

    @Override
    public String toString() {
        return "Random Switch Selection Cluster";
    }
}
