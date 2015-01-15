package edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition;

import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 10/5/11
 * Time: 9:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class RandomSwitchSelection extends SwitchSelection{
    private final Random random;

    public RandomSwitchSelection(Random random) {
        this.random = random;
    }

    public <T extends List<Switch>> T sortSwitches(T toFill, Map<Partition, Switch> placement,
                                                   Partition partition) {
        Collections.shuffle(toFill,random);
        return toFill;
    }

    @Override
    public String toString() {
        return "Random";
    }

}
