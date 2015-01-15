package edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/24/12
 * Time: 10:19 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class TrafficAwareSwitchSelection extends  SwitchSelection{
    public Map<Switch,Long> switchTrafficMap;
    public  Comparator<Switch> comparator;
    public Map<Switch, Integer> tieBreaker;
    public List<Switch> switches;

    public long getTrafficIf(Switch host) {
        return switchTrafficMap.get(host);
    }

    /**
     * can contain switches that did not fill by sort switch but their value must be Long.MAX_VALUE
     * @return
     */

    public Map<Switch, Long> getTrafficMap() {
        return switchTrafficMap;
    }
}
