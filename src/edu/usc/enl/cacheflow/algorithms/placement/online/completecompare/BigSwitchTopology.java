package edu.usc.enl.cacheflow.algorithms.placement.online.completecompare;

import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.AbstractSwitch;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.BigSwitch;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch.DummySwitch;
import edu.usc.enl.cacheflow.model.topology.FatTree;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/21/12
 * Time: 10:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class BigSwitchTopology {
    private Map<Switch, AbstractSwitch> switchMap = new HashMap<Switch, AbstractSwitch>();

    public BigSwitchTopology(FatTree topology) {
        //create switches
        final Set<Switch> cores = topology.getCores();
        BigSwitch bigSwitch = new BigSwitch("Core", cores);
        for (Switch aSwitch : topology.getSwitches()) {
            if (!cores.contains(aSwitch)) {
                switchMap.put(aSwitch, new DummySwitch(aSwitch));
            } else {
                switchMap.put(aSwitch, bigSwitch);
            }
        }

        //create links
        for (Switch aSwitch : switchMap.keySet()) {
            for (Link link : aSwitch.getLinks()) {
                switchMap.get(aSwitch).addLinkTo(switchMap.get(link.getEnd()), link, topology);
            }
        }
    }

    public AbstractSwitch get(Switch aSwitch) {
        return switchMap.get(aSwitch);
    }

    public Collection<AbstractSwitch> getSwitches() {
        return switchMap.values();
    }
}
