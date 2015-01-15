package edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch;

import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.link.AbstractLink;
import edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.link.BigLink;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/21/12
 * Time: 9:35 AM
 * To change this template use File | Settings | File Templates.
 */
public class BigSwitch extends AbstractSwitch {
    private Collection<Switch> switches;


    public BigSwitch(String id, Collection<Switch> switches) {
        super(id);
        this.switches = switches;
    }


    /*@Override
    public String toString() {
        return "BigSwitch " + super.toString();
    }*/

    public void resetAllBigLinks(){
        for (AbstractLink link : linksTo.values()) {
            ((BigLink) link).resetFlowCache();
            ((BigLink) link.getOtherSide()).resetFlowCache();
        }
    }
}
