package edu.usc.enl.cacheflow.algorithms.placement.online.completecompare.aswitch;

import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/21/12
 * Time: 10:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class DummySwitch extends AbstractSwitch {
    private final Switch realSwitch;

    public DummySwitch(Switch realSwitch) {
        super(realSwitch.getId());
        this.realSwitch = realSwitch;
    }

    public Switch getRealSwitch() {
        return realSwitch;
    }
/*
    @Override
    public String toString() {
        return "DummySwitch "+super.toString();
    }*/
}
