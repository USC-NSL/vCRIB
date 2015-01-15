package edu.usc.enl.cacheflow.model.topology;

import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 6/5/12
 * Time: 6:58 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractFatTree extends Topology{
    public static final String EDGE_STRING = "Edge";
    public static final String CORE_STRING = "Core";
    public static final String AGGREGATE1_STRING = "Agg1";
    public static final String AGGREGATE2_STRING = "Agg2";

    public abstract void addCoreSwitch(Switch aSwitch);
    public abstract int getCoresSize();
}
