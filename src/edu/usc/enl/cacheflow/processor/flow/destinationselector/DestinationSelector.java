package edu.usc.enl.cacheflow.processor.flow.destinationselector;

import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/27/11
 * Time: 7:33 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class DestinationSelector {
    protected Topology topology;
    private final Topology.SwitchPair pair = new Topology.SwitchPair(null, null);

    public DestinationSelector(Topology topology) {
        this.topology = topology;
    }

    public abstract Switch getDestination(Random random, Switch source, List<Switch> destinations);

    public void setSource(Switch source, Collection<Switch> destinations) {

    }

    public abstract DestinationSelector createNew();

    public int getPathLength(Switch source, Switch destination) {
        return topology.getPathLength(source,destination);
    }

    public abstract Map<? extends Object, Double> getCategoryProb();

    public abstract List<Switch> getSwitchInCategory(Object category);

}
