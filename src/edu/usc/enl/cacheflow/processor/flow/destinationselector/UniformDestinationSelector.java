package edu.usc.enl.cacheflow.processor.flow.destinationselector;

import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/27/11
 * Time: 7:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class UniformDestinationSelector extends DestinationSelector{
    private List<Switch> destinations;

    public UniformDestinationSelector(Topology topology) {
        super(topology);
    }


    public Switch getDestination(Random random, Switch source, List<Switch> destinations) {
        return destinations.get(random.nextInt(destinations.size()));
    }


    @Override
    public void setSource(Switch source, Collection<Switch> destinations) {
        this.destinations =new ArrayList<Switch>(destinations);
    }

    @Override
    public DestinationSelector createNew() {
        return new UniformDestinationSelector(topology);
    }

    @Override
    public Map<? extends Object, Double> getCategoryProb() {
        final HashMap<Object, Double> objectDoubleHashMap = new HashMap<Object, Double>();
        objectDoubleHashMap.put(0,1d);
        return objectDoubleHashMap;
    }

    @Override
    public List<Switch> getSwitchInCategory(Object category) {
        return destinations;
    }
}
