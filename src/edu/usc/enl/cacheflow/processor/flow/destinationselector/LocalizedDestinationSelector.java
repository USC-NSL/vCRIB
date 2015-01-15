package edu.usc.enl.cacheflow.processor.flow.destinationselector;

import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.CustomRandomGenerator;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/27/11
 * Time: 7:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class LocalizedDestinationSelector extends DestinationSelector {
    private CustomRandomGenerator<Integer> globalPathLengthWeight;
    public Map<Integer, List<Switch>> pathLengthDestination;

    public LocalizedDestinationSelector(Topology topology, CustomRandomGenerator<Integer> globalPathLengthWeight) {
        super(topology);
        this.globalPathLengthWeight = globalPathLengthWeight;
    }

    @Override
    public void setSource(Switch source, Collection<Switch> destinations) {
        pathLengthDestination = new HashMap<Integer, List<Switch>>();
        for (Switch destination : destinations) {
            int pathLength = getPathLength(source, destination);
            List<Switch> switches = pathLengthDestination.get(pathLength);
            if (switches == null) {
                switches = new LinkedList<Switch>();
                pathLengthDestination.put(pathLength, switches);
            }
            switches.add(destination);
        }
    }

    @Override
    public DestinationSelector createNew() {
        return new LocalizedDestinationSelector(topology, globalPathLengthWeight);
    }

    @Override
    public Map<? extends Object, Double> getCategoryProb() {
        return globalPathLengthWeight.getObjectWeights();
    }

    @Override
    public List<Switch> getSwitchInCategory(Object category) {
        return pathLengthDestination.get(category);
    }

    @Override
    public Switch getDestination(Random random, Switch source, List<Switch> destinations) {

        List<Switch> selectedList = pathLengthDestination.get(globalPathLengthWeight.getRandom(random.nextDouble()));
        return selectedList.get(random.nextInt(selectedList.size()));
    }

    public static Object getARandomObject(Random random, Map<?, Double> weights) {
        double sum = 0;
        TreeMap<Double, Object> cummuliativeWeights = new TreeMap<Double, Object>();
        for (Map.Entry<?, Double> objectWeightEntry : weights.entrySet()) {
            final Double probability = objectWeightEntry.getValue();
            if (probability > 0) {
                sum += probability;
                cummuliativeWeights.put(sum, objectWeightEntry.getKey());
            }
        }
        final Map.Entry<Double, Object> doubleObjectEntry = cummuliativeWeights.ceilingEntry(random.nextDouble() * sum);
        return doubleObjectEntry.getValue();
    }
}
