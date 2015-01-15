package edu.usc.enl.cacheflow.processor.flow;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.DestinationSelector;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/27/11
 * Time: 7:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class RandomFlowGenerator implements FlowGenerator{

    public List<Flow> generate(Random random, List<Switch> sources, List<Switch> destinations, List<RangeDimensionRange> propertiesRange,
                               DestinationSelector destinationSelector, CustomRandomFlowDistribution flowDistribution) {

        List<Flow> flows = new LinkedList<Flow>();
        SortedMap<DimensionInfo, Long> properties = new TreeMap<DimensionInfo, Long>();
        for (Switch source : sources) {
            int flowsPerSource = flowDistribution.getRandomFlowNum(random.nextDouble());
            destinationSelector.setSource(source, destinations);
            for (int i = 0; i < flowsPerSource; i++) {
                Switch destination = destinationSelector.getDestination(random, source, destinations);
                for (RangeDimensionRange dimensionRange : propertiesRange) {
                    properties.put(dimensionRange.getInfo(), dimensionRange.getRandomNumber(random));
                }
                Flow flow = new Flow(flowDistribution.getRandomFlowSize(random.nextDouble()),source, destination, properties);
                System.out.println(source.toString() + ", " + destination.toString() + " " + flow.toString());
                flows.add(flow);
            }
        }
        return flows;
    }
}
