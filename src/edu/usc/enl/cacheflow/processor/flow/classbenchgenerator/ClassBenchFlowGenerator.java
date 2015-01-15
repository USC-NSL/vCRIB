package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.DestinationSelector;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/27/11
 * Time: 8:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class ClassBenchFlowGenerator extends AbstractClassBenchFlowGenerator {
    @Override
    public List<Flow> generate(Random random, List<Switch> sources, List<Switch> destinations, List<String> classbenchProperties, DestinationSelector destinationSelector, CustomRandomFlowDistribution flowDistribution) {

        List<Flow> flows = new LinkedList<Flow>();
        Iterator<String> propertiesIterator = classbenchProperties.iterator();
        for (Switch source : sources) {
            int flowPerSwitch = flowDistribution.getRandomFlowNum(random.nextDouble());
            destinationSelector.setSource(source, destinations);
            SortedMap<DimensionInfo, Long> propertiesMap = new TreeMap<DimensionInfo, Long>();
            for (int i = 0; i < flowPerSwitch; i++) {
                if (!propertiesIterator.hasNext()) {
                    propertiesIterator = classbenchProperties.iterator();
                    Util.logger.warning("Input classbench flows are not enough");
                }
                final String properties = propertiesIterator.next();
                parseProperties(propertiesMap, properties);

                Switch destination = destinationSelector.getDestination(random, source, destinations);
                Flow flow = new Flow(flowDistribution.getRandomFlowSize(random.nextDouble()), source, destination, propertiesMap);
                //System.out.println(source.toString() + ", " + destination.toString() + " " + flow.toString());
                flows.add(flow);
            }
        }
        return flows;
    }

}
