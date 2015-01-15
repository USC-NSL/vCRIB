package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.FlowGenerator;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.DestinationSelector;
import edu.usc.enl.cacheflow.util.Util;

import java.util.List;
import java.util.Random;
import java.util.SortedMap;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/11/12
 * Time: 8:31 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractClassBenchFlowGenerator implements FlowGenerator{
    public abstract List<Flow> generate(Random random, List<Switch> sources, List<Switch> destinations,  List<String> classbenchProperties,
                                        DestinationSelector destinationSelector, CustomRandomFlowDistribution flowDistribution);

    protected void parseProperties(SortedMap<DimensionInfo, Long> propertiesMap, String properties) {
        final String[] fields = properties.split("\t");
        propertiesMap.put(Util.SRC_IP_INFO, Long.parseLong(fields[0]));
        propertiesMap.put(Util.DST_IP_INFO, Long.parseLong(fields[1]));
        propertiesMap.put(Util.SRC_PORT_INFO, Long.parseLong(fields[2]));
        propertiesMap.put(Util.DST_PORT_INFO, Long.parseLong(fields[3]));
        propertiesMap.put(Util.PROTOCOL_INFO, Long.parseLong(fields[4]));
    }

}
