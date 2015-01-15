package edu.usc.enl.cacheflow.processor.flow.classbenchgenerator;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
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
public class IPRangeClassBenchFlowGenerator extends AbstractClassBenchFlowGenerator {


    protected Map<RangeDimensionRange, Switch> getIPRangeSwitchMap(Random random, List<Switch> sources, List<String> classbenchProperties, CustomRandomFlowDistribution flowDistribution, List<Flow> flows) {
        //load flows
        loadFlows(random, sources, classbenchProperties, flowDistribution, flows);

        int SRCIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        Collections.sort(flows, Flow.getFieldComparator(SRCIPIndex));

        //assign IPs for destinations
        Map<RangeDimensionRange, Switch> switchIPRangeMap = new HashMap<RangeDimensionRange, Switch>();
        int i = 0;
        final int IPperSwitch = flows.size() / sources.size();
        for (int i1 = 0, sourcesSize = sources.size(); i1 < sourcesSize - 1; i1++) {
            Switch source = sources.get(i1);
            switchIPRangeMap.put(new RangeDimensionRange(flows.get(IPperSwitch * i).getProperty(SRCIPIndex),
                    flows.get(IPperSwitch * (i + 1) - 1).getProperty(SRCIPIndex), Util.SRC_IP_INFO),
                    source);
            i++;
        }
        //last source
        switchIPRangeMap.put(new RangeDimensionRange(flows.get(IPperSwitch * i).getProperty(SRCIPIndex),
                flows.get(flows.size() - 1).getProperty(SRCIPIndex), Util.SRC_IP_INFO),
                sources.get(sources.size() - 1));
        return switchIPRangeMap;
    }

    protected void loadFlows(Random random, List<Switch> sources, List<String> classbenchProperties, CustomRandomFlowDistribution flowDistribution, List<Flow> flows) {
        final Switch dummySource = sources.get(0);
        SortedMap<DimensionInfo, Long> propertiesMap = new TreeMap<DimensionInfo, Long>();
        for (String properties : classbenchProperties) {
            parseProperties(propertiesMap, properties);
            Flow flow = new Flow(flowDistribution.getRandomFlowSize(random.nextDouble()), dummySource, dummySource, propertiesMap);
            flows.add(flow);
        }
    }

    @Override
    public List<Flow> generate(Random random, List<Switch> sources, List<Switch> destinations, List<String> classbenchProperties, DestinationSelector destinationSelector, CustomRandomFlowDistribution flowDistribution) {

        List<Flow> flows = new ArrayList<Flow>(classbenchProperties.size());
        Map<RangeDimensionRange, Switch> switchIPRangeMap = getIPRangeSwitchMap(random, sources, classbenchProperties, flowDistribution, flows);


        final int IPperSwitch = flows.size() / sources.size();
        int DSTIPIndex = Util.getDimensionInfoIndex(Util.DST_IP_INFO);
        // correct source and destination of flows
        int k = 0;
        List<Flow> outputFlows = new LinkedList<Flow>();
        for (Switch source : sources) {
            int randomDest = 0;
            int flowPerSwitch = flowDistribution.getRandomFlowNum(random.nextDouble());
            final Iterator<Flow> flowIterator = flows.listIterator(IPperSwitch * (k++));
            destinationSelector.setSource(source, sources);
            for (int j = 0; j < flowPerSwitch; j++) {
                final Flow flow = flowIterator.next();
                flow.setSource(source);
                //Switch destination = getDestination(flow.getProperty(DSTIPIndex), switchIPRangeMap);
                Switch destination = null;
                if (destination == null) {
                    destination = destinationSelector.getDestination(random, source, sources);
                    randomDest++;
                }
                flow.setDestination(destination);
                outputFlows.add(flow);
            }
            System.out.println(randomDest);
        }

        return outputFlows;
    }
}
