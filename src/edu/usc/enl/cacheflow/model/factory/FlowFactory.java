package edu.usc.enl.cacheflow.model.factory;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/8/12
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class FlowFactory extends FileFactory<Flow> {

    public List<DimensionInfo> dimensionInfos;
    public final Map<String, Switch> switches;

    public FlowFactory(StopCondition stopCondition, Topology topology) {
        super(stopCondition);
        switches = new HashMap<String, Switch>();
        for (Switch aSwitch : topology.getSwitches()) {
            switches.put(aSwitch.getId(), aSwitch);
        }
    }

    @Override
    public void parseHeaderLine(BufferedReader reader, Map<String, Object> parameters) throws IOException {
        final RuleFactory helper = new RuleFactory(stopCondition);
        helper.parseHeaderLine(reader, parameters);
        dimensionInfos = helper.getDimensionInfos();
    }

    @Override
    protected Flow create(String line) {
        Long[] properties = new Long[dimensionInfos.size()];
        StringTokenizer st = new StringTokenizer(line, ",");
        //String[] split = line.split(",");
        int i = 0;
        for (DimensionInfo dimensionInfo : dimensionInfos) {
            properties[i++] = Long.parseLong(st.nextToken());
        }

        Switch source = switches.get(st.nextToken());
        Switch destination = switches.get(st.nextToken());
        if (source == null || destination == null) {
            System.out.println("Could not find source or destination for " + line);
            throw new RuntimeException("Could not find source or destination for " + line);
        }
        long traffic = Long.parseLong(st.nextToken());
        Flow flow = new Flow(traffic, source, destination, properties);
        flow.setNew(Boolean.parseBoolean(st.nextToken()));
        return flow;
    }


}
