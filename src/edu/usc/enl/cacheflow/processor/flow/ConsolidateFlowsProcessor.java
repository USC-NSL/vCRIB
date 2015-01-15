package edu.usc.enl.cacheflow.processor.flow;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/26/12
 * Time: 5:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class ConsolidateFlowsProcessor  {

    public List<Flow> process(List<Flow> flows) throws Exception {
        Map<Flow, Long> flowSizes = new HashMap<Flow, Long>();
        long oldSum = 0;
        for (Flow flow : flows) {
            Long oldTraffic = flowSizes.get(flow);
            long traffic = 0;
            if (oldTraffic != null) {
                traffic = oldTraffic;
            }
            traffic += flow.getTraffic();
            flowSizes.put(flow, traffic);
            oldSum += flow.getTraffic();
        }
        List<Flow> outputFlows = new ArrayList<Flow>(flowSizes.size());
        long sum = 0;
        for (Map.Entry<Flow, Long> flowLongEntry : flowSizes.entrySet()) {
            final Flow flow = flowLongEntry.getKey();
            flow.setTraffic(flowLongEntry.getValue());
            outputFlows.add(flow);
            sum += flow.getTraffic();
        }
        if (sum != oldSum) {
            Util.logger.severe("old sum != new sum");
        }
        return outputFlows;
    }
}
