package edu.usc.enl.cacheflow.processor.flow.online;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/27/12
 * Time: 3:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class FlowMovementProcessor {
    private Random random;
    public static final String SRCT_IP = "SRCIP";
    private double srcChangeRatio;
    private final boolean edge;

    public FlowMovementProcessor( Random random, double srcChangeRatio,boolean edge) {
        this.random = random;
        this.srcChangeRatio = srcChangeRatio;
        this.edge = edge;
    }

    public Collection<Flow> process(Topology topology,Collection<Flow> flows) throws Exception {
        if (flows.size() == 0) {
            return flows;
        }
        int srcIPDimensionInfoIndex = getSrcIPIndex();
        if (srcIPDimensionInfoIndex == -1) {
            Util.logger.severe("SRCIP dimension info not found in the properties of flows");
            return flows;
        }

        List<Switch> sourceSwitches;
        if (edge) {
            sourceSwitches = topology.findEdges();
        } else {
            sourceSwitches = topology.getSwitches();
        }
        Map<Long, List<Flow>> sourceFlowsMap = new HashMap<Long, List<Flow>>();

        for (Flow flow : flows) {
            List<Flow> flowsList = sourceFlowsMap.get(flow.getProperty(srcIPDimensionInfoIndex));
            if (flowsList == null) {
                flowsList = new LinkedList<Flow>();
                sourceFlowsMap.put(flow.getProperty(srcIPDimensionInfoIndex), flowsList);
            }
            flowsList.add(flow);
        }
        List<Long> sourceIPs = new ArrayList<Long>(sourceFlowsMap.keySet());
        Collections.shuffle(sourceIPs,Util.random);
        final int toBeChangedSources = (int) (sourceFlowsMap.size() * srcChangeRatio);
        for (int i = 0; i < toBeChangedSources && sourceIPs.size() > 0; i++) {
            final List<Flow> toBeMovedFlows = sourceFlowsMap.get(sourceIPs.remove(0));
            Switch source = toBeMovedFlows.get(0).getSource();
            Switch newSource = null;
            while (newSource == null || source.equals(newSource)) {
                newSource = sourceSwitches.get(random.nextInt(sourceSwitches.size()));
            }
            for (Flow toBeMovedFlow : toBeMovedFlows) {
                toBeMovedFlow.setSource(newSource);
            }
        }
        return flows;
    }

    private int getSrcIPIndex() {
        int srcIPDimensionInfoIndex = -1;
        List<DimensionInfo> dimensionInfos = Util.getDimensionInfos();
        for (int i1 = 0, dimensionInfosSize = dimensionInfos.size(); i1 < dimensionInfosSize; i1++) {
            DimensionInfo dimensionInfo = dimensionInfos.get(i1);
            if (dimensionInfo.getName().equals(SRCT_IP)) {
                srcIPDimensionInfoIndex = i1;
                break;
            }
        }
        return srcIPDimensionInfoIndex;
    }
}
