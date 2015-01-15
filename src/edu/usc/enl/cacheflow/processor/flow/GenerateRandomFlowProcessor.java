package edu.usc.enl.cacheflow.processor.flow;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.DestinationSelector;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.LocalizedDestinationSelector;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.UniformDestinationSelector;
import edu.usc.enl.cacheflow.util.Util;

import java.util.List;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/30/11
 * Time: 1:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateRandomFlowProcessor  {
    private Random random;
    private boolean edge;
    private boolean local;
    private CustomRandomFlowDistribution flowDistribution;
    private RandomFlowGenerator flowGenerator;

    public GenerateRandomFlowProcessor(Random random, boolean edge, boolean local, CustomRandomFlowDistribution flowDistribution,
                                       RandomFlowGenerator flowGenerator) {
        this.random = random;
        this.edge = edge;
        this.local = local;
        this.flowDistribution = flowDistribution;
        this.flowGenerator = flowGenerator;
    }

    protected List<Flow> processRequirements(Topology topology, List<Rule> rules) throws Exception {
        final List<RangeDimensionRange> totalRanges = DimensionInfo.getTotalRanges(Util.getDimensionInfos());
        List<Switch> senderReceiverSwitches;
        if (edge) {
            senderReceiverSwitches = topology.findEdges();
        } else {
            senderReceiverSwitches = topology.getSwitches();
        }
        DestinationSelector destinationSelector;
        if (local){
            destinationSelector=new LocalizedDestinationSelector(topology,flowDistribution.getLocalizedFlowDistribution());
        }else{
            destinationSelector=new UniformDestinationSelector(topology);
        }
        return flowGenerator.generate(random,senderReceiverSwitches,senderReceiverSwitches,totalRanges,destinationSelector,flowDistribution);
    }
}
