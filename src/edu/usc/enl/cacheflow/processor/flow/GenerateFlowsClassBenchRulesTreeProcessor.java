package edu.usc.enl.cacheflow.processor.flow;

import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.rulebased.TreeIPTreeFlow;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.DestinationSelector;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.LocalizedDestinationSelector;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.UniformDestinationSelector;

import java.io.PrintWriter;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/26/12
 * Time: 1:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateFlowsClassBenchRulesTreeProcessor {
    private Random random;
    private boolean edge;
    private boolean local;
    private CustomRandomFlowDistribution flowDistribution;
    private TreeIPTreeFlow flowGenerator;

    public GenerateFlowsClassBenchRulesTreeProcessor(CustomRandomFlowDistribution flowDistribution,
                                                     TreeIPTreeFlow flowGenerator,
                                                     Random random, boolean edge, boolean local) {
        this.random = random;
        this.local = local;
        this.edge = edge;
        this.flowDistribution = flowDistribution;
        this.flowGenerator = flowGenerator;

    }

    public void process(Topology topology, int ruleSize, PrintWriter writer) {
        List<Switch> senderReceiverSwitches;
        if (edge) {
            senderReceiverSwitches = topology.findEdges();
        } else {
            senderReceiverSwitches = topology.getSwitches();
        }
        DestinationSelector destinationSelector;
        if (local) {
            destinationSelector = new LocalizedDestinationSelector(topology, flowDistribution.getLocalizedFlowDistribution());
        } else {
            destinationSelector = new UniformDestinationSelector(topology);
        }
        flowGenerator.generate(ruleSize, random, senderReceiverSwitches, destinationSelector, flowDistribution, writer, topology);
    }
}
