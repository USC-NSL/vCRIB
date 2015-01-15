package edu.usc.enl.cacheflow.processor.flow;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.AbstractClassBenchFlowGenerator;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.DestinationSelector;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.LocalizedDestinationSelector;
import edu.usc.enl.cacheflow.processor.flow.destinationselector.UniformDestinationSelector;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/27/11
 * Time: 10:12 AM
 * <br/> Select completely
 */
public class ConvertClassBenchFlowsProcessor {
    private Random random;
    private boolean edge;
    private boolean local;
    private CustomRandomFlowDistribution flowDistribution;
    private AbstractClassBenchFlowGenerator flowGenerator;

    public ConvertClassBenchFlowsProcessor(CustomRandomFlowDistribution flowDistribution,
                                           AbstractClassBenchFlowGenerator flowGenerator,
                                           Random random, boolean edge, boolean local) {
        super();
        this.random = random;
        this.local = local;
        this.edge = edge;
        this.flowDistribution = flowDistribution;
        this.flowGenerator = flowGenerator;
    }

    public List<Flow> process(Topology topology, List<String> inputString) throws Exception {
        Collections.shuffle(inputString);
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
        return flowGenerator.generate(random,senderReceiverSwitches,senderReceiverSwitches,inputString,destinationSelector,flowDistribution);
    }
}
