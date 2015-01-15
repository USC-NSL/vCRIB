package edu.usc.enl.cacheflow.processor.topology.tree;

import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.processor.topology.TopologyFileFormatException;

import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/29/11
 * Time: 9:51 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractTreeTopologyGenerator {
    protected TreeTemplate template;

    protected AbstractTreeTopologyGenerator(TreeTemplate template) {
        this.template = template;
    }

    protected void addController(Aggregator aggregator, Topology topology,Set<Rule> memoryTemplate) throws TopologyFileFormatException {
        //controller
        List<String> controllerProperties = template.getLevelNodeProperties(Switch.CONTROLLER_ID);
        if (controllerProperties == null) {
            throw new TopologyFileFormatException("Controller properties not found in the template");
        }
        topology.setControllerSwitch(new Switch.ControllerSwitch(aggregator, 0,memoryTemplate));
    }

}
