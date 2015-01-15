package edu.usc.enl.cacheflow.processor.topology.tree;

import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.AbstractFatTree;
import edu.usc.enl.cacheflow.model.topology.Large4LevelFatTree;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.processor.topology.TopologyFileFormatException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/5/11
 * Time: 3:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class FatTreeTopologyGenerator extends AbstractTreeTopologyGenerator {
    public int degree = 2;
    public final int digits;

    public FatTreeTopologyGenerator(TreeTemplate template, int degree) {
        super(template);
        this.degree = degree;
        digits = (int) Math.ceil(Math.log(degree) / Math.log(10));
    }

    public Topology generate(Aggregator aggregator, Set<Rule> memoryTemplate) throws IOException, TopologyFileFormatException {


        AbstractFatTree topology = new Large4LevelFatTree();

        //controller
        List<String> controllerProperties = template.getLevelNodeProperties(Switch.CONTROLLER_ID);
        if (controllerProperties == null) {
            throw new TopologyFileFormatException("Controller properties not found in the template");
        }
        topology.setControllerSwitch(new Switch.ControllerSwitch(aggregator, Integer.parseInt(controllerProperties.get(0)),memoryTemplate));

        //get k from core switches definition

        int kei = degree;
        List<Switch> coreSwitches = new ArrayList<Switch>(kei);
        final int coresNum = (kei / 2) * (kei / 2);
        for (int i = 0; i < coresNum; i++) {
            {
                //generate core switches
                final Switch aSwitch = topology.createASwitch(aggregator, template.getLevelNodeProperties(AbstractFatTree.CORE_STRING),
                        String.format("Core_%0" + digits + "d", i), memoryTemplate);
                coreSwitches.add(aSwitch);
                topology.addCoreSwitch(aSwitch);
            }
        }
        for (int i = 0; i < kei; i++) {
            {
                //generate pod switches
                List<Switch> podSwitches = new ArrayList<Switch>(kei);
                for (int j = 0; j < kei / 2; j++) {
                    final Switch aggSwitch = topology.createASwitch(aggregator, template.getLevelNodeProperties(AbstractFatTree.AGGREGATE1_STRING),
                            String.format("Agg1_%0" + digits + "d_%0" + digits + "d", i, j), memoryTemplate);
                    podSwitches.add(aggSwitch);
                    topology.addSwitch(aggSwitch);

                    //generate core to aggregate links
                    for (int t = 0; t < kei / 2; t++) {
                        topology.addLink(aggSwitch, coreSwitches.get(j * kei / 2 + t), template.getLevelLinkProperties(AbstractFatTree.CORE_STRING, AbstractFatTree.AGGREGATE1_STRING).get(TreeTemplate.LINK_CAPACITY_INDEX));
                    }
                }
                for (int j = kei / 2; j < kei; j++) {
                    final Switch agg2Switch = topology.createASwitch(aggregator, template.getLevelNodeProperties(AbstractFatTree.AGGREGATE2_STRING),
                            String.format("Agg2_%0" + digits + "d_%0" + digits + "d", i, j), memoryTemplate);
                    podSwitches.add(agg2Switch);
                    topology.addSwitch(agg2Switch);
                    //add intra-pod links
                    for (int k = 0; k < kei / 2; k++) {
                        topology.addLink(podSwitches.get(k), agg2Switch, template.getLevelLinkProperties(AbstractFatTree.AGGREGATE1_STRING, AbstractFatTree.AGGREGATE2_STRING).get(TreeTemplate.LINK_CAPACITY_INDEX));
                    }

                    // create edge switches
                    for (int k = 0; k < kei / 2; k++) {
                        final Switch edgeSwitch = topology.createASwitch(aggregator, template.getLevelNodeProperties(AbstractFatTree.EDGE_STRING),
                                String.format("Edge_%0" + digits + "d_%0" + digits + "d_%0" + digits + "d", i, j, k), memoryTemplate);
                        topology.addSwitch(edgeSwitch);
                        topology.addLink(edgeSwitch, agg2Switch, template.getLevelLinkProperties(AbstractFatTree.AGGREGATE2_STRING, AbstractFatTree.EDGE_STRING).get(TreeTemplate.LINK_CAPACITY_INDEX));
                    }
                }

            }
        }
        return topology;
    }
}
