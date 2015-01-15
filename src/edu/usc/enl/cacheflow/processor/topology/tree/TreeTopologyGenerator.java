package edu.usc.enl.cacheflow.processor.topology.tree;

import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.GeneralTopology;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.processor.topology.TopologyFileFormatException;

import java.io.IOException;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/5/11
 * Time: 3:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class TreeTopologyGenerator extends AbstractTreeTopologyGenerator {


    public TreeTopologyGenerator(TreeTemplate template) {
        super(template);
    }

    public Topology generate(Aggregator aggregator, Set<Rule> memoryTemplate) throws IOException, TopologyFileFormatException {
        Topology topology = new GeneralTopology();
        addController(aggregator, topology, memoryTemplate);


        //generate switches
        double log2 = Math.log(2);

        int id = 1;
        List<Switch> switches = new LinkedList<Switch>();
        Map<Switch, String> switchLevelName = new HashMap<Switch, String>();
        List<String> levelsName = template.getLevelsName();
        int levels = levelsName.size() - 1;
        for (int i = 1; i < Math.pow(2, levels); i++) {
            int level = ((int) (Math.log(i) / log2) + 1);
            String levelName = levelsName.get(level);
            List<String> levelProperties = template.getLevelNodeProperties(levelName);
            String idS = "" + id;
            if (idS.equals(Switch.CONTROLLER_ID)) {
                idS = "" + (++id);
            }
            Switch aSwitch = topology.createASwitch(aggregator, levelProperties, idS, memoryTemplate);
            topology.addSwitch(aSwitch);
            switches.add(aSwitch);
            switchLevelName.put(aSwitch, levelName);
            id++;
        }

        //generate links
        //each node has two children

        for (int i = 1; i < Math.pow(2, levels - 1); i++) {
            Switch parent = switches.get(i - 1);
            Switch child1 = switches.get(2 * i - 1);
            List<Long> parentChild1LinkProperties = template.getLevelLinkProperties(switchLevelName.get(parent), switchLevelName.get(child1));
            topology.addLink(parent, child1, parentChild1LinkProperties.get(TreeTemplate.LINK_CAPACITY_INDEX));
            Switch child2 = switches.get(2 * i);
            List<Long> parentChild2LinkProperties = template.getLevelLinkProperties(switchLevelName.get(parent), switchLevelName.get(child1));
            topology.addLink(parent, child2, parentChild2LinkProperties.get(TreeTemplate.LINK_CAPACITY_INDEX));
        }

        return topology;
    }


}
