package edu.usc.enl.cacheflow.model.factory;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.model.topology.*;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.processor.topology.TopologyFileFormatException;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/8/12
 * Time: 5:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class TopologyFactory extends FileFactory<Topology> {
    private Aggregator aggregator;
    private final Set<Rule> memoryTemplate;

    public TopologyFactory(StopCondition stopCondition, Aggregator aggregator, Set<Rule> memoryTemplate) {
        super(stopCondition);
        this.aggregator = aggregator;
        this.memoryTemplate = memoryTemplate;
    }


    @Override
    protected Topology create(String s) {
        return null;
    }

    @Override
    public <C extends Collection<Topology>> C create(BufferedReader reader, Map<String, Object> parameters, C toFill) throws IOException {
        super.parseHeaderLine(reader, parameters);
        boolean nodesDefinitionFound = false;
        boolean linksDefinitionFound = false;
        Topology topology = new GeneralTopology();
        Map<String, Switch> switches = new HashMap<String, Switch>();

        Switch.ControllerSwitch controllerSwitch = null;
        int fatTreeCores = 0;
        int fatTreeCoresTillNow = 0;
        while (reader.ready()) {
            String line = reader.readLine();
            if (line.toLowerCase().startsWith("fattree")) {
                final String[] split = line.split(",|\\s");
                fatTreeCores = Integer.parseInt(split[1]);
                topology = new FatTree();
            } else if (line.toLowerCase().startsWith("largefattree")) {
                final String[] split = line.split(",|\\s");
                fatTreeCores = Integer.parseInt(split[1]);
                topology = new Large2LevelFatTree();
            } else if (line.toLowerCase().startsWith("large4fattree")) {
                final String[] split = line.split(",|\\s");
                fatTreeCores = Integer.parseInt(split[1]);
                topology = new Large4LevelFatTree();
            }
            if (line.toLowerCase().startsWith("#nodes")) {
                //found nodes definition
                nodesDefinitionFound = true;
            } else if (line.toLowerCase().startsWith("#linkes")) {
                linksDefinitionFound = true;
                nodesDefinitionFound = false;
            } else if (nodesDefinitionFound) {
                //handle node definition section
                String[] s = line.split("(,|\\s)+");

                if (s[0].trim().equals(Switch.CONTROLLER_ID)) {
                    controllerSwitch = new Switch.ControllerSwitch(
                            aggregator, s.length > 1 ? Integer.parseInt(s[1]) : 0, memoryTemplate);
                    topology.setControllerSwitch(controllerSwitch);
                } else {
                    if (controllerSwitch == null) {
                        throw new TopologyFileFormatException("undefined controller node");
                    }
                    Switch aSwitch = topology.createASwitch(aggregator, Arrays.asList(s).subList(1, s.length), s[0], memoryTemplate);
                    /*if (s[1].equals("OVS")) {
                        aSwitch = new OVSSwitch(s[0], controllerSwitch.getForwardToControllerRule(), aggregator, Integer.parseInt(s[2]));
                    } else if (s[1].equals("TCAMSRAM")) {
                        aSwitch = new TCAMSRAMSwitch(s[0], controllerSwitch.getForwardToControllerRule(), aggregator, Integer.parseInt(s[2]), Integer.parseInt(s[3]));
                    } else {
                        aSwitch = new MemorySwitch(s[0], controllerSwitch.getForwardToControllerRule(), aggregator, Integer.parseInt(s[2]));
                    }*/

                    switches.put(s[0], aSwitch);
                    if (fatTreeCoresTillNow < fatTreeCores) {
                        ((AbstractFatTree) topology).addCoreSwitch(aSwitch);
                        fatTreeCoresTillNow++;
                    } else {
                        topology.addSwitch(aSwitch);
                    }
                }

            } else if (linksDefinitionFound) {
                String[] s = line.split(",|\\s+");
                Switch s1 = switches.get(s[0]);
                Switch s2 = switches.get(s[1]);
                if (s1 == null || s2 == null) {
                    throw new TopologyFileFormatException("Undefined switch id for link " + s[0] + " - " + s[1]);
                } else {
                    topology.addLink(s1, s2, Long.parseLong(s[2]));
                }
            }
        }
        topology.computeShortestPaths();
        toFill.add(topology);
        return toFill;
    }
}
