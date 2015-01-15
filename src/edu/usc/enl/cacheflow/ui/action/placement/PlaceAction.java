package edu.usc.enl.cacheflow.ui.action.placement;

import edu.usc.enl.cacheflow.algorithms.placement.Assigner;
import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.algorithms.placement.partitionselection.MaxSizePartitionSorter;
import edu.usc.enl.cacheflow.algorithms.placement.switchselectionpartition.MinTrafficSwitchSelection;
import edu.usc.enl.cacheflow.model.Assignment;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.PartitionFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.classifier.LinearMatchTrafficProcessor;
import edu.usc.enl.cacheflow.processor.flow.classifier.TwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.algorithms.Placer;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePlacementScript;
import edu.usc.enl.cacheflow.scripts.vcrib.MultiplePostPlaceScriptCluster2;
import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.MultipleTabInputDialog;
import edu.usc.enl.cacheflow.util.Util;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 6:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class PlaceAction extends edu.usc.enl.cacheflow.ui.action.AbstractAction {


    public PlaceAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    @Override
    protected void doAction(ActionEvent e) throws Exception {
        List<String> parameters = Arrays.asList("Topology", "Partitions", "Flows");
        MultipleTabInputDialog parameterSelector = new MultipleTabInputDialog(mainWindow, parameters, mainWindow.getTabList());
        parameterSelector.setVisible(true);
        if (parameterSelector.isOkPressed()) {
            Map<String, BufferedReader> selected = parameterSelector.getSelectedReader();
            try {
                Map<String, Object> parameters2 = new HashMap<String, Object>();
                Collection<Partition> partitions =
                        new PartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>()).create(selected.get("Partitions"), parameters2, new LinkedList<Partition>());
                Topology topology =
                        new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()).create(
                                selected.get("Topology"), parameters2, new LinkedList<Topology>()).get(0);
                Collection<Flow> flows =
                        new FlowFactory(new FileFactory.EndOfFileCondition(), topology).create(selected.get("Flows"), parameters2, new LinkedList<Flow>());

                for (BufferedReader bufferedReader : selected.values()) {
                    bufferedReader.close();
                }
                final Map<Partition, Map<Rule, Collection<Flow>>> classified = new TwoLevelTrafficProcessor(
                        new LinearMatchTrafficProcessor(),
                        new LinearMatchTrafficProcessor()
                ).classify(flows, partitions);
                MinTrafficSwitchSelection switchSelection = new MinTrafficSwitchSelection();
                Map<Switch, Collection<Partition>> sourcePartitions = MultiplePostPlaceScriptCluster2.getSourcePartitions(classified);
                Map<Partition, Map<Switch, Rule>> forwardingRules = MultiplePlacementScript.createForwardingRules(partitions, topology, sourcePartitions);
                MaxSizePartitionSorter partitionSelection = new MaxSizePartitionSorter();
                Placer placer = new Assigner(switchSelection,
                        partitionSelection,
                        100, true,forwardingRules, sourcePartitions);
                parameters2.put("placement.alg", placer);
                final Assignment assignment = placer.place(topology, partitions);
                parameters2.put("placement." + placer + ".switchSelection", switchSelection);
                parameters2.put("placement." + placer + ".partitionSelection", partitionSelection);
                final StringWriter out = new StringWriter();
                PrintWriter writer = new PrintWriter(out);
                //topology.saveSwitchMemories(writer, parameters2);
                mainWindow.createTab(null, out.toString());
            } catch (NoAssignmentFoundException e1) {
                JOptionPane.showMessageDialog(mainWindow, "No solution found");
            }
        }
    }
}
