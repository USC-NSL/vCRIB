package edu.usc.enl.cacheflow.ui.action.statistics;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.network.RunFlowsOnNetworkProcessor2;
import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.MultipleTabInputDialog;
import edu.usc.enl.cacheflow.ui.action.AbstractAction;
import edu.usc.enl.cacheflow.util.Util;

import java.awt.event.ActionEvent;
import java.io.BufferedReader;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/11/11
 * Time: 6:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class PlacementStatAction extends AbstractAction {
    public PlacementStatAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    @Override
    protected void doAction(ActionEvent e) throws Exception {
        List<String> parameters = Arrays.asList("Topology", "Switch Memories", "Flows");
        MultipleTabInputDialog parameterSelector = new MultipleTabInputDialog(mainWindow, parameters, mainWindow.getTabList());
        parameterSelector.setVisible(true);
        if (parameterSelector.isOkPressed()) {
            Map<String, BufferedReader> selected = parameterSelector.getSelectedReader();
            HashMap<String, Object> parameters2 = new HashMap<String, Object>();
            Topology topology =
                    new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()).create(
                            selected.get("Topology"), parameters2, new LinkedList<Topology>()).get(0);
            Collection<Flow> flows =
                    new FlowFactory(new FileFactory.EndOfFileCondition(), topology).create(selected.get("Flows"),parameters2, new LinkedList<Flow>());
            topology.loadSwitchesMemory(selected.get("Switch Memories"));
            for (BufferedReader bufferedReader : selected.values()) {
                bufferedReader.close();
            }
            new RunFlowsOnNetworkProcessor2().process(topology, flows);
            mainWindow.createTab(null, topology.getStat(parameters2).toString());

        }
    }

}
