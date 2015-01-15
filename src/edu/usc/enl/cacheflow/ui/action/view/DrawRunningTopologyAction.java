package edu.usc.enl.cacheflow.ui.action.view;

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
import edu.usc.enl.cacheflow.ui.topology.TopologyFrame;
import edu.usc.enl.cacheflow.util.Util;

import java.awt.event.ActionEvent;
import java.io.BufferedReader;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/13/11
 * Time: 12:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class DrawRunningTopologyAction extends AbstractAction {
    public DrawRunningTopologyAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    @Override
    protected void doAction(ActionEvent e) throws Exception {
        List<String> parameters = Arrays.asList("Topology", "Switch Memories", "Flows");
        MultipleTabInputDialog parameterSelector = new MultipleTabInputDialog(mainWindow, parameters, mainWindow.getTabList());
        parameterSelector.setVisible(true);
        if (parameterSelector.isOkPressed()) {
            Map<String, BufferedReader> selected = parameterSelector.getSelectedReader();
            Topology topology =
                    new TopologyFactory(new FileFactory.EndOfFileCondition(),Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()).create(
                            selected.get("Topology"),new HashMap<String, Object>(), new LinkedList<Topology>()).get(0);

            List<Flow> flows =
                    new FlowFactory(new FileFactory.EndOfFileCondition(),topology).create(selected.get("Flows"),new HashMap<String, Object>(), new LinkedList<Flow>());
            topology.loadSwitchesMemory(new BufferedReader(selected.get("Switch Memories")));
            new RunFlowsOnNetworkProcessor2().process(topology,flows);
            for (BufferedReader bufferedReader : selected.values()) {
                bufferedReader.close();
            }

            TopologyFrame topologyFrame = new TopologyFrame(topology);
            mainWindow.addSubWindow(topologyFrame);
            topologyFrame.setVisible(true);
        }
    }
}
