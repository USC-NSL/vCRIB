package edu.usc.enl.cacheflow.ui.action.view;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.WorkingPanel;
import edu.usc.enl.cacheflow.ui.action.NeedTabAction;
import edu.usc.enl.cacheflow.ui.topology.TopologyFrame;
import edu.usc.enl.cacheflow.util.Util;

import java.awt.event.ActionEvent;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 5:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrawTopologyAction extends NeedTabAction {
    public DrawTopologyAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    @Override
    protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
        Topology topology =
                new TopologyFactory(new FileFactory.EndOfFileCondition(),Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()).create(
                        new BufferedReader(new StringReader(selectedTab.getData())),new HashMap<String, Object>(), new LinkedList<Topology>()).get(0);
        TopologyFrame topologyFrame = new TopologyFrame(topology);
        mainWindow.addSubWindow(topologyFrame);
        topologyFrame.setVisible(true);
    }


}
