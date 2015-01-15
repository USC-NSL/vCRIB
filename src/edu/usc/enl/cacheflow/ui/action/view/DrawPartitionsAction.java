package edu.usc.enl.cacheflow.ui.action.view;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.WorkingPanel;
import edu.usc.enl.cacheflow.ui.action.NeedTabAction;
import edu.usc.enl.cacheflow.ui.rulerender.PartitionCanvas;
import edu.usc.enl.cacheflow.ui.rulerender.RenderWindow;
import edu.usc.enl.cacheflow.util.Util;

import java.awt.event.ActionEvent;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 5:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrawPartitionsAction extends NeedTabAction {
    public DrawPartitionsAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    @Override
    protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {

        List<Partition> partitions = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>()).create(
                new BufferedReader(new StringReader(selectedTab.getData())),new HashMap<String, Object>(), new LinkedList<Partition>());

        //List<String> names = new ArrayList<String>(partitions.get(0).getProperties().keySet());
        PartitionCanvas canvas = new PartitionCanvas(mainWindow, RenderWindow.PREFERREDSIZE.width, RenderWindow.PREFERREDSIZE.height,
                Util.getDimensionInfos(), partitions);
        RenderWindow renderWindow = new RenderWindow(selectedTab.getTitle(), canvas);
        mainWindow.addSubWindow(renderWindow);
        renderWindow.setVisible(true);
    }
}
