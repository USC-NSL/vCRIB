package edu.usc.enl.cacheflow.ui.action.view;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.WorkingPanel;
import edu.usc.enl.cacheflow.ui.action.NeedTabAction;
import edu.usc.enl.cacheflow.ui.rulerender.RenderWindow;
import edu.usc.enl.cacheflow.ui.rulerender.RuleCanvas;
import edu.usc.enl.cacheflow.util.Util;

import java.awt.event.ActionEvent;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 5:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrawRulesAction extends NeedTabAction {
    public DrawRulesAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    @Override
    protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
        final List<Rule> rules = new RuleFactory(new FileFactory.EndOfFileCondition()).create(new BufferedReader(
                new StringReader(selectedTab.getData())),new HashMap<String, Object>(), new LinkedList<Rule>());

        //List<String> names = new ArrayList<String>(rules.get(0).getProperties().keySet());
        RuleCanvas canvas = new RuleCanvas(mainWindow, RenderWindow.PREFERREDSIZE.width, RenderWindow.PREFERREDSIZE.height,
               Util.getDimensionInfos(), rules);


        RenderWindow renderWindow = new RenderWindow(selectedTab.getTitle(), canvas);
        mainWindow.addSubWindow(renderWindow);
        renderWindow.setVisible(true);
    }
}
