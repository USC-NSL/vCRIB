package edu.usc.enl.cacheflow.ui.action;

import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.WorkingPanel;

import java.awt.event.ActionEvent;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 1:10 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class NeedTabAction extends AbstractAction {
    protected NeedTabAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    public void doAction(ActionEvent e) throws Exception {
        WorkingPanel selectedTab = mainWindow.getSelectedTab2();
        if (selectedTab != null) {
            doAction(e, selectedTab);
        }
    }

    protected abstract void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception;
}
