package edu.usc.enl.cacheflow.ui.action.file;

import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.WorkingPanel;
import edu.usc.enl.cacheflow.ui.action.AbstractAction;
import edu.usc.enl.cacheflow.util.Util;

import java.awt.event.ActionEvent;
import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 1:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class OpenAction extends AbstractAction {
    public OpenAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    protected void doAction(ActionEvent e) throws Exception {
        File selectedFile = MainWindow.openAFile(mainWindow);
        if (selectedFile != null) {
            WorkingPanel tab = mainWindow.createTab(selectedFile.getName(), Util.readFile(selectedFile));
            tab.setFile(selectedFile);
        }
    }
}
