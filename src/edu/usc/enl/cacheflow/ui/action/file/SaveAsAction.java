package edu.usc.enl.cacheflow.ui.action.file;

import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.WorkingPanel;
import edu.usc.enl.cacheflow.ui.action.NeedTabAction;
import edu.usc.enl.cacheflow.ui.exception.FileException;
import edu.usc.enl.cacheflow.util.Util;

import javax.swing.*;
import javax.swing.filechooser.FileFilter;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.event.ActionEvent;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 1:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class SaveAsAction extends NeedTabAction {
    public SaveAsAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    @Override
    protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws FileException {
        JFileChooser fileChooser = new JFileChooser(Util.getCurrentDirectory());
        FileFilter filter = new FileNameExtensionFilter("Text File (*.txt)", "txt");
        fileChooser.setFileFilter(filter);
        int returnVal = fileChooser.showOpenDialog(mainWindow);
        if (returnVal == JFileChooser.APPROVE_OPTION) {
            selectedTab.saveFile(fileChooser.getSelectedFile());
        }
    }
}
