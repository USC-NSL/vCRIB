package edu.usc.enl.cacheflow.ui.action;

import edu.usc.enl.cacheflow.ui.MainWindow;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 1:09 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractAction implements ActionListener {
    protected MainWindow mainWindow;

    protected AbstractAction(MainWindow mainWindow) {
        this.mainWindow = mainWindow;
    }

    public void actionPerformed(ActionEvent e) {
        try {
            doAction(e);
        } catch (Exception e1) {
            JOptionPane.showMessageDialog(mainWindow, e1.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            e1.printStackTrace();
        }
    }

    protected abstract void doAction(ActionEvent e) throws Exception;

}
