package edu.usc.enl.cacheflow.ui.topology;

import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.topology.Link;
import edu.usc.enl.cacheflow.ui.WorkingPanel;

import javax.swing.*;
import java.awt.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/13/11
 * Time: 2:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class LinkPropertiesWindow extends JFrame {
    private Link link;

    public LinkPropertiesWindow(Link link)  {
        super(link.toString());
        this.link = link;

        try {
            createGUI();
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            e.printStackTrace();
        }
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        pack();
    }

    private void createGUI() throws Exception {
         JPanel mainPanel = new JPanel(new BorderLayout());
        this.getContentPane().add(mainPanel);

        JTabbedPane tabbedPane = new JTabbedPane();
        mainPanel.add(tabbedPane, BorderLayout.CENTER);

        WorkingPanel statPanel = new WorkingPanel(tabbedPane, 0);
        tabbedPane.add("Statistics", statPanel);
        statPanel.setText(link.getStats().toString());
        statPanel.setEditable(false);

        WorkingPanel flowPanel =new WorkingPanel(tabbedPane,1);
        tabbedPane.add("Flows",flowPanel);
        flowPanel.setEditable(false);
        flowPanel.setText(WriterSerializableUtil.getString(link.getFlows(),null));
    }

}
