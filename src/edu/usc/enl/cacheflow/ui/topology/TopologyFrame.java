package edu.usc.enl.cacheflow.ui.topology;

import edu.usc.enl.cacheflow.model.topology.Topology;

import javax.swing.*;
import java.awt.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/8/11
 * Time: 10:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class TopologyFrame extends JFrame {
    private final JFrame thisFrame;

    public TopologyFrame(Topology topology) throws HeadlessException {
        thisFrame = this;

        JPanel mainPanel = new JPanel(new BorderLayout());
        this.setContentPane(mainPanel);
        mainPanel.add(topology.draw(new Dimension(300, 300)));


        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        pack();
    }
}
