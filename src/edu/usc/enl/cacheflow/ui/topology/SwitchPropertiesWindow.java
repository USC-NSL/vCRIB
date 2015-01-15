package edu.usc.enl.cacheflow.ui.topology;

import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.ui.WorkingPanel;
import edu.usc.enl.cacheflow.ui.rulerender.RenderWindow;
import edu.usc.enl.cacheflow.ui.rulerender.RuleCanvas;
import edu.usc.enl.cacheflow.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/12/11
 * Time: 11:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class SwitchPropertiesWindow extends JFrame {
    private Switch aSwitch;
    private JFrame thisFrame;

    public SwitchPropertiesWindow(Switch aSwitch) {
        super(aSwitch.toString() + " Properties");
        this.aSwitch = aSwitch;
        thisFrame = this;

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

        final WorkingPanel memory = new WorkingPanel(tabbedPane, 0);
        tabbedPane.add("Memory", memory);
        memory.setEditable(false);
        final HashSet<Rule> toFill = new HashSet<>();
        aSwitch.getState().getRules(toFill);
        memory.setText(WriterSerializableUtil.getString(toFill, null));

        WorkingPanel statPanel = new WorkingPanel(tabbedPane, 1);
        tabbedPane.add("Statistics", statPanel);
        statPanel.setEditable(false);
        statPanel.setText(aSwitch.getStats().toString());

        WorkingPanel flowPanel = new WorkingPanel(tabbedPane, 2);
        tabbedPane.add("Flows", flowPanel);
        flowPanel.setEditable(false);
        flowPanel.setText(WriterSerializableUtil.getString(aSwitch.getInputFlows(), null));

        //menu
        JMenuBar menuBar = new JMenuBar();
        mainPanel.add(menuBar, BorderLayout.PAGE_START);

        JMenuItem drawMemory = new JMenuItem("Draw Memory");
        menuBar.add(drawMemory);
        drawMemory.setMnemonic(KeyEvent.VK_D);
        drawMemory.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                try {
                    final java.util.List<Rule> rules = new RuleFactory(new FileFactory.EndOfFileCondition()).create(
                            new BufferedReader(new StringReader(memory.getData())), new HashMap<String, Object>(), new LinkedList<Rule>());
                    RuleCanvas canvas = new RuleCanvas(thisFrame, RenderWindow.PREFERREDSIZE.width, RenderWindow.PREFERREDSIZE.height,
                            Util.getDimensionInfos(), rules);


                    RenderWindow renderWindow = new RenderWindow("Memory of " + aSwitch, canvas);
                    renderWindow.setVisible(true);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        });

    }
}
