package edu.usc.enl.cacheflow.ui.rulerender;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/29/11
 * Time: 12:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class DimensionSelectionDialog extends JDialog {

    public static final int OK_BUTTON = 0;
    public static final int CANCEL_BUTTON = 1;

    private JComboBox box1;
    private JComboBox box2;
    private List<DimensionInfo> choices;
    private int returnCode = CANCEL_BUTTON;

    public DimensionSelectionDialog(JFrame parent, List<DimensionInfo> choices) {
        super(parent, "Select dimension", true);
        this.choices = choices;
        createGUIDialog();
        pack();
        setResizable(false);
    }


    private void createGUIDialog() {
        JPanel mainPanel = new JPanel(new BorderLayout());
        setContentPane(mainPanel);
        mainPanel.add(new JLabel("Select dimensions to rulerender: "), BorderLayout.PAGE_START);

        JPanel combosPanel = new JPanel(new GridLayout(2, 2, 10, 10));
        mainPanel.add(combosPanel, BorderLayout.CENTER);

        combosPanel.add(new JLabel("Dimension 1:"));
        box1 = new JComboBox(choices.toArray());
        box1.setEditable(false);
        box1.setSelectedIndex(0);
        combosPanel.add(box1);

        combosPanel.add(new JLabel("Dimension 2:"));
        box2 = new JComboBox(choices.toArray());
        box2.setEditable(false);
        box2.setSelectedIndex(Math.min(choices.size(), 1));
        combosPanel.add(box2);

        Box h = Box.createHorizontalBox();
        mainPanel.add(h, BorderLayout.PAGE_END);
        h.add(Box.createHorizontalGlue());
        JButton okButton = new JButton("OK");
        h.add(okButton);
        okButton.setDefaultCapable(true);

        h.add(Box.createHorizontalStrut(10));

        JButton cancelButton = new JButton("Cancel");
        h.add(cancelButton);

        okButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                returnCode = OK_BUTTON;
                dispose();
            }
        });

        cancelButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                returnCode = CANCEL_BUTTON;
                dispose();
            }
        });

    }

    public int getReturnCode() {
        return returnCode;
    }

    public DimensionInfo getDimension1() {
        return (DimensionInfo) box1.getSelectedItem();
    }

    public DimensionInfo getDimension2() {
        return (DimensionInfo) box2.getSelectedItem();
    }
}
