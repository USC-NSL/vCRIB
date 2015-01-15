package edu.usc.enl.cacheflow.ui;

import edu.usc.enl.cacheflow.util.Util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.util.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/6/11
 * Time: 10:03 AM
 * To change this template use File | Settings | File Templates.
 */
public class MultipleTabInputDialog extends JDialog {
    private boolean okPressed = false;
    private Map<String, JComboBox> params = new HashMap<String, JComboBox>();

    public MultipleTabInputDialog(final JFrame frame, List<String> inputLabels, List<WorkingPanel> openTabs) {
        super(frame, true);
        JPanel mainPanel = new JPanel(new BorderLayout());
        setContentPane(mainPanel);

        JLabel instrucitonLabel = new JLabel("Select a tab or a file for each parameter:");
        mainPanel.add(instrucitonLabel, BorderLayout.PAGE_START);

        {
            GridBagConstraints c = new GridBagConstraints();
            c.gridy = 0;
            c.fill = GridBagConstraints.HORIZONTAL;
            c.anchor = GridBagConstraints.FIRST_LINE_START;
            c.insets = new Insets(5, 5, 0, 0);
            //JPanel paramsPanel = new JPanel(new GridLayout(inputLabels.size(), 2, 10, 10));
            JPanel paramsPanel = new JPanel(new GridBagLayout());
            mainPanel.add(paramsPanel, BorderLayout.CENTER);
            for (String inputLabel : inputLabels) {
                c.weightx = 0;
                paramsPanel.add(new JLabel(inputLabel), c);
                JComboBox tabSelector = createTabSelector(openTabs);
                params.put(inputLabel, tabSelector);
                c.weightx = 1;
                paramsPanel.add(tabSelector, c);
                c.gridy++;
            }
        }

        Box h = Box.createHorizontalBox();
        mainPanel.add(h, BorderLayout.PAGE_END);
        h.add(Box.createHorizontalGlue());
        JButton okBtn = new JButton("OK");
        okBtn.setDefaultCapable(true);
        h.add(okBtn);
        h.add(Box.createHorizontalStrut(10));
        JButton cancelBtn = new JButton("Cancel");
        h.add(cancelBtn);
        okBtn.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                //validate
                for (Map.Entry<String, JComboBox> stringJComboBoxEntry : params.entrySet()) {
                    JComboBox selector = stringJComboBoxEntry.getValue();
                    Object selectedItem = selector.getSelectedItem();
                    if (selectedItem==null ||
                            (selectedItem instanceof ComboBoxChooseFileItem && ((ComboBoxChooseFileItem) selectedItem).file==null)){
                        JOptionPane.showMessageDialog(frame,"One input field is empty", "Error", JOptionPane.ERROR_MESSAGE);
                        return;
                    }
                }
                okPressed = true;
                dispose();
            }
        });
        cancelBtn.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                dispose();
            }
        });

        setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
        pack();
    }

    public boolean isOkPressed() {
        return okPressed;
    }

    public Map<String, LinkedList<String>> getSelected() throws IOException {
        Map<String, LinkedList<String>> output = new HashMap<String, LinkedList<String>>();
        for (Map.Entry<String, JComboBox> stringJComboBoxEntry : params.entrySet()) {
            JComboBox selector = stringJComboBoxEntry.getValue();
            Object selectedItem = selector.getSelectedItem();
            if (selectedItem instanceof ComboBoxChooseFileItem) {
                //open file
                output.put(stringJComboBoxEntry.getKey(), Util.loadFile(((ComboBoxChooseFileItem) selectedItem).file));
            } else {
                output.put(stringJComboBoxEntry.getKey(), new LinkedList<String>(Arrays.asList(((WorkingPanel) selectedItem).getData2())));
            }
        }
        return output;
    }

    public Map<String, BufferedReader> getSelectedReader() throws IOException {
        Map<String, BufferedReader> output = new HashMap<String, BufferedReader>();
        for (Map.Entry<String, JComboBox> stringJComboBoxEntry : params.entrySet()) {
            JComboBox selector = stringJComboBoxEntry.getValue();
            Object selectedItem = selector.getSelectedItem();
            if (selectedItem instanceof ComboBoxChooseFileItem) {
                //open file
                output.put(stringJComboBoxEntry.getKey(), new BufferedReader(new FileReader(((ComboBoxChooseFileItem) selectedItem).file)));
            } else {
                output.put(stringJComboBoxEntry.getKey(), new BufferedReader(new StringReader(((WorkingPanel) selectedItem).getData())));
            }
        }
        return output;
    }



    private JComboBox createTabSelector(List<WorkingPanel> openTabs) {
        final JComboBox selector = new JComboBox(openTabs.toArray());
        selector.setEditable(false);
        selector.addItem(new ComboBoxChooseFileItem());

        selector.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                Object selectedItem = selector.getSelectedItem();
                if (selectedItem != null) {
                    if (selectedItem instanceof ComboBoxChooseFileItem) {
                        //open a file open dialog
                        File file = MainWindow.openAFile((JFrame) getOwner());
                        if (file != null) {
                            ((ComboBoxChooseFileItem) selectedItem).file = file;
                        } else {
                            selector.setSelectedIndex(0);
                        }
                    }
                }
            }
        });

        return selector;
    }

    private class ComboBoxChooseFileItem {
        final String chooseAFile = "Choose a File";
        File file;

        @Override
        public String toString() {
            if (file == null) {
                return chooseAFile;
            } else {
                return chooseAFile + "(" + file.getName() + ")";
            }
        }
    }
}
