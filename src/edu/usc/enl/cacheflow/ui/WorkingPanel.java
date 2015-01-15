package edu.usc.enl.cacheflow.ui;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.ui.exception.FileException;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/28/11
 * Time: 11:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class WorkingPanel extends JPanel {
    private File file;
    private JTextArea dataText;
    private JTabbedPane container;
    private int index;
    /*private TYPES type;

    public static enum TYPES {
        RULE, PARTITION
    }*/

    public WorkingPanel( JTabbedPane container, int index) {
        this.index = index;
        this.container = container;
//        this.type=type;
        dataText = new JTextArea(10, 80);
        createGUI();
    }

//    public TYPES getType() {
//        return type;
//    }

    public void setFile(File file) throws FileException {
        this.file = file;
        setTitle(file.getName());
    }

    private void createGUI() {
        //setLayout(new GridLayout(2, 1));
        setLayout(new GridLayout(1, 1));
        JScrollPane dataPane = new JScrollPane(dataText);
        this.add(dataPane);
        //add(dataPane);
    }

    public void saveFile(File file) throws FileException {
        setFile(file);
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            writer.write(dataText.getText());
            writer.close();
        } catch (IOException e) {
            throw new FileException(e.getMessage(), e, file);
        }
    }

    public File getFile() {
        return file;
    }

    public void reSaveFile() throws FileException {
        if (file != null) {
            saveFile(file);
        }
    }

    public String getData() {
        return dataText.getText();
    }

    public String[] getData2() {
        return dataText.getText().split("\n");
    }

    public LinkedList<String> getData3() {
        LinkedList<String> list = new LinkedList<String>();
        for (String s : getData2()) {
            list.add(s);
        }
        return list;
    }

    public List<Rule> getRules (HashMap<String, Object> parameters) throws Exception {
        return new RuleFactory(new FileFactory.EndOfFileCondition()).create(new BufferedReader(
                new StringReader(getData())),parameters, new LinkedList<Rule>());
    }

    @Override
    public String toString() {
        return getTitle();
    }

    public void setIndex(int i) {
        index = i;
    }

    public int getIndex() {
        return index;
    }

    public String getTitle(){
        return container.getTitleAt(index);
    }

    public void setTitle(String title){
        container.setTitleAt(index,title);
    }

    public void setText(String text) {
        dataText.setText(text);
    }

    public void setEditable(boolean b) {
        dataText.setEditable(b);
    }
}
