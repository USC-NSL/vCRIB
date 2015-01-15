package edu.usc.enl.cacheflow.ui.action.rule;

import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.processor.rule.generator.RandomRuleGenerator;
import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.WorkingPanel;
import edu.usc.enl.cacheflow.ui.action.NeedTabAction;
import edu.usc.enl.cacheflow.ui.exception.RuleInputException;
import edu.usc.enl.cacheflow.util.Util;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 1:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class RandomRuleAction extends NeedTabAction {
    public RandomRuleAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    @Override
    protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
        //show options dialog
        String input = JOptionPane.showInputDialog(mainWindow, "Note that you need to describe dimensions before. Enter number of Rules", "5");
        if (input != null) {
            try {
                int numberOfRules = Integer.parseInt(input);

                String[] infosDefinition = selectedTab.getData2();
                if (infosDefinition.length < 3) {
                    throw new RuleInputException("Incorrect format");
                }
                final RuleFactory ruleFactory = new RuleFactory(new FileFactory.EndOfFileCondition());
                HashMap<String, Object> parameters = new HashMap<String, Object>();
                ruleFactory.parseHeaderLine(new BufferedReader(new StringReader(selectedTab.getData())), parameters);
                List<DimensionInfo> infos = ruleFactory.getDimensionInfos();
                mainWindow.createTab(null, WriterSerializableUtil.getString(new RandomRuleGenerator(Util.random, infos, numberOfRules).generateRules(), parameters));

            } catch (NumberFormatException e1) {
                JOptionPane.showMessageDialog(mainWindow, "Not a Number", "Error", JOptionPane.ERROR_MESSAGE);
            } catch (RuleInputException e1) {
                JOptionPane.showMessageDialog(mainWindow, e1.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
                e1.printStackTrace();
            }
        }
    }
}
