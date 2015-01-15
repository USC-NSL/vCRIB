package edu.usc.enl.cacheflow.ui.action;

import edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.partitioner.MinNewRulePartitioner;
import edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.partitioner.Partitioner;
import edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.partitioner.RangeNumBalancedPartitioner;
import edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.partitioner.RuleNumBalancedPartitioner;
import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.partition.PartitionProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.processor.rule.aggregator.IntegratedSemiGridAndMergeProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.MinimumAggregator;
import edu.usc.enl.cacheflow.processor.rule.aggregator.patch.PatchMergeProcessor;
import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.WorkingPanel;
import edu.usc.enl.cacheflow.util.Util;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 1:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class CreatePartitionsAction extends NeedTabAction {

    public CreatePartitionsAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    @Override
    protected void doAction(ActionEvent e, WorkingPanel selectedTab) throws Exception {
        try {
            String input = JOptionPane.showInputDialog(mainWindow, "Note that you need to describe dimensions before. Enter number of Rules", "5");
            if (input != null) {
                int capacityOfPartition = Integer.parseInt(input);
                //if (rules.size() > 0) {
                //select the algorithm
                Partitioner[] partitioners = new Partitioner[]{new RangeNumBalancedPartitioner(), new RuleNumBalancedPartitioner(), new MinNewRulePartitioner()};
                Object partitioner = JOptionPane.showInputDialog(mainWindow, "Select partitioning algorihtm", "PartiitonSelection", JOptionPane.QUESTION_MESSAGE, null, partitioners, partitioners[0]);
                if (partitioner != null) {
                    //run
                    List<Aggregator> processors = new LinkedList<Aggregator>();
                    processors.add(new Aggregator(Util.EMPTY_LIST));
                    processors.add(new PatchMergeProcessor(new IntegratedSemiGridAndMergeProcessor(Util.EMPTY_LIST)));

                    Aggregator aggregator = new MinimumAggregator(Util.EMPTY_LIST, processors);
                    Map<String, Object> parameters = new HashMap<String, Object>();
                    final List<Rule> rules = new RuleFactory(new FileFactory.EndOfFileCondition()).create(
                            new BufferedReader(new StringReader(selectedTab.getData())), parameters, new LinkedList<Rule>());
                    parameters.put("partition.alg", partitioner);
                    mainWindow.createTab(null, WriterSerializableUtil.getString(new PartitionProcessor(
                            (Partitioner) partitioner, capacityOfPartition, aggregator).process(rules), parameters));
                }
                // }
            }
        } catch (NumberFormatException e1) {
            JOptionPane.showMessageDialog(mainWindow, "Not a Number", "Error", JOptionPane.ERROR_MESSAGE);
        } catch (Exception e1) {
            JOptionPane.showMessageDialog(mainWindow, "Error in parsing rules: " + e1.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            e1.printStackTrace();
        }
    }
}
