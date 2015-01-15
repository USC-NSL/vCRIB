package edu.usc.enl.cacheflow.ui.action.flow;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.topology.TopologyFileFormatException;
import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.MultipleTabInputDialog;
import edu.usc.enl.cacheflow.util.Util;

import javax.swing.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/27/11
 * Time: 10:25 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class ClassBenchFlowGenerationAction extends edu.usc.enl.cacheflow.ui.action.AbstractAction {

    public Map<String,Object> parameters2;

    protected ClassBenchFlowGenerationAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    protected Map<String, Object> getFlowGenerationRequiremetns() {

        //get parameters using parameter window
        List<String> parameters = Arrays.asList("Topology", "Classbench Flows", "Flow Distribution");
        MultipleTabInputDialog parameterSelector = new MultipleTabInputDialog(mainWindow, parameters, mainWindow.getTabList());
        parameterSelector.setVisible(true);
        if (parameterSelector.isOkPressed()) {
            try {
                Map<String, Object> requirements = new HashMap<String, Object>();
                Map<String, BufferedReader> selected = parameterSelector.getSelectedReader();
                parameters2 = new HashMap<String, Object>();
                Topology topology = new TopologyFactory(new FileFactory.EndOfFileCondition(),Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()).create(
                        selected.get("Topology"), parameters2, new LinkedList<Topology>()).get(0);
                for (BufferedReader bufferedReader : selected.values()) {
                    bufferedReader.close();
                }
                Map<String, LinkedList<String>> selected2 = parameterSelector.getSelected();
                CustomRandomFlowDistribution flowDistribution = new CustomRandomFlowDistribution(selected2.get("Flow Distribution"));
                List<String> classbenchFlows = selected2.get("Classbench Flows");
                requirements.put("Topology", topology);
                requirements.put("Flow Distribution", flowDistribution);
                requirements.put("Classbench Flows", classbenchFlows);
                return requirements;
            } catch (IOException e1) {
                JOptionPane.showMessageDialog(mainWindow, "Error in loading a selected file: " + e1.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
                e1.printStackTrace();
                return null;
            } catch (TopologyFileFormatException e1) {
                e1.printStackTrace();
                return null;
            }
        } else {
            return null;
        }
    }
}