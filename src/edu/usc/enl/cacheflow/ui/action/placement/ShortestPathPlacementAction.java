package edu.usc.enl.cacheflow.ui.action.placement;

import edu.usc.enl.cacheflow.ui.MainWindow;
import edu.usc.enl.cacheflow.ui.MultipleTabInputDialog;

import java.awt.event.ActionEvent;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/21/11
 * Time: 12:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class ShortestPathPlacementAction extends edu.usc.enl.cacheflow.ui.action.AbstractAction {


    public ShortestPathPlacementAction(MainWindow mainWindow) {
        super(mainWindow);
    }

    @Override
    protected void doAction(ActionEvent e) throws Exception {
        List<String> parameters = Arrays.asList("Topology", "Rules", "Flows");
        MultipleTabInputDialog parameterSelector = new MultipleTabInputDialog(mainWindow, parameters, mainWindow.getTabList());
        parameterSelector.setVisible(true);
       /* if (parameterSelector.isOkPressed()) {

            Map<String, BufferedReader> selected = parameterSelector.getSelectedReader();
            Map<String, Object> parameters2 = new HashMap<String, Object>();
            List<Rule> rules =
                    new RuleFactory(new FileFactory.EndOfFileCondition()).create(selected.get("Rules"), parameters2, new LinkedList<Rule>());
            Topology topology =
                    new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()).create(selected.get("Topology"), parameters2, new LinkedList<Topology>()).get(0);
            List<Flow> flows =
                    new FlowFactory(new FileFactory.EndOfFileCondition(), topology).create(selected.get("Flows"), parameters2, new LinkedList<Flow>());
            for (BufferedReader bufferedReader : selected.values()) {
                bufferedReader.close();
            }
            MinimalRuleLoadProcessor minimalRuleLoadProcessor = new MinimalRuleLoadProcessor();
            parameters2.put("placement.alg", minimalRuleLoadProcessor);
            minimalRuleLoadProcessor.process(topology, new OVSClassifier().classify(flows, rules));

            new RunFlowsOnNetworkProcessor2().process(topology, flows);
            StringWriter out = new StringWriter();
            PrintWriter writer = new PrintWriter(out);
            topology.saveSwitchMemories(writer, parameters2);
            mainWindow.createTab(null, out.toString());
        }*/
    }
}
