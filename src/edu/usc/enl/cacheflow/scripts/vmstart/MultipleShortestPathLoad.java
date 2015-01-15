package edu.usc.enl.cacheflow.scripts.vmstart;

import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/7/12
 * Time: 4:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class MultipleShortestPathLoad {

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("");
            System.exit(1);
        }
        String rulesFolder = args[0];
        String flowFolder = args[1];
        String topologyFile = args[2];
        String outputFile = args[3];
        String runFlowStatsFile = args[4];
        boolean append = Boolean.parseBoolean(args[5]);


        new File(outputFile).getParentFile().mkdirs();

        List<File> ruleFiles = Arrays.asList(new File(rulesFolder).listFiles());
        Collections.sort(ruleFiles);
        Map<String, Object> parameters = new HashMap<String, Object>();
        Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()),
                topologyFile, parameters, new ArrayList<Topology>()).get(0);

        RuleFactory ruleFactory = new RuleFactory(
                new FileFactory.EndOfFileCondition());
        FlowFactory flowFactory = new FlowFactory(new FileFactory.EndOfFileCondition(), topology);
        for (File ruleFile : ruleFiles) {
            System.out.println(ruleFile);

            List<File> flowFiles = Arrays.asList(new File(flowFolder).listFiles());
            for (File flowFile : flowFiles) {
                if (Util.fromEqualRuleSet(ruleFile, flowFile, ruleFactory, flowFactory)) {

                    GetShortestPathLoad.place(ruleFile.getPath(), topologyFile, flowFile.getPath(),null,
                            outputFile, runFlowStatsFile, append, true);
                    append = true;
                }

            }

        }
    }
}
