package edu.usc.enl.cacheflow.scripts.vmstart;

import edu.usc.enl.cacheflow.model.factory.*;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.factory.VMAssignmentFactory;
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
public class MultipleShortestPathLoadPartition {

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("");
            System.exit(1);
        }
        Util.threadNum = Integer.parseInt(args[0]);
        String partitionFolder = args[1];
        String flowFolder = args[2];
        String inputVmsFolder=args[3];
        String topologyFile = args[4];
        String outputFile = args[5];
        String runFlowStatsFile = args[6];
        boolean append = Boolean.parseBoolean(args[7]);


        new File(outputFile).getParentFile().mkdirs();

        List<File> partitionFiles = Arrays.asList(new File(partitionFolder).listFiles());
        Collections.sort(partitionFiles);
        Map<String, Object> parameters = new HashMap<String, Object>();
        Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()),
                topologyFile, parameters, new ArrayList<Topology>()).get(0);

        UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(
                new FileFactory.EndOfFileCondition(),new LinkedList<Rule>());
        FlowFactory flowFactory = new FlowFactory(new FileFactory.EndOfFileCondition(), topology);
        for (File partitionFile : partitionFiles) {
            if (!partitionFile.getName().matches(".*\\.txt")){
                continue;
            }
            System.out.println(partitionFile);

            List<File> flowFiles = Arrays.asList(new File(flowFolder).listFiles());
            for (File flowFile : flowFiles) {
                if (Util.fromEqualRuleSet(partitionFile, flowFile, partitionFactory, flowFactory)) {
                    File vmsFound = null;
                    if (inputVmsFolder != null && new File(inputVmsFolder).exists()) {
                        List<File> vmsFiles = Arrays.asList(new File(inputVmsFolder).listFiles());
                        VMAssignmentFactory vmAssignmentFactory = new VMAssignmentFactory(new FileFactory.EndOfFileCondition(), topology);
                        for (File vmFile : vmsFiles) {
                            if (Util.fromEqualRuleSet(vmFile, flowFile, vmAssignmentFactory, flowFactory)) {
                                vmsFound = vmFile;
                                break;
                            }
                        }
                    }
                    GetShortestPathLoad.place(partitionFile.getPath(), topologyFile, flowFile.getPath(),vmsFound, outputFile, runFlowStatsFile, append, true);
                    append = true;
                    GetShortestPathLoad.place(partitionFile.getPath(), topologyFile, flowFile.getPath(),vmsFound, outputFile, runFlowStatsFile, append, false);
                    break;
                }

            }

        }
    }
}
