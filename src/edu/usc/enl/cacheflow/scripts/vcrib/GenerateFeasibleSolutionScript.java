package edu.usc.enl.cacheflow.scripts.vcrib;

import edu.usc.enl.cacheflow.algorithms.feasibility.memory.FFDNeighborSharing.FFDNeighborSharing;
import edu.usc.enl.cacheflow.algorithms.placement.NoAssignmentFoundException;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveEqualIDProcessor;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/2/12
 * Time: 1:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateFeasibleSolutionScript {
    public static void main(String[] args) throws IOException {
        String parentFolder = "input/nsdi";
        String partitionFile = parentFolder + "\\classbenchpartition\\vmstart\\imc\\20480_classbench_131072_2.txt";
        //String partitionFile = parentFolder + "\\partitiontenant\\vmstart\\-1\\20480_0_64_8_16_0.5_-1_0.25.txt";


//        String parentFolder = "input/nsdismall";
//        String partitionFile = parentFolder + "\\partitionclassbench\\vmstart\\uniform\\-1\\2560_classbench_32768_2.txt";
        String topologyFolder = parentFolder + "/topologylm/memory";
        String outputFolder = parentFolder + "/classbenchclusterffd\\20480_classbench_131072_2\\2";


        //load partitions
        new File(outputFolder).mkdirs();
        Map<String, Object> parameters = new HashMap<String, Object>();
        //compute sizes and input matrix

        UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), true, new HashSet<Rule>());
        List<Partition> partitions = Util.loadFile(partitionFactory, partitionFile, parameters, new LinkedList<Partition>());
        MatrixRuleSet.setRuleSet(partitionFactory.getRules());

        FFDNeighborSharing ffdNeighborSharing = new FFDNeighborSharing(partitions);
        partitions.clear();

        // run ffd neighbor sharing algorithm
        parameters.put("cluster.algorithm", "FFDNeighborFull");
        List<File> topologyFiles = new ArrayList<File>(Arrays.asList(new File(topologyFolder).listFiles()));
        for (File topologyFile : topologyFiles) {
            System.out.println(topologyFile);
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                    new RemoveEqualIDProcessor(Util.EMPTY_LIST), new HashSet<Rule>()), topologyFile.getPath(), parameters, new ArrayList<Topology>()).get(0);
            try {
                Integer vmPerMachine;
                if (parameters.containsKey("flow.vmPerMachine")) {
                    vmPerMachine = ((Number) parameters.get("flow.vmPerMachine")).intValue();
                } else {
                    vmPerMachine = 20;
                }
                long start = System.currentTimeMillis();
                List<FFDNeighborSharing.Machine> machines = ffdNeighborSharing.ffdNeighborhoodSharing( true,
                        true, FFDNeighborSharing.extractMachineCapacities(topology, vmPerMachine));
                System.out.println(System.currentTimeMillis()-start);
                //use the same format as clusters
                PrintWriter writer = new PrintWriter(outputFolder + "/" + new File(partitionFile).getName() + "_" + topologyFile.getName());
                writer.println(Statistics.getParameterLine(parameters));
                for (FFDNeighborSharing.Machine machine : machines) {
                    boolean first = true;
                    for (FFDNeighborSharing.PartitionObject partition : machine.getPartitions()) {
                        writer.print((first ? "" : ",") + partition.getId());
                        first = false;
                    }
                    writer.println();
                }
                writer.close();
            } catch (NoAssignmentFoundException e) {
                e.printStackTrace();
            }
        }
    }


}
