package edu.usc.enl.cacheflow.scripts.stats;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.FlowFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.flow.classifier.LinearMatchTrafficProcessor;
import edu.usc.enl.cacheflow.processor.flow.classifier.TwoLevelTrafficProcessor;
import edu.usc.enl.cacheflow.processor.statistics.ClassifiedFlowsStatisticsProcessor;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/14/12
 * Time: 1:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class ClassifiedFlowsStats {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("");
            System.exit(1);
        }
        String partitionFolder = args[0];
        String flowFolder = args[1];
        String topologyFile = args[2];
        String outputFile = args[3];
        String distributionsFile=args[4];


        new File(outputFile).getParentFile().mkdirs();
        new File(distributionsFile).getParentFile().mkdirs();

        List<File> partitionFiles = Arrays.asList(new File(partitionFolder).listFiles());
        Collections.sort(partitionFiles);
        List<Statistics> statistics = new LinkedList<Statistics>();
        boolean append = false;
        try {
            Map<String, Object> parameters = new HashMap<String, Object>();
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()),
                    topologyFile, parameters, new LinkedList<Topology>()).get(0);

            UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(
                    new FileFactory.EndOfFileCondition(), new HashSet<Rule>());
            FlowFactory flowFactory = new FlowFactory(new FileFactory.EndOfFileCondition(), topology);
            for (File partitionFile : partitionFiles) {
                final List<Partition> partitions = Util.loadFile(partitionFactory, partitionFile.getPath(), parameters, new LinkedList<Partition>());
                System.out.println(partitionFile);

                List<File> flowFiles = Arrays.asList(new File(flowFolder).listFiles());
                for (File flowFile : flowFiles) {
                    if (Util.fromEqualRuleSet(partitionFile, flowFile, partitionFactory, flowFactory)) {
                        List<Flow> flows = Util.loadFileFilterParam(flowFactory, flowFile.getPath(), parameters, new LinkedList<Flow>(), "flow\\..*");
                        final Map<Partition, Map<Rule, Collection<Flow>>> classified = new TwoLevelTrafficProcessor(
                                new LinearMatchTrafficProcessor(), new LinearMatchTrafficProcessor()).classify(flows, partitions);
                        final Statistics stats = new ClassifiedFlowsStatisticsProcessor(classified, parameters, null).run();
                        statistics.add(stats);
                        final ArrayList<String> parameterNames = new ArrayList<String>(parameters.keySet());
                        Util.writeFile(Statistics.csvStatistics(parameterNames,
                                Statistics.categorize(parameterNames, statistics), statistics.get(0).getStatNames(),
                                true, !append), new File(outputFile), append);
                        statistics.clear();
                        append = true;
                    }

                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
