package edu.usc.enl.cacheflow.scripts.stats;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/14/12
 * Time: 1:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class TenantClassifiedFlowsStatsForRuleGeneration {
   /* public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("");
            System.exit(1);
        }
        String partitionFolder = args[0];
        String flowFolder = args[1];
        String topologyFile = args[2];
        String outputFolder = args[3];

        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put("PartitionMethod", "half3");
        parameters.put("PartitionNum", 16384);
        parameters.put("TopologySize", 1000);

        partitionFolder = partitionFolder + "/" + parameters.get("PartitionMethod");
        //flowFolder = flowFolder + "/" + parameters.get("TopologySize");
        new File(outputFolder).mkdirs();
        topologyFile = topologyFile + "/cpu/tree_1000_100.txt";

        List<File> files = Arrays.asList(new File(flowFolder).listFiles());
        Collections.sort(files);


        List<Statistics> statistics = new LinkedList<Statistics>();
        boolean append = false;
        try {
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR),
                    topologyFile).get(0);

            for (File file : files) {
                //folder=new File("input/osdi/partition/equal2/64_classbench_8192_5.txt");
                final String[] split = file.getName().replaceAll("\\.[^\\.]*$", "").split("_");
                parameters.put("BlockPerMachine", split[5]);
                parameters.put("rnd", split[0]);
                parameters.put("intratenant", split[6]);
                System.out.println(file);
                processFile(parameters, statistics, file, topology, partitionFolder);
                final ArrayList<String> parameterNames = new ArrayList<String>(parameters.keySet());
                if (statistics.size() > 0) {

                    Util.writeFile(Statistics.csvStatistics(parameterNames,
                            Statistics.categorize(parameterNames, statistics), statistics.get(0).getStatNames(),
                            true, !append), new File(outputFolder + "/tenantrulegeneration_" + parameters.get("PartitionNum") + "_" + parameters.get("PartitionMethod") + ".csv"), append);
                    statistics.clear();
                    append = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void processFile(Map<String, Object> parameters, List<Statistics> statistics, File flowFile,
                                    Topology topology, String partitionFolder) throws Exception {
        List<Flow> flows = Util.loadFile(new FlowFactory(new FileFactory.EndOfFileCondition(), topology),
                flowFile.getPath());
        final List<Partition> partitions = Util.loadFile(new UnifiedPartitionFactory(
                new FileFactory.EndOfFileCondition()), partitionFolder + "/" + parameters.get("PartitionNum") + "_" + flowFile.getName());
        final Map<Partition, Map<Rule, List<Flow>>> classified = new ThreadTwoLevelTrafficProcessor(
                new OVSClassifier(), new OVSClassifier(), 4).classify(flows, partitions);
        final Statistics stats = new ClassifiedFlowsStatisticsProcessor(classified, parameters,topology).run();
        statistics.add(stats);
    }*/
}
