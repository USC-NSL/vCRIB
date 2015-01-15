package edu.usc.enl.cacheflow;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.Persistanter;
import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PersistentPartitionTree2;
import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.factory.TopologyFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.model.topology.switchmodel.Switch;
import edu.usc.enl.cacheflow.processor.flow.CustomRandomFlowDistribution;
import edu.usc.enl.cacheflow.processor.flow.GenerateFlowsClassBenchRulesTreeProcessor;
import edu.usc.enl.cacheflow.processor.flow.classbenchgenerator.rulebased.TreeIPTreeFlow;
import edu.usc.enl.cacheflow.processor.flow.ipassigner.ServerAggregateIPAssigner;
import edu.usc.enl.cacheflow.util.Util;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/9/11
 * Time: 10:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class GraphDemo {
    public static void main(String[] args) throws IOException, InterruptedException {
        List<Integer> s = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            s.add(i);
        }
        final ListIterator<Integer> iterator = s.listIterator();
        while (iterator.hasNext()) {
            Integer next = iterator.next();
            iterator.set(0);
        }

        for (Integer integer : s) {
            System.out.println(integer);
        }

        System.exit(0);
        {
            String inputFile = "input\\nsdi\\partitionclassbench\\vmstart\\uniform\\-1\\4076\\20480_classbench_131072_4.txt";
            String outputFile = "input\\nsdi\\partitionclassbench\\vmstart\\uniform\\-1\\4076\\stat_20480_classbench_131072_4.txt";

            HashMap<String, Object> parameters = new HashMap<String, Object>();
            List<Partition> partitions = Util.loadFile(new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(),
                    false, new HashSet<Rule>()), inputFile, parameters,new LinkedList<Partition>());
            PrintWriter writer = new PrintWriter(outputFile);
            boolean first = true;
            for (Partition partition : partitions) {
                writer.print((first ? "" : ",") + partition.getRules().size());
                first = false;
            }

            System.exit(0);
        }

        /*{
            String inputFile = "";
            BufferedReader reader = new BufferedReader(new FileReader("input\\nsdi\\cluster_200_1\\output_200_1.txt"));
            Map<Integer, Map<Integer, Float>> clusters = new HashMap<Integer, Map<Integer, Float>>();
            int i = 1;
            while (reader.ready()) {
                String s = reader.readLine();
                s = s.substring(s.indexOf("c=[") + 3);
                s = s.substring(0, s.indexOf("]"));
                StringTokenizer st = new StringTokenizer(s, ", ");
                HashMap<Integer, Float> fields = new HashMap<Integer, Float>();
                while (st.hasMoreElements()) {
                    String field = (String) st.nextElement();
                    int colon = field.indexOf(":");
                    fields.put(Integer.parseInt(field.substring(0, colon)), Float.parseFloat(field.substring(colon + 1)));
                }
                clusters.put(i++, fields);
            }

            //find similarity
            for (Map.Entry<Integer, Map<Integer, Float>> entry1 : clusters.entrySet()) {
                for (Map.Entry<Integer, Map<Integer, Float>> entry2 : clusters.entrySet()) {
                    Map<Integer, Float> value1 = entry1.getValue();
                    Map<Integer, Float> value2 = entry2.getValue();
                    Set<Integer> common = new HashSet<Integer>(value1.keySet());
                    common.addAll(value2.keySet());
                    float distance = 0;
                    for (Integer dim : common) {
                        Float f1 = value1.get(dim);
                        Float f2 = value2.get(dim);
                        distance += Math.abs((f1 == null ? 0 : f1) - (f2 == null ? 0 : f2));
                    }
                    System.out.print(distance + ",");
                }
                System.out.println();
            }

            System.exit(0);
        }*/


        try {
            final UnifiedPartitionFactory partitionFactory = new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), false, new HashSet<Rule>());
            final List<Partition> partitions = Util.loadFile(partitionFactory,
                    "input\\nsdi\\partitiontenant\\vmstart\\20480_0_300_8_16_0.5_-1_0.25.txt",
                    new HashMap<String, Object>(),new LinkedList<Partition>());
            //"input\\nsdismall\\partition2\\vmstart\\-1\\2559_classbench_8192_2.txt");
//            final List<Cluster> clusters = Util.loadFile(new ClusterFactory(new FileFactory.EndOfFileCondition(), partitions),
//                    //        "input\\nsdi\\clustertenant\\0\\0.txt");
//                    "input\\nsdismall\\cluster_200_1\\-1\\0.txt");
            final int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
            Comparator<Partition> partitionSrcIPStartComparator = new Comparator<Partition>() {
                public int compare(Partition o1, Partition o2) {
                    return Long.compare(o1.getProperty(srcIPIndex).getStart(), o2.getProperty(srcIPIndex).getStart());
                }
            };


            {
                Collections.sort(partitions, partitionSrcIPStartComparator);
                int i = 1;
                for (Partition partition : partitions) {
                    partition.setId(i++);
                }
            }
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            /*{
                System.out.println(clusters.size());
                int i = 1;
                for (Cluster cluster : clusters) {
                    System.out.println(i++ + ": " + cluster.getRulesNum() + ", " + cluster.getPartitions().size());
                }
            }
            int x = 1;
            for (Cluster cluster1 : clusters) {
                List<Integer> distances = new ArrayList<Integer>(clusters.size());
                for (Cluster cluster2 : clusters) {
                    List<Rule> common = new LinkedList<Rule>(cluster1.getRules());
                    common.retainAll(cluster2.getRules());
                    distances.add(common.size());

                    *//*List<Partition> partitions1 = cluster1.getPartitions();
                    Collections.sort(partitions1,partitionIDComparator);
                    for (Partition partition : partitions1) {
                        System.out.print(partition.getProperty(srcIPIndex).getStart() + ",");
                    }
                    System.out.println();

                    List<Partition> partitions2 = cluster2.getPartitions();
                    Collections.sort(partitions2,partitionIDComparator);

                    for (Partition partition : partitions2) {
                        System.out.print(partition.getProperty(srcIPIndex).getStart()+",");
                    }
                    System.out.println();

                    List<Rule> rules1 = new ArrayList<Rule>(cluster1.getRules());
                    Collections.sort(rules1,Rule.ID_COMPARATOR);
                    for (Rule rule : rules1) {
                        System.out.print(rule.getId() + ",");
                    }
                    System.out.println();


                    List<Rule> rules2 = new ArrayList<Rule>(cluster2.getRules());
                    Collections.sort(rules2,Rule.ID_COMPARATOR);
                    for (Rule rule : rules2) {
                        System.out.print(rule.getId() + ",");
                    }
                    System.out.println();

                    Collections.sort(common,Rule.ID_COMPARATOR);
                    for (Rule rule : common) {
                        System.out.print(rule.getId() + ",");
                    }
                    System.out.println();
                    System.out.println();*//*

                }
                System.out.print(x + "," + cluster1.getRulesNum());
                int z = 1;
                for (Integer distance : distances) {
                    System.out.print(z == x ? ",-" : "," + distance);
                    z++;
                }
                System.out.println();
                x++;
            }
            System.out.println("--------------------------------");

            *//*Map<Cluster,float[]> clusterCentroids = new HashMap<Cluster, float[]>();
            for (Cluster cluster : clusters) {
                float[] features = new byte[partitionFactory.getRulesSize()];
                for (Partition partition : cluster.getPartitions()) {
                    for (Rule rule : partition.getRules()) {
                        features[rule.getId()-1]+=1.0/
                    }
                }
            }*//*

            System.out.println("--------------------------------");

            for (Partition partition : partitions) {
                System.out.println(partition.getId() + "," + partition.getSize());
            }*/


            final int imageSegmentss = 5;
            final IntegerWrapper sum = new IntegerWrapper();
            List<Thread> threads = new LinkedList<Thread>();
            for (int i = 0; i < imageSegmentss; i++) {
                final int j = i;
                threads.add(new Thread() {
                    @Override
                    public void run() {
                        try {
                            List<Rule> common = new LinkedList<Rule>();
                            int width = partitions.size() / imageSegmentss;
                            int[][] distanceMatrix = new int[width][partitions.size()];
                            int start = j * width;
                            int max = -1;
                            for (Partition partition1 : partitions.subList(start, (j + 1) * width)) {
                                System.out.println(partition1.getId());
                                for (Partition partition2 : partitions) {
                                    common.clear();
                                    common.addAll(partition1.getRules());
                                    common.retainAll(partition2.getRules());
                                    int value = common.size();
                                    if (max < value) {
                                        max = value;
                                    }
                                    //bi.setRGB(partition1.getId() - 1 - start, partition2.getId() - 1, value);
                                    //int rgb = bi.getRGB(partition1.getId() - 1 - start, partition2.getId() - 1);
                                    distanceMatrix[partition1.getId() - 1 - start][partition2.getId() - 1] = value;
                                }
                            }
                            synchronized (sum) {
                                sum.setValue(sum.getValue() + 1);
                                sum.setMax(max);
                                if (sum.getValue() < imageSegmentss) {
                                    sum.wait();
                                } else {
                                    sum.notifyAll();
                                }
                                max = sum.getMax();
                            }
                            System.out.println("Scaling");
                            BufferedImage bi = new BufferedImage(width, partitions.size(), BufferedImage.TYPE_BYTE_GRAY);
                            double scale = 1.0 * max * 256 / partitionFactory.getRulesSize();
                            for (Partition partition1 : partitions.subList(start, (j + 1) * width)) {
                                System.out.println(partition1.getId());
                                for (Partition partition2 : partitions) {
                                    if (common.size() > 0) {
                                        bi.setRGB(partition1.getId() - 1 - start, partition2.getId() - 1,
                                                (int) (scale * distanceMatrix[partition1.getId() - 1 - start][partition2.getId() - 1] * 0x010101));
                                    }
                                }
                            }

                            ImageIO.write(bi, "gif", new File("partitionpartition_" + j + ".gif"));
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                );
            }
            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }


            /*{
                int ystrech = partitions.size() / clusters.size();
                int width = partitions.size() / 2;
                BufferedImage bi = new BufferedImage(width, ystrech * clusters.size(), BufferedImage.TYPE_BYTE_BINARY);
                Graphics g2 = bi.getGraphics();
                //g2.setColor(Color.white);
                //g2.fillRect(0,0,bi.getWidth(),bi.getHeight());
                int ydim = 0;
                //g2.setColor(Color.BLACK);
                for (Cluster cluster : clusters) {
                    for (Partition partition : cluster.getPartitions()) {
                        //bi.setRGB(partition.getId()-1,ydim,0x000000);
                        if (partition.getId() - 1 < width) {
                            g2.fillRect(partition.getId() - 1, ydim, 1, ystrech);
                        }
                    }
                    ydim += ystrech;
                }
                ImageIO.write(bi, "gif", new File("clusterpartition1_200.gif"));
            }
            {
                int ystrech = partitions.size() / clusters.size();
                int width = partitions.size() / 2;
                BufferedImage bi = new BufferedImage(width, ystrech * clusters.size(), BufferedImage.TYPE_BYTE_BINARY);
                Graphics g2 = bi.getGraphics();
                //g2.setColor(Color.white);
                //g2.fillRect(0,0,bi.getWidth(),bi.getHeight());
                int ydim = 0;
                //g2.setColor(Color.BLACK);
                for (Cluster cluster : clusters) {
                    for (Partition partition : cluster.getPartitions()) {
                        //bi.setRGB(partition.getId()-1,ydim,0x000000);
                        if (partition.getId() - 1 >= width) {
                            g2.fillRect(partition.getId() - 1 - width, ydim, 1, ystrech);
                        }
                    }
                    ydim += ystrech;
                }
                ImageIO.write(bi, "gif", new File("clusterpartition2_200.gif"));
            }*/

            /*{
                int ystrech = 1;// partitionFactory.getRules().size() / partitions.size();
                int width = partitionFactory.getRules().size() / 2;
                {

                    BufferedImage bi = new BufferedImage(width, ystrech * partitions.size(), BufferedImage.TYPE_BYTE_GRAY);
                    Graphics g2 = bi.getGraphics();
                    //g2.fillRect(0,0,bi.getWidth(),bi.getHeight());
                    int ydim = 0;
                    for (Partition partition : partitions) {
                        for (Rule rule : partition.getRules()) {
                            if (rule.getId() - 1 < width) {
                                g2.fillRect(rule.getId() - 1, ydim, 1, ystrech);
                            }
                        }
                        ydim += ystrech;
                    }
                    ImageIO.write(resize(bi), "gif", new File("partitionrule.gif"));
                }

                {
                    BufferedImage bi2 = new BufferedImage(width, ystrech * partitions.size(), BufferedImage.TYPE_BYTE_GRAY);
                    Graphics g22 = bi2.getGraphics();
                    int ydim = 0;
                    for (Partition partition : partitions) {
                        for (Rule rule : partition.getRules()) {
                            if (rule.getId() - 1 >= width) {
                                g22.fillRect(rule.getId() - 1 - width, ydim, 1, ystrech);
                            }
                        }
                        ydim += ystrech;
                    }
                    ImageIO.write(resize(bi2), "gif", new File("partitionrule2.gif"));
                }

            }*/

            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), null, new HashSet<Rule>()),
                    "input/test/topology/tree_0_25.txt", new HashMap<String, Object>(),new ArrayList<Topology>(1)).get(0);
            Map<String, Switch> nameSwitchMap = new HashMap<String, Switch>();
            for (Switch aSwitch : topology.getSwitches()) {
                nameSwitchMap.put(aSwitch.getId(), aSwitch);
            }

            final Switch s1 = nameSwitchMap.get("Edge_05_11_06");
            final Switch s2 = nameSwitchMap.get("Edge_05_12_05");
            //System.out.println(topology.getPathLength(s1, s2));
            System.out.println(topology.getPath(s1, s2, new Flow(0, s1, s2, new Long[]{0l, 0l, 0l, 0l, 0l})));
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }


        /* int index = 1;
        for (int i = 1; i <= 25; i++) {
            for (int j = 1; j <= 50; j++) {
                System.out.println("Core_" + i + " Agg_" + j + " 6000000");
                index++;
            }
        }
        System.exit(0);*/


        String ruleFile = "input/out/classbenchrules/converted/classbench_65536_1.txt";
        //"input/osdi/classbenchrules/converted/out/classbench_32768_1.txt";
        //"input/osdi/classbenchrules/converted/classbench_4096_1.txt";
        Persistanter.tableName = "treenode_65_1";
        String flowFile = "output/nodb_flow_100_65_1.txt";
        int ruleSize = 100 * 1000;


        Util.setDimensionInfos(Arrays.asList(Util.DST_IP_INFO, Util.DST_PORT_INFO,
                Util.PROTOCOL_INFO, Util.SRC_IP_INFO, Util.SRC_PORT_INFO));

        /*{
            try {
                String topologyFile = "input/osdi/topology/100/tree_1000_100.txt";
                Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR), topologyFile).get(0);
                List<Flow> flows = Util.loadFile(new FlowFactory(new FileFactory.EndOfFileCondition(), topology), flowFile);

//                List<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()), "input/osdi/classbenchrules/converted/classbench_4096_1.txt");
//                int i=1;
//                for (Rule rule : rules) {
//                    rule.setId(i++);
//                }
//                final Map<Rule, List<Flow>> classify = new LinearMatchTrafficProcessor().classify(flows, rules);
//                List<Integer> ids = new ArrayList<Integer>();
//                for (Rule rule : classify.keySet()) {
//                    ids.add(rule.getId());
//                }
//                Collections.sort(ids);
//                for (Integer id : ids) {
//                    System.out.println(id);
//                }

                List<Partition> partitions = Util.loadFile(new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition()),
                        "input/osdi/partition/equal2/1024_classbench_32768_2.txt");
                final Map<Partition, Map<Rule, List<Flow>>> classified = new TwoLevelTrafficProcessor(new LinearMatchTrafficProcessor(),
                        new LinearMatchTrafficProcessor()).classify(flows, partitions);
                final Statistics stats = new ClassifiedFlowsStatisticsProcessor(classified, new HashMap<String, Object>()).run();
                for (String s : stats.getStatNames()) {
                    System.out.println(s+":"+stats.getStat(s));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.exit(0);
        }*/

        try {
            String topologyFile = "input/osdi/topology/100/tree_1000_100.txt";
            String flowDescriptionFile = "input/osdi/flowspecs.txt";
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(),
                    Util.DEFAULT_AGGREGATOR, new HashSet<Rule>()), topologyFile, new HashMap<String, Object>(),new ArrayList<Topology>(1)).get(0);
            new GenerateFlowsClassBenchRulesTreeProcessor(new CustomRandomFlowDistribution(
                    Util.loadFile(new File(flowDescriptionFile))), new TreeIPTreeFlow(20, new ServerAggregateIPAssigner(20)),
                    Util.random, true, true).process(topology, ruleSize,
                    new PrintWriter(new BufferedWriter(new FileWriter(flowFile))));
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);

        try {
            long start = System.currentTimeMillis();                                                                                       //out/classbench_32768_1.txt
            int i = 1;                                                                                                                     //classbench_4096_1.txt
            //"input/osdi/classbenchrules/converted/out/classbench_32768_5.txt"
            Collection<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()), ruleFile,
                    new HashMap<String, Object>(),new HashSet<Rule>());
            final List<Integer> permutation = new ArrayList<Integer>(Arrays.asList(
                    Util.getDimensionInfoIndex(Util.SRC_IP_INFO),
                    Util.getDimensionInfoIndex(Util.DST_IP_INFO),
                    Util.getDimensionInfoIndex(Util.PROTOCOL_INFO),
                    Util.getDimensionInfoIndex(Util.DST_PORT_INFO),
                    Util.getDimensionInfoIndex(Util.SRC_PORT_INFO)
            ));
            final PersistentPartitionTree2 persistentPartitionTree2 = new PersistentPartitionTree2(true);
            persistentPartitionTree2.init(rules.size(), Util.getDimensionInfos(), permutation);
            persistentPartitionTree2.semigridAndMergeTogether(rules, Util.getDimensionInfos(), permutation);
            persistentPartitionTree2.close();
            System.out.println((System.currentTimeMillis() - start) / 1000.0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(0);


        /*try {
            String topologyFile = "input/osdi/topology/100/tree_1000_100.txt";
            String flowDescriptionFile = "input/osdi/flowspecs.txt";
            //List<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()), ruleFile);
            Topology topology = Util.loadFile(new TopologyFactory(new FileFactory.EndOfFileCondition(), Util.DEFAULT_AGGREGATOR), topologyFile).get(0);
            List<Partition> partitions = Util.loadFile(new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition()),
                    "input/osdi/partition/equal2/1024_classbench_32768_2.txt");
            List<Rule> rules;
            {
                Set<Rule> rules2 = new HashSet<Rule>();
                for (Partition partition : partitions) {
                    rules2.addAll(partition.getRules());
                }
                rules = new ArrayList<Rule>(rules2);
            }
            final TwoLevelTrafficProcessorOneByOne classifier = new TwoLevelTrafficProcessorOneByOne(new TreeMatchTrafficProcessor(), partitions);

            new GenerateFlowsClassBenchRulesProcessor(new FlowDistribution(Util.loadFile(new File(flowDescriptionFile))),
                    new RuleIPRuleFlow(50, 50,classifier), Util.random, true, true).
                    process(topology, rules, new PrintWriter(new BufferedWriter(new FileWriter(flowFile))));
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);*/


        /*


    Set<String> s = new HashSet<String>();
    s.add("123");
    System.out.println(s);

    long start = System.currentTimeMillis();
    String ruleFile = "input/osdi/classbenchrules/converted/classbench_32768_1.txt";
    try {
        List<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()), ruleFile);
        final PartitionTree2 partitionTree2 = new PartitionTree2();
        partitionTree2.semigridAndMergeTogether(rules, Util.getDimensionInfos(), PartitionTree2.findPermutation(rules, Util.getDimensionInfos()));
    } catch (IOException e) {
        e.printStackTrace();
    }
    System.out.println(System.currentTimeMillis() - start);

    System.out.println("merge with child " + PartitionTree2.mergeWithChild);
    System.out.println("leaf agg " + PartitionTree2.getLeafAgg());
    System.out.println("internal agg " + PartitionTree2.getIntAgg());*/

        /* int start =61900;
        int end = 61909;


        int bitToZero=binlog(end^(start));
        int nearest2=(end)>>>bitToZero;
        nearest2<<=bitToZero;
        *//*if (nearest2==0){
            nearest2=1<<bitToZero;//middle power of 2
        }*//*
        start=start-1;

        //int nearest2 = (int)(Math.pow(2,binlog(start)+1));
        //come down from nearest
        int current = nearest2-1;
        while (current>start){
            int step = (int)Math.pow(2, binlog(current - start));
            System.out.println(current-step+1+":"+current);
            current-=step;
        }

        //go up to the end
        current =nearest2;
        while(current<end+1){
            int step = (int)Math.pow(2,binlog(end+1-current));
            System.out.println(current+":"+(current+step-1));
            current+=step;
        }

        //EdgeLabelDemo.main(new String[]{});
        //GraphEditorDemo.main(null);
        //ImageEdgeLabelDemo.main(null);
        //InternalFrameSatelliteViewDemo.main(null);
        //LensDemo.main(null);
        //LensVertexImageShaperDemo.main(null);
        //ShortestPathDemo.main(null);
        //VertexLabelPositionDemo .main(null);
        PluggableRendererDemo.main(null);*/
    }

    private static BufferedImage resize(BufferedImage bi) {
        int newWidth = bi.getWidth() / 10;
        int newHeight = bi.getHeight() / 10;
        BufferedImage resizedImage = new BufferedImage(newWidth, newHeight, BufferedImage.TYPE_BYTE_GRAY);
        Graphics2D g = resizedImage.createGraphics();
        g.drawImage(bi, 0, 0, newWidth, newHeight, null);
        g.dispose();
        g.setComposite(AlphaComposite.Src);
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        g.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        return resizedImage;
    }

    public static int binlog(int bits) // returns 0 for bits=0
    {
        int log = 0;
        if ((bits & 0xffff0000) != 0) {
            bits >>>= 16;
            log = 16;
        }
        if (bits >= 256) {
            bits >>>= 8;
            log += 8;
        }
        if (bits >= 16) {
            bits >>>= 4;
            log += 4;
        }
        if (bits >= 4) {
            bits >>>= 2;
            log += 2;
        }
        return log + (bits >>> 1);
    }

    private static class IntegerWrapper {
        int value = 0;
        int max = -1;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public void setMax(int max) {
            this.max = this.max < max ? max : this.max;
        }

        public int getMax() {
            return max;
        }
    }

}

