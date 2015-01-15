package edu.usc.enl.cacheflow.util;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.*;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.topology.Topology;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveEqualRulesProcessor;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 9/22/11
 * Time: 6:30 AM
 * To change this template use File | Settings | File Templates.
 */
public class Util {
    public static Random random = new Random(22332);
    public static final FilenameFilter TXT_FILTER = new txtFileFilter();
    public static final Aggregator DEFAULT_AGGREGATOR = createDefaultAggregator();
    public static final LinkedList<Rule> EMPTY_LIST = new LinkedList<Rule>();
    public static final Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static Map<DimensionInfo, Integer> dimensionInfos = null;
    private static List<DimensionInfo> dimensionInfoList;
    public static final DimensionInfo SRC_IP_INFO = new IPDimensionInfo("SRCIP", 0, (long) (Math.pow(2, 32) - 1));
    public static final DimensionInfo DST_IP_INFO = new IPDimensionInfo("DSTIP", 0, (long) (Math.pow(2, 32) - 1));
    public static final DimensionInfo SRC_PORT_INFO = new DimensionInfo("SRCPORT", 0, (long) (Math.pow(2, 16) - 1));
    public static final DimensionInfo DST_PORT_INFO = new DimensionInfo("DSTPORT", 0, (long) (Math.pow(2, 16) - 1));
    public static final DimensionInfo PROTOCOL_INFO = new HexDimensionInfo("PROTOCOL", 0, 255);
    public static int threadNum = 2;


    public static void setDimensionInfos(List<DimensionInfo> dimensionInfos1) {
        initWcShifts(dimensionInfos1);
        if (dimensionInfos == null) {
            dimensionInfoList = new ArrayList<DimensionInfo>(dimensionInfos1);
            dimensionInfos = new HashMap<DimensionInfo, Integer>();
            int i = 0;

            for (DimensionInfo dimensionInfo : dimensionInfos1) {
                dimensionInfos.put(dimensionInfo, i++);
            }
        }
    }


    public static List<DimensionInfo> getDimensionInfos() {
        return dimensionInfoList;
    }

    public static <T, C extends Collection<T>> C loadFile(FileFactory<T> fileFactory, String file, Map<String, Object> parameters,
                                                          C toFill) throws IOException {
        final BufferedReader reader = new BufferedReader(new FileReader(file));
        final Collection<T> ts = fileFactory.create(reader, parameters, toFill);
        reader.close();
        return toFill;
    }


    public static <T, C extends Collection<T>> C loadFileFilterParam(FileFactory<T> fileFactory, String file, Map<String, Object> parameters,
                                                                     C toFill, String regex) throws IOException {
        Map<String, Object> tempParameters = new HashMap<String, Object>();
        loadFile(fileFactory, file, tempParameters, toFill);
        for (Map.Entry<String, Object> entry : tempParameters.entrySet()) {
            if (entry.getKey().matches(regex)) {
                parameters.put(entry.getKey(), entry.getValue());
            }
        }
        return toFill;
    }

    public static void loadParameters(FileFactory fileFactory, String file, Map<String, Object> parameters) throws IOException {
        final BufferedReader reader = new BufferedReader(new FileReader(file));
        fileFactory.parseHeaderLine(reader, parameters);
        reader.close();
    }

    public static boolean haveEqualParameters(Map<String, Object> parameters1, Map<String, Object> parameters2, String regex) {
        for (Map.Entry<String, Object> entry1 : parameters1.entrySet()) {
            if (entry1.getKey().matches(regex)) {
                Object value2 = parameters2.get(entry1.getKey());
                Object value1 = entry1.getValue();
                if (value2 == null || !value2.equals(value1) ) {
                    return false;
                }
            }
        }
        return true;
    }

    public static void writeFile(Object o, File file, boolean append) throws IOException {
        final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, append)));
        writer.println(o);
        writer.close();
    }

    /*public static String getCommentLine() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }*/

    public static int getDimensionInfoIndex(DimensionInfo info) {
        return dimensionInfos.get(info);
    }

    public static File getCurrentDirectory() {
        return new File(".");
    }

    public static <T> List<T> permute(List<Integer> permutation, List<T> input) {
        List<T> output = new ArrayList<T>(input.size());
        for (Integer sourceIndex : permutation) {
            output.add(input.get(permutation.get(sourceIndex)));
        }
        return output;
    }

    public static String readFile(File file) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        while (reader.ready()) {
            sb.append(reader.readLine()).append("\n");
        }
        reader.close();
        return sb.toString();
    }

    public static Map<Flow, Rule> CalculateRuleTrafficMap2(List<Flow> flows, Collection<Rule> rules1) {
        List<Rule> rules;
        if (rules1 instanceof List) {
            rules = (List<Rule>) rules1;
        } else {
            rules = new ArrayList<Rule>(rules1);
        }
        Collections.sort(rules, Rule.PRIORITY_COMPARATOR);
        Map<Flow, Rule> output = new HashMap<Flow, Rule>();

        for (Flow flow : flows) {
            for (Rule rule : rules) {
                if (rule.match(flow)) {
                    output.put(flow, rule);
                    //output.get(rule).add(flow);
                    break;
                }
            }
        }
        return output;
    }

    public static Map<Rule, Collection<Flow>> CalculateRuleTrafficMap(Collection<Flow> flows, Collection<Rule> rules1) {
        List<Rule> rules;
        if (rules1 instanceof List) {
            rules = (List<Rule>) rules1;
        } else {
            rules = new ArrayList<Rule>(rules1);
        }
        Collections.sort(rules, Rule.PRIORITY_COMPARATOR);
        Map<Rule, Collection<Flow>> output = new HashMap<Rule, Collection<Flow>>(rules.size(), 1);

        for (Flow flow : flows) {
            for (Rule rule : rules) {
                if (rule.match(flow)) {
                    Collection<Flow> flows1 = output.get(rule);
                    if (flows1 == null) {
                        flows1 = new LinkedList<Flow>();
                        output.put(rule, flows1);
                    }
                    flows1.add(flow);
                    break;
                }
            }
        }

        /*for (Rule rule : rules) {
            List<Flow> ruleFlows = new LinkedList<Flow>();
            output.put(rule, ruleFlows);
            for (Flow flow : flows) {
                if (rule.match(flow)) {
                    ruleFlows.add(flow);
                }

            }
        }*/
        return output;

    }

    public static LinkedList<String> loadFile(File file) throws IOException {
        LinkedList<String> input = new LinkedList<String>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        while (reader.ready()) {
            input.add(reader.readLine());
        }
        reader.close();
        return input;
    }

    public static void setRandom(int randomSeedIndex) {
        try {
            BufferedReader br = new BufferedReader(new FileReader("input/random.txt"));
            for (int i = 0; i < randomSeedIndex; i++) {
                br.readLine();
            }
            random = new Random(Long.parseLong(br.readLine()));
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static <T extends Collection> T getNewCollectionInstance(T rules) {
        try {
            return (T) rules.getClass().getConstructor().newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static int randomSelect(double[] cumulativeScores, Random random) {
        double randomValue = random.nextDouble() * cumulativeScores[cumulativeScores.length - 1];
        int i = Arrays.binarySearch(cumulativeScores, randomValue);
        if (randomValue == 0 && i >= 0) {//there is a zero in the list and we matched on it!
            //go forward
            for (; i < cumulativeScores.length; i++) {
                if (cumulativeScores[i] != 0) {
                    break;
                }
            }
        } else {
            if (i < 0) {
                i = -i - 1;
            } else {
                //we need the earliest of equal items
                for (; i > 0; i--) {
                    if (cumulativeScores[i] != cumulativeScores[i - 1]) {
                        break;
                    }
                }
            }
        }
        return i;
    }

    public static void runThreads(Thread[] findMinThreads) {
        for (Thread findMinThread1 : findMinThreads) {
            findMinThread1.start();
        }

        try {
            for (Thread findMinThread : findMinThreads) {
                findMinThread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void resetDimensionInfos() {
        dimensionInfoList = null;
        dimensionInfos = null;
    }

    public static boolean fromEqualRuleSet(File partitionFile, File flowFile, FileFactory factory1, FileFactory factory2) throws IOException {
        Map<String, Object> parameters1 = new HashMap<String, Object>();
        loadParameters(factory2, flowFile.getPath(), parameters1);
        Map<String, Object> parameters2 = new HashMap<String, Object>();
        loadParameters(factory1, partitionFile.getPath(), parameters2);
        return haveEqualParameters(parameters1, parameters2, "^rule\\..*"); //&& haveEqualParameters(parameters1, parameters2, "randomSeedIndex");
    }

    private static void initWcShifts(List<DimensionInfo> dimensionInfoList) {
        try {
            int sumBits = 0;
            for (int j = dimensionInfoList.size() - 1; j >= 0; j--) {
                DimensionInfo dimensionInfo = dimensionInfoList.get(j);
                int shiftBits = dimensionInfo.getShiftBits();
                //long mask=((1l<<shiftBits)-1)<<sumBits;
                dimensionInfo.setWcShift(sumBits);
                sumBits += shiftBits;
            }
        } catch (UnalignedRangeException e) {
            e.printStackTrace();
        }
    }

    private static class txtFileFilter implements FilenameFilter {

        public boolean accept(File dir, String name) {
            return name.matches(".*\\.txt");
        }
    }

    public static Aggregator createDefaultAggregator() {
        return new RemoveEqualRulesProcessor(EMPTY_LIST);
    }

    /*public static int getRuleMatrixSize() {
        return MatrixRuleSet.getLongNums(ruleSet.size());
    }

    public static void runOnRules(long[] rules, RuleRunnable runnable) throws Exception {
        for (Rule rule : ruleSet) {
            if (MatrixRuleSet.hasOneAt(rules, rule.getId() - 1)) {
                runnable.run(rule);
            }
        }
    }*/


    /*public static Collection<Rule> convertToRules(Collection<Rule> rules, long[] matrix) {
        for (Rule rule : Util.ruleSet) {
            if (Rule.hasOneAt(matrix, rule.getId()-1)) {
                rules.add(rule);
            }
        }
        return rules;
    }*/


    public static interface RuleRunnable {
        public void run(Rule rule) throws Exception;
    }

    public static class IntegerWrapper {
        int value;

        public IntegerWrapper(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;

        }

        public void increment(int v) {
            value += v;
        }

    }

    public static class DoubleWrapper {
        double value;

        public DoubleWrapper(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;

        }

        public void increment(double v) {
            value += v;
        }

    }
}
