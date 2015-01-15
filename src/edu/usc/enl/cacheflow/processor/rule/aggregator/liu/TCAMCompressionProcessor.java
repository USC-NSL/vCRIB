package edu.usc.enl.cacheflow.processor.rule.aggregator.liu;

import edu.usc.enl.cacheflow.model.WriterSerializable;
import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.processor.rule.generator.RandomRuleGenerator;
import edu.usc.enl.cacheflow.util.Util;

import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/19/11
 * Time: 5:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class TCAMCompressionProcessor extends Aggregator {
    public int calls;
    private static Runtime runtime = Runtime.getRuntime();
    private static final Object common = new Object();
    private static int callsUntilNow = 0;

    private static final DimensionInfo EXTERNAL_DIMENSION_INFO = new DimensionInfo("External", 0, 0);
    public List<DimensionInfo> externalInfos;

    public TCAMCompressionProcessor(Collection<Rule> input) {
        super(input);
    }


    @Override
    public Collection<Rule> process(Collection<Rule> input) throws Exception {
        synchronized (common) {
            calls = callsUntilNow++;
        }
        String libPath = "lib/SCAPPatt";
        String tempfile = libPath + "/ttmp" + calls + ".txt";

        //change rule actions and shift rules dimensions to start from 0. ASSUMES LAST RULE IS THE DEFAULT RULE
        List<DimensionInfo> oldInfos = Util.getDimensionInfos();
        externalInfos = Collections.nCopies(oldInfos.size(), EXTERNAL_DIMENSION_INFO);

        Map<String, DimensionInfo> infosMap = new HashMap<String, DimensionInfo>();
        for (DimensionInfo oldInfo : oldInfos) {
            infosMap.put(oldInfo.getName(), oldInfo);
        }

        List<MyRule> myRules = new ArrayList<MyRule>(input.size());
        long[] shiftMatrix = new long[oldInfos.size()];
        {
            //shift
            for (Rule rule : input) {
                myRules.add(new MyRule(rule));
            }
            MyRule lastRule = myRules.get(myRules.size() - 1);
            int i = 0;
            for (RangeDimensionRange value : lastRule.values) {
                shiftMatrix[i] = value.getStart();
                i++;
            }
            for (MyRule myRule : myRules) {
                myRule.shift(shiftMatrix);
            }
        }

        {
            try {
                final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(tempfile)));
                //save file
                WriterSerializableUtil.write(myRules, writer, new HashMap<String, Object>());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        List<MyRule> newRules = new LinkedList<MyRule>();
        {//run
            String line;
            String command = libPath + "/SCAPP.exe Razor file=ttmp" + calls + ".txt permuation=any output=true stats=tignore" + calls + ".csv";
            System.out.println(command);
            Process p = runtime.exec(command, null, new File(libPath));
            BufferedReader bri = new BufferedReader
                    (new InputStreamReader(p.getInputStream()));


            {
                int j = 0;
                while ((line = bri.readLine()) != null) {
                    if (j == 1) {
                        //load names (can be exchanged because of permutation selection
                        String[] dimensionsName = line.split(",");
                        oldInfos.clear();
                        for (String name : dimensionsName) {
                            oldInfos.add(infosMap.get(name));
                        }
                    } else if (j > 2) {
                        newRules.add(new MyRule(line, oldInfos.size()));
                    }
                    j++;
                }
            }
            bri.close();

            BufferedReader bre = new BufferedReader
                    (new InputStreamReader(p.getErrorStream()));
            while ((line = bre.readLine()) != null) {
                System.out.println(line);
            }
            bre.close();

            p.waitFor();
            new File(tempfile).delete();
            new File(libPath + "/tignore" + calls + ".csv").delete();
        }

        // shift back rules
        for (MyRule newRule : newRules) {
            newRule.shiftBack(shiftMatrix);
        }

        //create rules
        List<Rule> output = new ArrayList<Rule>(newRules.size());
        int j = 0;
        for (MyRule newRule : newRules) {
            output.add(newRule.getRule(oldInfos, j));
            j++;
        }

        return output;
    }

    public static void main(String[] args) {
        try {
            File ruleSpec = new File("input/ruletemplate.txt");
            final RuleFactory ruleFactory = new RuleFactory(new FileFactory.EndOfFileCondition());
            ruleFactory.parseHeaderLine(new BufferedReader(new StringReader(Util.readFile(ruleSpec))), new HashMap<String, Object>());
            List<DimensionInfo> infos = ruleFactory.getDimensionInfos();
            Collection<Rule> run = new TCAMCompressionProcessor(new RandomRuleGenerator(new Random(System.currentTimeMillis()), infos, 32).generateRules()).run();
            for (Rule rule : run) {
                System.out.println(rule);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class MyRule implements WriterSerializable {
        List<RangeDimensionRange> values;
        String action;

        public MyRule(Rule rule) {
            List<RangeDimensionRange> properties = rule.getProperties();
            values = new ArrayList<RangeDimensionRange>(properties.size());
            for (RangeDimensionRange dimensionRange : properties) {
                values.add(new RangeDimensionRange(dimensionRange.getStart(), dimensionRange.getEnd(), EXTERNAL_DIMENSION_INFO));
            }
            action = rule.getAction() instanceof AcceptAction ? "1" : "0";
        }

        public MyRule(String ruleLine, int numOfRanges) {
            //extract action
            int actionIndex = ruleLine.lastIndexOf(",") + 1;
            action = ruleLine.substring(actionIndex);

            // parse ranges

            values = RuleFactory.parseRanges(ruleLine.substring(0, actionIndex - 1), externalInfos);
        }

        public void shift(long[] shiftMatrix) {
            for (int i = 0; i < shiftMatrix.length; i++) {
                long s = shiftMatrix[i];
                RangeDimensionRange v = values.get(i);
                v.setStart(v.getStart() - s);
                v.setEnd(v.getEnd() - s);
            }
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (RangeDimensionRange value : values) {
                sb.append(value.toString()).append(",");
            }
            sb.append(action);
            return sb.toString();
        }

        public String toStringNoAction() {
            StringBuilder sb = new StringBuilder();
            for (RangeDimensionRange value : values) {
                sb.append(value.toString()).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        }

        public void shiftBack(long[] shiftMatrix) {
            for (int i = 0; i < shiftMatrix.length; i++) {
                long s = shiftMatrix[i];
                RangeDimensionRange v = values.get(i);
                v.setStart(v.getStart() + s);
                v.setEnd(v.getEnd() + s);
            }
        }

        public Rule getRule(List<DimensionInfo> infos, int priority) {
            Iterator<DimensionInfo> infoIterator = infos.iterator();
            for (RangeDimensionRange value : values) {
                value.setInfo(infoIterator.next());
            }
            // action is reversed
            return new Rule(action.equals("0") ? AcceptAction.getInstance() : DenyAction.getInstance(),
                    values, priority, Rule.maxId + 1);
        }

        public void toString(PrintWriter p) {
            RangeDimensionRange.toString(p, values);
            p.println("," + action);
        }

        public void headerToString(PrintWriter p) {
            DimensionInfo.dimensionInfosToString(Util.getDimensionInfos(), p);

        }
    }
}