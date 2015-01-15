package edu.usc.enl.cacheflow.processor.rule.aggregator.liu;

import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.processor.Processor;
import edu.usc.enl.cacheflow.processor.file.SaveFileProcessor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.processor.rule.generator.RandomRuleGenerator;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/15/11
 * Time: 10:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class ACLCompressionProcessor extends Aggregator {
    public int calls;
    private static Runtime runtime = Runtime.getRuntime();
    private static final Object common = new Object();
    private static int callsUntilNow = 0;

    private static final DimensionInfo EXTERNAL_DIMENSION_INFO = new DimensionInfo("External", 0, 0);

    public ACLCompressionProcessor(Collection<Rule> input) {
        super(input);
    }

    public ACLCompressionProcessor(Processor<?, Collection<Rule>> processorInput) {
        super(processorInput);
    }

    @Override
    public Collection<Rule> process(Collection<Rule> input) throws Exception {
        synchronized (common) {
            calls = callsUntilNow++;
        }
        String libPath = "lib/SCAPPatt";
        String tempfile = libPath + "/tmp" + calls + ".txt";

        Map<String, DimensionInfo> infosMap = new HashMap<String, DimensionInfo>();
        for (DimensionInfo dimensionInfo : Util.getDimensionInfos()) {
            infosMap.put(dimensionInfo.getName(), dimensionInfo);

        }

        String rulesString = WriterSerializableUtil.getString(input, new HashMap<String, Object>());
        { //preprocess rules
            rulesString = rulesString.replaceAll("Deny", "0");
            rulesString = rulesString.replaceAll("Accept", "1");
        }

        //write output and run
        new SaveFileProcessor<String>(rulesString, new File(tempfile), false).run();
        String line;
        String command = libPath + "/SCAPP.exe ACL file=tmp" + calls + ".txt permuation=any output=true stats=ignore" + calls + ".csv";
        System.out.println(command);
        Process p = runtime.exec(command, null, new File(libPath));
        BufferedReader bri = new BufferedReader
                (new InputStreamReader(p.getInputStream()));
        LinkedList<String> commandOutput = new LinkedList<String>();
        List<DimensionInfo> oldInfos = new ArrayList<DimensionInfo>();
        {
            int i = 0;
            while ((line = bri.readLine()) != null) {
                //NOTE 0 AND 1 IS REPLACED!!
                /*line = line.replaceAll("1$", "Deny");
                line = line.replaceAll("0$", "Accept");*/
                line = line.replaceAll("Null$", "0");// Bug in external code for default rule
                if (i == 1) {
                    String[] dimensionsName = line.split(",");
                    for (String name : dimensionsName) {
                        oldInfos.add(infosMap.get(name));
                    }
                } else if (i > 2) {
                    commandOutput.add(line);
                }
                i++;
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
        new File(libPath + "/ignore" + calls + ".csv").delete();

        List<DimensionInfo> externalInfos = Collections.nCopies(oldInfos.size(), EXTERNAL_DIMENSION_INFO);
        List<Rule> rules = new ArrayList<Rule>(commandOutput.size());
        int priority = 0;
        for (String ruleLine : commandOutput) {
            //extract action
            int actionIndex = ruleLine.lastIndexOf(",") + 1;
            String action = ruleLine.substring(actionIndex);

            // parse ranges

            List<RangeDimensionRange> values = RuleFactory.parseRanges(ruleLine.substring(0, actionIndex - 1), externalInfos);
            Iterator<DimensionInfo> oldInfosIterator = oldInfos.iterator();
            for (RangeDimensionRange value : values) {
                value.setInfo(oldInfosIterator.next());
            }
            rules.add(new Rule(action.equals("0") ? AcceptAction.getInstance() : DenyAction.getInstance(), values, priority++, Rule.maxId + 1));
        }
        return rules;
    }

    public static void main(String[] args) {
        try {
            File ruleSpec = new File("input/ruletemplate.txt");
            final RuleFactory ruleFactory = new RuleFactory(new FileFactory.EndOfFileCondition());
            ruleFactory.parseHeaderLine(new BufferedReader(new StringReader(Util.readFile(ruleSpec))), new HashMap<String, Object>());
            List<DimensionInfo> infos = ruleFactory.getDimensionInfos();
            Collection<Rule> run = new ACLCompressionProcessor(new RandomRuleGenerator(new Random(System.currentTimeMillis()), infos, 32).generateRules()).run();
            for (Rule rule : run) {
                System.out.println(rule);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}