package edu.usc.enl.cacheflow.scripts.stats;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.statistics.RulesStatisticsProcessor;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/7/12
 * Time: 7:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class RulesStats {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("");
            System.exit(1);
        }
        String inputFolder = args[0];
        List<String> ruleSizes = Arrays.asList(args[1].split(","));
        List<String> paramSets = Arrays.asList(args[2].split(","));
        String outputFile = args[3];

        new File(outputFile).getParentFile().mkdirs();

        List<File> files = Arrays.asList(new File(inputFolder).listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.matches(".*\\.txt");
            }
        }));
        Collections.sort(files);
        Map<String, Object> parameters = new HashMap<String, Object>();
        List<Statistics> statistics = new LinkedList<Statistics>();
        boolean append = false;
        try {
            RuleFactory ruleFactory = new RuleFactory(new FileFactory.EndOfFileCondition());
            for (File file : files) {
                HashMap<String, Object> tempParameters = new HashMap<String, Object>();
                Util.loadParameters(ruleFactory, file.getPath(), tempParameters);
                if (ruleSizes.contains(tempParameters.get("rule.num").toString()) && paramSets.contains(tempParameters.get("rule.paramSet").toString())) {
                    PrintWriter ruleWildCardWriter = new PrintWriter(new File(outputFile).getParent() + "/" + file.getName());
                    final Statistics stats = new RulesStatisticsProcessor(
                            Util.loadFile(ruleFactory, file.getPath(), parameters,new HashSet<Rule>()), parameters, ruleWildCardWriter).run();
                    ruleWildCardWriter.close();
                    statistics.add(stats);
                    final ArrayList<String> parameterNames = new ArrayList<String>(parameters.keySet());
                    Util.writeFile(Statistics.csvStatistics(parameterNames,
                            Statistics.categorize(parameterNames, statistics), statistics.get(0).getStatNames(),
                            true, !append), new File(outputFile), append);
                    statistics.clear();
                    append = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
