package edu.usc.enl.cacheflow.scripts.stats;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.UnifiedPartitionFactory;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.statistics.PartitionStatisticsProcessor;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.FilenameFilter;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 3/7/12
 * Time: 7:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class PartitionStats {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("");
            System.exit(1);
        }
        Util.threadNum = Integer.parseInt(args[0]);
        String inputFolder = args[1];
        String outputFile = args[2];

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
            for (File file : files) {

                final Statistics stats = new PartitionStatisticsProcessor(
                        Util.loadFile(
                                new UnifiedPartitionFactory(new FileFactory.EndOfFileCondition(), new HashSet<Rule>()),
                                file.getPath(),
                                parameters,
                                new LinkedList<Partition>()),
                        parameters).run();
                statistics.add(stats);
                final ArrayList<String> parameterNames = new ArrayList<String>(parameters.keySet());
                Util.writeFile(Statistics.csvStatistics(parameterNames,
                        Statistics.categorize(parameterNames, statistics), statistics.get(0).getStatNames(),
                        true, !append), new File(outputFile), append);
                statistics.clear();
                append = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
