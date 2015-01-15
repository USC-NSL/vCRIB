package edu.usc.enl.cacheflow.processor.statistics;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.Processor;
import edu.usc.enl.cacheflow.util.Util;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/13/11
 * Time: 4:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class RulesStatisticsProcessor extends StatisticsProcessor<Collection<Rule>> {
    public static final String OVERLAPPING_RULES_STAT = "Overlapping Rules";
    private PrintWriter ruleWildCardWriter;

    public RulesStatisticsProcessor(Collection<Rule> input, Map<String, Object> parameters) {
        super(input, parameters);
    }

    public RulesStatisticsProcessor(Collection<Rule> input, Map<String, Object> parameters, PrintWriter ruleWildCardWriter) {
        super(input, parameters);
        this.ruleWildCardWriter = ruleWildCardWriter;
    }

    public RulesStatisticsProcessor(Processor<?, Collection<Rule>> processorInput, Map<String, Object> statisticParameters) {
        super(processorInput, statisticParameters);
    }

    public Statistics getStat(Collection<Rule> input) throws Exception {
        Statistics statistics = new Statistics();
        statistics.addStat(Partition.NUMBER_OF_RULES_STAT, input.size());
        //statistics.addStat(Partition.WILDCARD_SETS_STAT, getNumberOfUniqueWildcardSets2(input));
        //statistics.addStat(OVERLAPPING_RULES_STAT, averageOverlappingLowerPriority(input));
        if (ruleWildCardWriter != null) {
            writeDimensionsSizes(ruleWildCardWriter, input);
        }
        return statistics;
    }

    public void writeDimensionsSizes(PrintWriter writer, Collection<Rule> rules) throws UnalignedRangeException {
        int dimensionsNum = Util.getDimensionInfos().size();
        int[][] dimensionRuleSize = new int[dimensionsNum][];
        for (int i = 0; i < dimensionRuleSize.length; i++) {
            dimensionRuleSize[i] = new int[rules.size()];
        }
        int i = 0;
        for (Rule rule : rules) {
            for (int j = 0; j < dimensionsNum; j++) {
                dimensionRuleSize[j][i] = rule.getProperty(j).getNumberOfWildcardBits();
            }
            i++;
        }
        for (int[] ints : dimensionRuleSize) {
            boolean first = true;
            for (int anInt : ints) {
                writer.print((first ? "" : ",") + anInt);
                first = false;
            }
            writer.println();
        }
    }

    public static int getNumberOfUniqueWildcardSets2(Collection<Rule> rules) {
        Set<Long> wildCardSets = new HashSet<Long>();
        try {
            for (Rule rule : rules) {
                wildCardSets.add(rule.getWildCardBitPattern());
            }
        } catch (UnalignedRangeException e) {
            e.printStackTrace();
        }
        return wildCardSets.size();
    }

    public static double averageOverlappingLowerPriority(Collection<Rule> rules1) {
        List<Rule> rules = new ArrayList<Rule>(rules1);
        Collections.sort(rules, Rule.PRIORITY_COMPARATOR);
        int sum = 0;
        for (int i = 0; i < rules.size(); i++) {
            if (i % 1000 == 0) {
                System.out.println(i);
            }
            Rule rule1 = rules.get(i);
            skip:
            for (int j = i + 1; j < rules.size(); j++) {
                Rule rule2 = rules.get(j);
                final List<RangeDimensionRange> properties1 = rule1.getProperties();
                final List<RangeDimensionRange> properties2 = rule2.getProperties();
                for (int k = 0; k < properties1.size(); k++) {
                    RangeDimensionRange range1 = properties1.get(k);
                    RangeDimensionRange range2 = properties2.get(k);
                    if (!range1.hasIntersect(range2)) {
                        continue skip;
                    }
                }
                sum++;
            }
        }
        return 2.0 * sum / rules1.size();
    }
}
