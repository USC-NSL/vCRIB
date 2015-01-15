package edu.usc.enl.cacheflow.algorithms.partition.onedimensionpartition.partitioner;

import edu.usc.enl.cacheflow.algorithms.partition.decisiontree.PartitionTree2;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/6/11
 * Time: 12:07 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Partitioner {

    public abstract List<RangeDimensionRange> partition(List<RangeDimensionRange> ranges, int num, Collection<Rule> rules, Aggregator aggregator);


    public SortedMap<RangeDimensionRange, Integer> calculateRequiredRules(List<RangeDimensionRange> ranges, Collection<Rule> rules, Aggregator aggregator) {
        SortedMap<RangeDimensionRange, Integer> rangeRuleNum = new TreeMap<RangeDimensionRange, Integer>();
        try {
            final int infoIndex = Util.getDimensionInfoIndex(ranges.get(0).getInfo());
            for (RangeDimensionRange range : ranges) {
                //first algorithm
                Collection<Rule> matchedRules = PartitionTree2.getMatchedRules(rules, infoIndex, range);
                //chop the rules
                List<Rule> choppedRules = new ArrayList<Rule>(matchedRules.size());
                for (Rule matchedRule : matchedRules) {
                    final RangeDimensionRange property = matchedRule.getProperty(infoIndex);
                    if (range.covers(property)) {
                        choppedRules.add(matchedRule);
                    } else {
                        //it must be chopped
                        List<RangeDimensionRange> choppedRuleProperties = new ArrayList<RangeDimensionRange>();
                        int dim = 0;
                        for (RangeDimensionRange dimensionRange : matchedRule.getProperties()) {
                            if (dim == infoIndex) {
                                choppedRuleProperties.add(dimensionRange.intersect(range));
                            } else {
                                //just add it
                                choppedRuleProperties.add(dimensionRange);
                            }
                            dim++;
                        }
                        choppedRules.add(new Rule(matchedRule.getAction(), choppedRuleProperties, matchedRule.getPriority(), Rule.maxId + 1));
                    }
                }

                aggregator.setTailInput(choppedRules);
                Collection<Rule> aggregatedRules = aggregator.run();

                rangeRuleNum.put(range, Math.min(matchedRules.size(), aggregatedRules.size()));
                //sum += matchedRules.size();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rangeRuleNum;
    }
}
