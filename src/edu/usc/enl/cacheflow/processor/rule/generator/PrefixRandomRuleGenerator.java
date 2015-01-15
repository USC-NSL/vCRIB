package edu.usc.enl.cacheflow.processor.rule.generator;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.model.rule.action.Action;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/27/11
 * Time: 10:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class PrefixRandomRuleGenerator {
    private Random random;
    private List<DimensionInfo> ranges;
    private int numberOfRules;

    public PrefixRandomRuleGenerator(Random random, List<DimensionInfo> ranges, int numberOfRules) {
        this.random = random;
        this.ranges = ranges;
        this.numberOfRules = numberOfRules;
    }

    public List<Rule> generateRules() {
        List<Action> possibleActions = Arrays.asList(DenyAction.getInstance(), AcceptAction.getInstance());
        List<Rule> outputRules = new ArrayList<Rule>(numberOfRules);
        for (int i = 0; i < numberOfRules - 1; i++) {
            //generate rule properties
            Rule rule = getNewRule(random, possibleActions, i, ranges);
            outputRules.add(rule);
        }

        //add default rule
        List<RangeDimensionRange> defaultRuleProperties = new ArrayList<RangeDimensionRange>(ranges.size());
        for (DimensionInfo range : ranges) {
            defaultRuleProperties.add(range.getDimensionRange());
        }
        outputRules.add(new Rule(possibleActions.get(0), defaultRuleProperties, numberOfRules, Rule.maxId + 1));

        return outputRules;
    }

    public static Rule getNewRule(Random random, List<Action> possibleActions, int priority, List<DimensionInfo> ranges) {
        List<RangeDimensionRange> ruleProperties = new ArrayList<RangeDimensionRange>(ranges.size());
        for (DimensionInfo rangeInfo : ranges) {
            RangeDimensionRange dimensionRange = getRandomRange(random, rangeInfo);
            ruleProperties.add(dimensionRange);
        }

        //select rule action
        return new Rule(possibleActions.get(random.nextInt(possibleActions.size())), ruleProperties, priority, Rule.maxId + 1);
    }

    public static RangeDimensionRange getRandomRange(Random random, DimensionInfo rangeInfo) {
        long size = rangeInfo.getDimensionRange().getSize();
        int wildcardBits = random.nextInt(RangeDimensionRange.binlog(size) + 1);
        long rand = (long) (random.nextDouble() * size);
        rand = rand % size;
        long rangeSize = 1l << wildcardBits;

        long start1 = (rand >>> wildcardBits) << wildcardBits;

        long end1 = start1 + rangeSize - 1;
        return new RangeDimensionRange(start1, end1, rangeInfo);
    }
}
