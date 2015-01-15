package edu.usc.enl.cacheflow.processor.rule;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/25/12
 * Time: 10:48 AM
 * To change this template use File | Settings | File Templates.
 */
public class BreakPrefixWildcardProcessor {


    public static List<Rule> process(Collection<Rule> input) {
        LinkedList<Rule> input2 = new LinkedList<Rule>( input);
        List<Rule> output = new LinkedList<Rule>();
        while (input2.size() > 0) {
            final Rule rule = input2.pop();
            int i = 0;
            try {
                for (; i < Util.getDimensionInfos().size(); i++) {
                    final RangeDimensionRange property = rule.getProperty(i);

                    property.getNumberOfWildcardBits();
                }
                output.add(rule);
            } catch (UnalignedRangeException e) {
                //prperty i must be broken
                final List<RangeDimensionRange> rangeDimensionRanges = RangeDimensionRange.breakRange(rule.getProperty(i));
                for (RangeDimensionRange rangeDimensionRange : rangeDimensionRanges) {
                    List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(rule.getProperties());
                    properties.set(i,rangeDimensionRange);
                    input2.addFirst(new Rule(rule.getAction(),properties,rule.getPriority(),Rule.maxId+1));
                }
            }
        }
        return output;
    }

    public static List<Rule> process(Rule input) {
        LinkedList<Rule> input2 = new LinkedList<Rule>();
        input2.add(input);
        List<Rule> output = new LinkedList<Rule>();
        while (input2.size() > 0) {
            final Rule rule = input2.pop();
            int i = 0;
            try {
                for (; i < Util.getDimensionInfos().size(); i++) {
                    final RangeDimensionRange property = rule.getProperty(i);
                    property.getNumberOfWildcardBits();
                }
                output.add(rule);
            } catch (UnalignedRangeException e) {
                //prperty i must be broken
                final List<RangeDimensionRange> rangeDimensionRanges = RangeDimensionRange.breakRange(rule.getProperty(i));
                for (RangeDimensionRange rangeDimensionRange : rangeDimensionRanges) {
                    List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(rule.getProperties());
                    properties.set(i,rangeDimensionRange);
                    input2.addFirst(new Rule(rule.getAction(),properties,rule.getPriority(),Rule.maxId+1));
                }
            }
        }
        return output;
    }
}
