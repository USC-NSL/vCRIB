package edu.usc.enl.cacheflow.processor.rule.generator;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.model.rule.action.Action;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.processor.rule.aggregator.RemoveCoveredRulesProcessor;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/20/11
 * Time: 2:16 PM
 */
public class ClassBenchRuleGenerator {

    public static Collection<Rule> generate(List<String> input, Random random) {

        List<Rule> rules = new ArrayList<Rule>(input.size() + 1);
        String NotInIPPattern = "[^0-9\\./]";
        Action[] actions = new Action[]{DenyAction.getInstance(), AcceptAction.getInstance()};
        int priority = 0;
        for (String line : input) {
            List<RangeDimensionRange> ranges = new ArrayList<RangeDimensionRange>(5);
            String[] fields = line.split("\t");

            //field 1 and 2 are IPs
            ranges.add(Util.SRC_IP_INFO.parseRange(fields[0].replaceAll(NotInIPPattern, "")));
            ranges.add(Util.DST_IP_INFO.parseRange(fields[1].replaceAll(NotInIPPattern, "")));
            //fields  3 and 4 are ports
            ranges.add(Util.SRC_PORT_INFO.parseRange(fields[2]));
            ranges.add(Util.DST_PORT_INFO.parseRange(fields[3]));
            ranges.add(Util.PROTOCOL_INFO.parseRange(fields[4]));
            rules.add(new Rule(actions[random.nextInt(2)], ranges, priority++,Rule.maxId+1));
        }

        //default rule
        List<RangeDimensionRange> ranges = new ArrayList<RangeDimensionRange>(5);
        ranges.add(Util.SRC_IP_INFO.getDimensionRange());
        ranges.add(Util.DST_IP_INFO.getDimensionRange());
        ranges.add(Util.SRC_PORT_INFO.getDimensionRange());
        ranges.add(Util.DST_PORT_INFO.getDimensionRange());
        ranges.add(Util.PROTOCOL_INFO.getDimensionRange());
        rules.add(new Rule(DenyAction.getInstance(), ranges, rules.size() + 10,Rule.maxId+1));
        try {
            rules = new ArrayList<Rule>(new RemoveCoveredRulesProcessor(rules).run());//to remove rules that are generated based on additional header fields
        } catch (Exception e) {
            e.printStackTrace();
        }

        //reset actions
        for (Rule rule : rules) {
            rule.setAction(random.nextDouble() >= 0.5 ? DenyAction.getInstance() : AcceptAction.getInstance());
        }


//        //start from the least priority action,
//        //if there is a rule that contains this rule and has lower priority, my action should be different
//        final ListIterator<Rule> iter1 = rules.listIterator(rules.size());
//        while (iter1.hasPrevious()) {
//            Rule rule1 = iter1.previous();
//            final ListIterator<Rule> iter2 = rules.listIterator(iter1.nextIndex()+1);
//            while (iter2.hasNext()) {
//                //if iter1 contains iter2 set iter2 action different than iter1
//                Rule rule2 = iter2.next();
//                if (RangeDimensionRange.covers(rule2.getProperties(), rule1.getProperties())) {
//                    if (rule2.getAction() instanceof DenyAction){
//                        rule1.setAction(AcceptAction.getInstance());
//                    }else{
//                        rule1.setAction(DenyAction.getInstance());
//                    }
//                    break;
//                }
//            }
//        }

        return rules;
    }
}
