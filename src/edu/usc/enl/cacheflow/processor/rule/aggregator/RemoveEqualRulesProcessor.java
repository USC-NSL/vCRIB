package edu.usc.enl.cacheflow.processor.rule.aggregator;

import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.Processor;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 4/25/12
 * Time: 6:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class RemoveEqualRulesProcessor extends Aggregator {
    public RemoveEqualRulesProcessor(List<Rule> input) {
        super(input);
    }

    public RemoveEqualRulesProcessor(Processor<?, Collection<Rule>> processorInput) {
        super(processorInput);
    }

    @Override
    public Collection<Rule> process(Collection<Rule> input) throws Exception {
        Map<Integer, Object> rules = new HashMap<Integer, Object>();
        List<Rule> outputRules = new ArrayList<Rule>(input.size());
        for (Rule rule : input) {
            final Object oldRule = rules.get(rule.hashCode());
            if (oldRule != null) {//I have a/some rule(s) with this hashcode
                if (oldRule instanceof Rule) {//there is only one rule there
                    final Rule oldRule1 = (Rule) oldRule;
                    if (oldRule1.getAction().equals(rule.getAction()) && oldRule1.equalProperties(rule)) {//same rule
                        if (rule.getPriority() < oldRule1.getPriority()) {
                            rules.put(rule.hashCode(), rule);
                        } else {
                            //System.out.println(rule + " removed because of " + oldRule1);
                        }
                    } else {
                        List<Rule> collisionList = new LinkedList<Rule>();
                        collisionList.add(rule);
                        collisionList.add(oldRule1);
                        rules.put(rule.hashCode(), collisionList);
                    }
                } else {
                    //there is a collision list
                    List<Rule> collisionList = (List<Rule>) oldRule;
                    boolean found = false;
                    for (int i = 0, collisionListSize = collisionList.size(); i < collisionListSize; i++) {
                        Rule rule1 = collisionList.get(i);
                        if (rule1.getAction().equals(rule.getAction()) && rule1.equalProperties(rule)) {//same rule
                            if (rule.getPriority() < rule1.getPriority()) {
                                collisionList.set(i, rule);
                                found = true;
                                //System.out.println(rule + " removed because of " + rule1);
                                break;
                            }
                        }
                    }
                    if (!found) {//new collision
                        collisionList.add(rule);
                    }
                }
            } else {
                rules.put(rule.hashCode(), rule);
            }
        }

        for (Object o : rules.values()) {
            if (o instanceof Rule) {
                outputRules.add((Rule) o);
            } else {
                outputRules.addAll((List<Rule>) o);
            }
        }

        final int removed = input.size() - outputRules.size();
        if (removed > 0) {
            Util.logger.info(removed + " rules removed");
        }
        return outputRules;
    }
}
