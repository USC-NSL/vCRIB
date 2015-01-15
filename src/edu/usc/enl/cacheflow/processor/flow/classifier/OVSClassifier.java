package edu.usc.enl.cacheflow.processor.flow.classifier;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.dimension.UnalignedRangeException;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.WildCardPattern;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/6/12
 * Time: 9:11 AM
 * To change this template use File | Settings | File Templates.
 */
public class OVSClassifier implements RuleClassifier {
    private Map<Long, Bucket> buckets = new HashMap<Long, Bucket>();

    public OVSClassifier() {
    }


    public OVSClassifier(Collection<Rule> tempRules) {
        setRules(tempRules);
    }

    public Map<Rule, Collection<Flow>> classify(Collection<Flow> flows, Collection<Rule> rules) {
        Map<Rule, Collection<Flow>> output = new HashMap<Rule, Collection<Flow>>();
        try {
            Map<Long, Bucket> buckets = new HashMap<Long, Bucket>(rules.size() / 10);
            for (Rule rule : rules) {
                final long wc = rule.getWildCardBitPattern();
                Bucket bucket = buckets.get(wc);
                if (bucket == null) {
                    bucket = new Bucket();
                    buckets.put(wc, bucket);
                }
               /* if (rule.getProperty(0).getStart()==2367984801L){
                    System.out.println();
                    long wc2 = rule.getWildCardBitPattern();
                }*/
                bucket.addRule(rule);
            }
            final long[] maskedProperties = new long[Util.getDimensionInfos().size()];
            for (Flow flow : flows) {
                /*if (flow.getProperty(0)==2367984801L){
                    System.out.println();
                }*/
                Rule matchedRule = null;
                for (Map.Entry<Long, Bucket> entry : buckets.entrySet()) {
                    WildCardPattern.mask(flow.getProperties(), entry.getKey(), maskedProperties);
                    final Rule rule = entry.getValue().getRule(maskedProperties);
                    if (matchedRule == null || (rule != null && rule.getPriority() < matchedRule.getPriority())) {
                        matchedRule = rule;
                    }
                }
                if (matchedRule == null) {
                    throw new RuntimeException("No match for flow " + flow);
                }
                Collection<Flow> flows1 = output.get(matchedRule);
                if (flows1 == null) {
                    flows1 = new LinkedList<Flow>();
                    output.put(matchedRule, flows1);
                }
                flows1.add(flow);
            }
        } catch (UnalignedRangeException e) {
            e.printStackTrace();
        }
        return output;
    }

    public void setRules(Collection<Rule> rules) {
        try {
            for (Rule rule : rules) {
                final long wc = rule.getWildCardBitPattern();
                Bucket bucket = buckets.get(wc);
                if (bucket == null) {
                    bucket = new Bucket();
                    buckets.put(wc, bucket);
                }
                bucket.addRule(rule);
            }
        } catch (UnalignedRangeException e) {
            e.printStackTrace();
        }
    }

    public RuleClassifier cloneNew() {
        return new OVSClassifier();
    }

    public Rule classify(Flow flow) {
        try {
            Rule matchedRule = null;
            final long[] maskedProperties = new long[flow.getProperties().length];
            for (Map.Entry<Long, Bucket> entry : buckets.entrySet()) {
                WildCardPattern.mask(flow.getProperties(), entry.getKey(), maskedProperties);
                final Rule rule = entry.getValue().getRule(maskedProperties);
                if (matchedRule == null || (rule != null && rule.getPriority() < matchedRule.getPriority())) {
                    matchedRule = rule;
                }
            }
            if (matchedRule == null) {
                throw new RuntimeException("No match for flow " + flow);
            }

            return matchedRule;
        } catch (UnalignedRangeException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static class Bucket {
        private Map<Integer, Object> rules = new HashMap<Integer, Object>();

        private Rule match(Flow flow) {
            for (Map.Entry<Integer, Object> entry : rules.entrySet()) {
                if (entry.getValue() instanceof Rule) {
                    if (((Rule) entry.getValue()).match(flow)) {
                        return (Rule) entry.getValue();
                    }
                } else {
                    final List<Rule> list = (List<Rule>) entry.getValue();
                    for (Rule rule : list) {
                        if (rule.match(flow)) {
                            return rule;
                        }
                    }
                }

            }
            return null;
        }

        private Rule getRule(long[] properties) {
            final Object o = rules.get(hash(properties));
            if (o == null) {
                return null;
            } else if (o instanceof Rule) {
                return ((Rule) o).match(properties) ? (Rule) o : null;
            }
            //its a list
            Rule matchedRule = null;
            for (Rule rule : (List<Rule>) o) {
                if ((matchedRule == null || matchedRule.getPriority() > rule.getPriority()) && rule.match(properties)) {
                    matchedRule = rule;
                }
            }
            return matchedRule;
        }

        private int hash(long[] properties) {
            int output = 1;
            for (Long property : properties) {
                output = 31 * output + (int) (property ^ (property >>> 32));
            }
            return output;
        }

        private int ruleHashCode(Rule rule) {
            int output = 1;
            for (RangeDimensionRange range : rule.getProperties()) {
                final long start = range.getStart();
                output = 31 * output + (int) (start ^ (start >>> 32));
            }
            return output;
        }

        private void addRule(Rule rule) {
            final int ruleStartHashCode = ruleHashCode(rule);
            final Object oldRule = rules.get(ruleStartHashCode);
            if (oldRule != null) {
                if (oldRule instanceof Rule) {
                    final Rule oldRule1 = (Rule) oldRule;
                    if (!oldRule1.equalProperties(rule) || !oldRule1.getAction().equals(rule.getAction())) {
                        List<Rule> collisionList = new ArrayList<Rule>();
                        collisionList.add(rule);
                        collisionList.add(oldRule1);
                        rules.put(ruleStartHashCode, collisionList);
                    } else {//same rule
                        //Duplicate
                        if (rule.getPriority() < ((Rule) oldRule).getPriority()) {
                            rules.put(ruleStartHashCode, rule);
                        }
                    }
                } else {
                    //there is a collision list
                    List<Rule> collisionList = (List<Rule>) oldRule;
                    boolean found = false;
                    for (int i = 0, collisionListSize = collisionList.size(); i < collisionListSize; i++) {
                        Rule rule1 = collisionList.get(i);
                        if (rule1.equalProperties(rule) && rule1.getAction().equals(rule.getAction())) {//same rule
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
                rules.put(ruleStartHashCode, rule);
            }
        }
    }
}
