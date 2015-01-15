package edu.usc.enl.cacheflow.processor.flow.classifier;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/6/12
 * Time: 11:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class LinearMatchTrafficProcessor implements RuleClassifier {
    private List<Rule> rules;

    public LinearMatchTrafficProcessor() {
    }

    public LinearMatchTrafficProcessor(Collection<Rule> rules) {
        setRules(rules);
    }

    public Map<Rule, Collection<Flow>> classify(Collection<Flow> flows, Collection<Rule> rules) {
        return Util.CalculateRuleTrafficMap(flows, rules);
    }

    public Rule classify(Flow flow) {
        for (Rule rule : rules) {
            if (rule.match(flow)) {
                return rule;
            }
        }
        return null;
    }

    public void setRules(Collection<Rule> rules) {
        if (rules instanceof ArrayList) {
            this.rules = (List<Rule>) rules;
        } else {
            this.rules = new ArrayList<Rule>(rules);
        }
        Collections.sort(this.rules, Rule.PRIORITY_COMPARATOR);
    }

    public RuleClassifier cloneNew() {
        return new LinearMatchTrafficProcessor();
    }

}
