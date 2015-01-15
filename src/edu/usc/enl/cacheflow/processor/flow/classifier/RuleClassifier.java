package edu.usc.enl.cacheflow.processor.flow.classifier;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.rule.Rule;

import java.util.Collection;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/8/12
 * Time: 11:25 PM
 * To change this template use File | Settings | File Templates.
 */
public interface RuleClassifier {
    /**
     * MAKE THIS THREAD SAFE
     *
     *
     * @param flows
     * @param rules
     * @return
     */
    public Map<Rule, Collection<Flow>> classify(Collection<Flow> flows, Collection<Rule> rules);
    public Rule classify(Flow flow);

    public void setRules(Collection<Rule> rules);

    public RuleClassifier cloneNew();

}
