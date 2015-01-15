package edu.usc.enl.cacheflow.processor.rule.aggregator;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.Processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/24/11
 * Time: 7:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class RemoveCoveredRulesProcessor extends Aggregator {
    public RemoveCoveredRulesProcessor(Collection<Rule> input) {
        super(input);
    }

    public RemoveCoveredRulesProcessor(Processor<?, Collection<Rule>> processorInput) {
        super(processorInput);
    }

    @Override
    public Collection<Rule> process(Collection<Rule> input1) throws Exception {
        List<Rule> input;
        if (input1 instanceof List){
            input= (List<Rule>) input1;
        }else{
            input = new ArrayList<Rule>(input1);
        }

        Collections.sort(input, Rule.PRIORITY_COMPARATOR);

        List<RuleSeen> ruleSeens = new ArrayList<RuleSeen>(input.size());
        for (Rule rule : input) {
            ruleSeens.add(new RuleSeen(rule, false));
        }

        List<Rule> outputRuleList = new ArrayList<Rule>();
        int i = 0;
        for (RuleSeen ruleSeen : ruleSeens) {
            if (!ruleSeen.seen) {
                outputRuleList.add(ruleSeen.rule);
                for (int j = i + 1; j < ruleSeens.size(); j++) {
                    RuleSeen ruleSeen2 = ruleSeens.get(j);
                    if (!ruleSeen2.seen) {
                        if (RangeDimensionRange.covers(ruleSeen.rule.getProperties(), ruleSeen2.rule.getProperties())) {
                            //System.out.println(ruleSeen2.rule+" covered by "+ruleSeen.rule);
                            ruleSeen2.seen = true;
                        }
                    }

                }
            }

            i++;
        }
        //System.out.println(input.size()-outputRuleList.size()+" rules removed");



        return outputRuleList;
    }

    private static class RuleSeen {
        Rule rule;
        boolean seen;

        private RuleSeen(Rule rule, boolean seen) {
            this.rule = rule;
            this.seen = seen;
        }
    }
}
