package edu.usc.enl.cacheflow.processor.rule.aggregator;

import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.Processor;

import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/11/11
 * Time: 8:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class MinimumAggregator extends Aggregator {
    private final List<Aggregator> aggregators;

    public MinimumAggregator(Collection<Rule> input, List<Aggregator> aggregators) {
        super(input);
        this.aggregators = aggregators;
    }

    public MinimumAggregator(Processor<?, Collection<Rule>> processorInput, List<Aggregator> aggregators) {
        super(processorInput);
        this.aggregators = aggregators;
    }


    @Override
    public Collection<Rule> process(Collection<Rule> input) throws Exception {
        Collection<Rule> minimumRuleList = null;
        for (Processor<Collection<Rule>, Collection<Rule>> listProcessor : aggregators) {
            listProcessor.setTailInput(input);
            Collection<Rule> processOutput = listProcessor.run();
            if (minimumRuleList == null) {
                minimumRuleList = processOutput;
            } else if (minimumRuleList.size() > processOutput.size()) {
                minimumRuleList = processOutput;
            }
        }
        return minimumRuleList;
    }
}
