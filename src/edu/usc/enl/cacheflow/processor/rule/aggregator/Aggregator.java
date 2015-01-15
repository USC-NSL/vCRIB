package edu.usc.enl.cacheflow.processor.rule.aggregator;

import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.Processor;

import java.util.Collection;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/13/12
 * Time: 1:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class Aggregator extends Processor<Collection<Rule>,Collection<Rule>> {
    public Aggregator(Collection<Rule> input) {
        super(input);
    }

    public Aggregator(Processor<?, Collection<Rule>> processorInput) {
        super(processorInput);
    }

    @Override
    public Collection<Rule> process(Collection<Rule> input) throws Exception {
        return input;
    }

    public abstract static class AggregatorFactory{
        public abstract Aggregator getProcessor();
    }
}
