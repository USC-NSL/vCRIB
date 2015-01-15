package edu.usc.enl.cacheflow.processor.rule.aggregator;

import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.Processor;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/13/12
 * Time: 12:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class RemoveEqualIDProcessor extends Aggregator {
    public RemoveEqualIDProcessor(List<Rule> input) {
        super(input);
    }

    public RemoveEqualIDProcessor(Processor<?, Collection<Rule>> processorInput) {
        super(processorInput);
    }

    @Override
    public Collection<Rule> process(Collection<Rule> input) throws Exception {
        Map<Integer, Rule> set = new HashMap<Integer, Rule>();
        for (Rule rule : input) {
            set.put(rule.getId(),rule);
        }
        return set.values();
    }
}
