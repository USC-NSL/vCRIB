package edu.usc.enl.cacheflow.processor.rule.aggregator.patch;

import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.processor.Processor;
import edu.usc.enl.cacheflow.processor.rule.aggregator.Aggregator;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/11/11
 * Time: 11:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class PatchMergeProcessor extends Aggregator{
    public PatchMergeProcessor(Collection<Rule> input) {
        super(input);
    }

    public PatchMergeProcessor(Processor<?, Collection<Rule>> processorInput) {
        super(processorInput);
    }

    @Override
    public Collection<Rule> process(Collection<Rule> rules) throws Exception {
        List<Patch> patchList = Patch.aggregate(rules);
        List<Rule> patchRules = new LinkedList<Rule>();
        for (Patch patch : patchList) {
            patchRules.add(patch.getDefinition());
        }
        return patchRules;
    }
}
