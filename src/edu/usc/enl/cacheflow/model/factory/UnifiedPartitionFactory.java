package edu.usc.enl.cacheflow.model.factory;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/13/12
 * Time: 2:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class UnifiedPartitionFactory extends FileFactory<Partition> {
    private boolean keepRules;
    private List<Rule> rules;
    private int rulesSize;
    private final Collection<Rule> rulesTemplate;

    public UnifiedPartitionFactory(StopCondition stopCondition, boolean keepRules, Collection<Rule> rulesTemplate) {
        super(stopCondition);
        this.keepRules = keepRules;
        this.rulesTemplate = rulesTemplate;
    }

    public UnifiedPartitionFactory(StopCondition stopCondition, Collection<Rule> rulesTemplate) {
        super(stopCondition);
        this.rulesTemplate = rulesTemplate;
    }

    public UnifiedPartitionFactory(StopCondition stopCondition, Collection<Rule> rulesTemplate, List<Rule> rules) {
        super(stopCondition);
        this.rulesTemplate = rulesTemplate;
        this.rules = rules;
    }

    @Override
    protected Partition create(String s) {
        return null;
    }

    @Override
    public <C extends Collection<Partition>> C create(BufferedReader reader, Map<String, Object> parameters, C toFill) throws IOException {
        List<Rule> rules;
        final List<DimensionInfo> dimensionInfos;
        if (this.rules == null) {
            final RuleFactory helper = new RuleFactory(new EmptyLineStopCondition());
            rules = new LinkedList<Rule>();
            helper.create(reader, parameters, rules);
            if (keepRules) {
                this.rules = rules;
            } else {
                this.rules = null;
            }
            if (rulesTemplate instanceof MatrixRuleSet) {
                MatrixRuleSet.setRuleSet(rules);
            }
            dimensionInfos = helper.getDimensionInfos();
        } else {
            rules = this.rules;
            dimensionInfos = Util.getDimensionInfos();
        }
        Map<String, Rule> idRules = new HashMap<String, Rule>();
        {
            for (Rule rule : rules) {
                idRules.put(rule.getId() + "", rule);
            }
        }
        rulesSize = rules.size();
        rules = null;
        reader.readLine();
        int i = 1;
        while (!stopCondition.stop(reader)) {
            final String partitionDefinition = reader.readLine();
            final List<RangeDimensionRange> properties = RuleFactory.parseRanges(partitionDefinition, dimensionInfos);
            StringTokenizer st = new StringTokenizer(reader.readLine(), ",");
            Collection<Rule> partitionRules = Util.getNewCollectionInstance(rulesTemplate);
            while (st.hasMoreTokens()) {
                String id = st.nextToken();
                Rule e = idRules.get(id);
                if (e == null) {
                    System.out.println("Not found rule with id " + id);
                }
                partitionRules.add(e);
            }
            Partition partition = new Partition(partitionRules, properties);
            partition.setId(i++);
            toFill.add(partition);
        }

        return toFill;
    }

    public List<Rule> getRules() {
        return rules;
    }

    public int getRulesSize() {
        return rulesSize;
    }
}
