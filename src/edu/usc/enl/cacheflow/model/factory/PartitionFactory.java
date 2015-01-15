package edu.usc.enl.cacheflow.model.factory;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/8/12
 * Time: 4:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class PartitionFactory extends FileFactory<Partition> {
    private List<DimensionInfo> dimensionInfos;
    private final Collection<Rule> rulesTemplate;

    public PartitionFactory(StopCondition stopCondition, Collection<Rule> rulesTemplate) {
        super(stopCondition);
        this.rulesTemplate = rulesTemplate;
    }

    @Override
    public <C extends Collection<Partition>> C create(BufferedReader reader, Map<String, Object> parameters, C toFill) throws IOException {
        //for header
        final RuleFactory helper = new RuleFactory(new EmptyLineStopCondition());
        helper.parseHeaderLine(reader, parameters);
        dimensionInfos = helper.getDimensionInfos();

        boolean first = true;
        while (!stopCondition.stop(reader)) {
            if (!first) {
                reader.readLine();//skip empty line
            }
            first = false;
            final String partitionDefinition = reader.readLine();
            final List<RangeDimensionRange> properties = RuleFactory.parseRanges(partitionDefinition, dimensionInfos);
            Collection<Rule> rules = Util.getNewCollectionInstance(rulesTemplate);
            helper.createBody(reader, rules);
            toFill.add(new Partition(rules, properties));
        }
        return toFill;
    }

    @Override
    protected Partition create(String s) {
        return null;
    }


    public List<DimensionInfo> getDimensionInfos() {
        return dimensionInfos;
    }
}
