package edu.usc.enl.cacheflow.processor.partition;

import edu.usc.enl.cacheflow.model.Statistics;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.MatrixRuleSet;
import edu.usc.enl.cacheflow.model.rule.Partition;
import edu.usc.enl.cacheflow.model.rule.Rule;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/13/12
 * Time: 2:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class UnifiedPartitionWriter {
    private final int ruleStartID;
    private final boolean dontMerge;

    public UnifiedPartitionWriter() {
        this(1, false);
    }

    public UnifiedPartitionWriter(int ruleStartID) {
        this(ruleStartID, false);
    }

    public UnifiedPartitionWriter(boolean dontMerge) {
        this(1, dontMerge);
    }

    private UnifiedPartitionWriter(int ruleStartID, boolean dontMerge) {
        this.ruleStartID = ruleStartID;
        this.dontMerge = dontMerge;
    }

    //IF YOU MAKE COPY OF RULES IN EACH PARTITION CONSOLIDATE IT BEFORE USING THIS METHOD
    public int write(Collection<Partition> partitions, PrintWriter writer, Map<String, Object> parameters) throws IOException {
        //write rules
        //Consolidate ids
        Set<Rule> idRules = new HashSet<Rule>(partitions.size());
        for (Partition partition : partitions) {
            for (Rule rule : partition.getRules()) {
                idRules.add(rule);
            }
        }
        final Map<Integer, Integer> idMap = new HashMap<>(idRules.size());
        int id = ruleStartID;
        if (!dontMerge) {
            for (Rule idRule : idRules) {
                idMap.put(idRule.getId(),id++);
            }
        }else{
            for (Rule idRule : idRules) {
                idMap.put(idRule.getId(),idRule.getId());
            }
        }
        /*int id = 1;
        for (Map.Entry<Integer, Rule> entry : idRules.entrySet()) {
            entry.getValue().setId(id++);
        }*/
        List<Rule> rules = new ArrayList<Rule>(idRules);
        idRules.clear();

        Collections.sort(rules, new Comparator<Rule>() {
            @Override
            public int compare(Rule o1, Rule o2) {
                return idMap.get(o1.getId())-idMap.get(o2.getId());
            }
        });
        if (partitions.iterator().next().getRules() instanceof MatrixRuleSet) {
            throw new RuntimeException("MatrixRuleSet rules cannot be unified");
        }
        //
        writer.println(Statistics.getParameterLine(parameters));
        rules.iterator().next().headerToString(writer);
        for (Rule rule : rules) {
            int oldID = rule.getId();
            rule.setId(idMap.get(oldID));
            rule.toString(writer);
            rule.setId(oldID);
        }
        writer.println();
        int i = 0;
        for (Partition partition : partitions) {
            RangeDimensionRange.toString(writer, partition.getProperties());
            writer.println();
            boolean first = true;
            for (Rule rule : partition.getRules()) {
                if (!first) {
                    writer.print(",");
                }
                first = false;
                writer.print(idMap.get(rule.getId()));
            }
            if (i < partitions.size() - 1) {
                writer.println();
            }
        }
        return id;
    }
}
