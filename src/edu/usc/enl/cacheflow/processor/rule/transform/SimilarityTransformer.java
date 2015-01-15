package edu.usc.enl.cacheflow.processor.rule.transform;

import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.model.rule.action.DenyAction;
import edu.usc.enl.cacheflow.util.CustomRandomGenerator;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 10/23/12
 * Time: 7:15 AM
 * To change this template use File | Settings | File Templates.
 */
public class SimilarityTransformer {
    private final Random random;

    public SimilarityTransformer(Random random) {
        this.random = random;
    }

    public List<Rule> transform(List<Rule> rules, CustomRandomGenerator<Long> toRemove, CustomRandomGenerator<Long> toAdd,
                               int transformNumber) {

        System.out.println("start");
        Map<Long, Set<Rule>> sizeRule = new TreeMap<>();
        int srcIPIndex = Util.getDimensionInfoIndex(Util.SRC_IP_INFO);
        for (Rule rule : rules) {
            long size = rule.getProperty(srcIPIndex).getSize();
            Set<Rule> rules1 = sizeRule.get(size);
            if (rules1 == null) {
                rules1 = new HashSet<>();
                sizeRule.put(size, rules1);
            }
            rules1.add(rule);
        }
        System.out.println("map created");
        ///////////////////////
        for (int i = 0; i < transformNumber; i++) {
            System.out.println("start "+i);

            //select the size of new rule that want to add
            Long toAddRandom = toAdd.getRandom(random.nextDouble());

            //go through small rules and select them until their total size is equal to the one we want to add
            long sum = 0;
            Rule removedRule = null;
            int failure =0;
            while (sum < toAddRandom) {
                Long toRemoveRandom = toRemove.getRandom(random.nextDouble());
                Set<Rule> rules1 = sizeRule.get(toRemoveRandom);
                if (rules1 != null && rules1.size() > 0) {
                    Iterator<Rule> itr = rules1.iterator();
                    Rule next = itr.next();
                    sum += next.getProperty(srcIPIndex).getSize();
                    System.out.println(sum);
                    //remove them from their category
                    itr.remove();
                    removedRule = next;
                }else{
                    failure++;
                }
            }

            //try to add the rule in the same place as removed ones
            Set<Rule> rules1 = sizeRule.get(toAddRandom);
            if (rules1 == null) {
                rules1 = new HashSet<>();
                sizeRule.put(toAddRandom, rules1);
            }
            List<RangeDimensionRange> properties = new ArrayList<>(Util.getDimensionInfos().size());
            long srcIPStart = (long) (random.nextDouble() * Util.SRC_IP_INFO.getDimensionRange().getSize() / toAddRandom) * toAddRandom;
            properties.add(new RangeDimensionRange(srcIPStart, srcIPStart + toAddRandom - 1, Util.getDimensionInfos().get(srcIPIndex)));
            properties.add(removedRule.getProperty(1));
            properties.add(removedRule.getProperty(2));
            properties.add(removedRule.getProperty(3));
            properties.add(removedRule.getProperty(4));
            rules1.add(new Rule(random.nextBoolean() ? AcceptAction.getInstance() : DenyAction.getInstance(),
                    properties, (int) (random.nextDouble() * 10000), rules.size() + i));
        }
        System.out.println("create output");

        //create output;
        List<Rule> output = new LinkedList<>();
        for (Set<Rule> ruleSet : sizeRule.values()) {
            for (Rule rule : ruleSet) {
                output.add(rule);
            }
        }
        return output;
    }
}
