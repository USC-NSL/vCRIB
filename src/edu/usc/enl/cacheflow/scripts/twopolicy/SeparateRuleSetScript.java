package edu.usc.enl.cacheflow.scripts.twopolicy;

import edu.usc.enl.cacheflow.model.WriterSerializableUtil;
import edu.usc.enl.cacheflow.model.factory.FileFactory;
import edu.usc.enl.cacheflow.model.factory.RuleFactory;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.model.rule.action.AcceptAction;
import edu.usc.enl.cacheflow.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 9/13/12
 * Time: 12:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class SeparateRuleSetScript {
    public static void main(String[] args) {
        int randomSeedIndex = Integer.parseInt(args[0]);
        String ruleFileName = args[1];
        String outputFileName1 = args[2];
        String outputFileName2 = args[3];
        String outputFileName3 = args[4];

        Util.setRandom(randomSeedIndex);

        try {
            //load rule
            HashMap<String, Object> parameters = new HashMap<>();
            List<Rule> rules = Util.loadFile(new RuleFactory(new FileFactory.EndOfFileCondition()), ruleFileName, parameters, new ArrayList<Rule>());
            int initRulesSize = rules.size();

            Rule defaultRule = rules.remove(rules.size() - 1);
            /*List<Rule> forSecondOnly;
            {
                defaultRule = rules.remove(rules.size() - 1);
                List<Rule> subList = rules.subList(rules.size() - 115, rules.size());
                forSecondOnly = new ArrayList<>(subList);
                subList.clear();
            }*/
            Collections.shuffle(rules, Util.random);
            List<Rule> firstHalf;
            int priorityStart = 0;
            {
                List<Rule> firstSubList = rules.subList(0, rules.size() / 2);
                firstHalf = new ArrayList<>(firstSubList);
                firstSubList.clear();
                firstHalf.add(defaultRule);
                Collections.sort(firstHalf, Rule.PRIORITY_COMPARATOR);
                for (Rule rule : firstHalf) {
                    rule.setPriority(priorityStart++);
                    rule.setId(priorityStart);
                }

                File file1 = new File(outputFileName1);
                file1.getParentFile().mkdirs();
                WriterSerializableUtil.writeFile(firstHalf, file1, false, parameters);
            }

            {
                //reorganize priorities
                int denyPriority = 100;
                int acceptPriority = 1000;


                //rules.addAll(forSecondOnly);
                rules.add(new Rule(defaultRule.getAction(), defaultRule.getProperties(), defaultRule.getPriority(), 0));
                Collections.sort(rules, Rule.PRIORITY_COMPARATOR);
                for (Rule rule : rules) {
                    rule.setPriority(priorityStart++);
                    //rule.setAction(AcceptAction.getInstance());
                    rule.setId(priorityStart);
                }
                //defaultRule.setPriority(initRulesSize);
                Collections.sort(rules, Rule.PRIORITY_COMPARATOR);

                File file2 = new File(outputFileName2);
                file2.getParentFile().mkdirs();
                WriterSerializableUtil.writeFile(rules, file2, false, parameters);
            }
            {
                List<Rule> jointRules = new ArrayList<>(firstHalf);
                jointRules.addAll(rules);
                int id = 1;
                for (Rule jointRule : jointRules) {
                    jointRule.setId(id++);
                }
                File file3 = new File(outputFileName3);
                file3.getParentFile().mkdirs();
                WriterSerializableUtil.writeFile(jointRules, file3, false, parameters);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
