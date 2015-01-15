package edu.usc.enl.cacheflow.processor.rule.aggregator.patch;

import edu.usc.enl.cacheflow.model.exceptions.InvalidPatchException;
import edu.usc.enl.cacheflow.model.rule.Rule;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 9/22/11
 * Time: 3:13 AM
 * To change this template use File | Settings | File Templates.
 */
public class Patch {
    private List<Rule> rules;
    private Rule definition;


    private Patch(Rule definition) {
        rules = new LinkedList<Rule>();
        this.definition=definition;
    }


    public static List<Patch> aggregate(Collection<Rule> rulesList) throws InvalidPatchException {

        //create patches from rules
        Map<Rule, Integer> ruleSeenNum = new HashMap<Rule, Integer>();
        for (Rule rule : rulesList) {
            ruleSeenNum.put(rule, 0);
        }
        List<Patch> patchesList = new LinkedList<Patch>();
        for (Rule rule : rulesList) {
            Integer seenNum = ruleSeenNum.get(rule);
            if (seenNum == 0) {//can be the seed of a patch
                Patch patch = Patch.CreatePatch(Arrays.asList(rule));
                patchesList.add(patch);
                ruleSeenNum.put(rule, seenNum + 1);
                for (Rule otherRule : rulesList) {
                    if (rule != otherRule) {
                        Rule newDefinition = patch.canAggregate(otherRule);
                        //extend patch
                        if (newDefinition != null) {
                            ruleSeenNum.put(otherRule, ruleSeenNum.get(otherRule) + 1);
                            patch.setDefinition(newDefinition);
                        }
                    }
                }
            }
        }



        //merge patches

        boolean merge;
        Map<Patch, Integer> patchSeen = new HashMap<Patch, Integer>();

        do {
            List<Patch> mergedPatchList = new LinkedList<Patch>();

            //init seen numbers
            patchSeen.clear();
            for (Patch patch1 : patchesList) {
                patchSeen.put(patch1, 0);
            }

            merge = false;
            for (Patch patch1 : patchesList) {
                Integer seenNum = patchSeen.get(patch1);
                if (seenNum == 0) {
                    patchSeen.put(patch1, seenNum + 1);
                    for (Patch patch2 : patchesList) {
                        if (patch1 != patch2) {
                            Patch aggregatePatch = patch1.canAggregate(patch2);
                            if (aggregatePatch != null) {
                                patch1 = aggregatePatch;
                                patchSeen.put(patch2, patchSeen.get(patch2) + 1);
                                merge = true;
                            }
                        }
                    }
                    mergedPatchList.add(patch1);
                }
            }
            patchesList = mergedPatchList;

        } while (merge);

        return patchesList;
    }


    public static Patch CreatePatch(List<Rule> rules) throws InvalidPatchException {
        if (rules.size() == 0) {
            throw new InvalidPatchException("No rule to make Patch");
        }
        LinkedList<Rule> toBeMergedRules = new LinkedList<Rule>(rules);
        List<Rule> unMergableRules = new LinkedList<Rule>();

        Rule definition = toBeMergedRules.pop();
        while (true) {
            int startingToBeMerged = toBeMergedRules.size();
            while (toBeMergedRules.size() > 0) {
                Rule rule = toBeMergedRules.pop();
                Rule newDefinition = definition.canAggregate(rule);
                if (newDefinition != null) {
                    definition = newDefinition;
                } else {
                    unMergableRules.add(rule);
                }
            }
            if (unMergableRules.size() == 0) {
                break;
            }
            if (startingToBeMerged == unMergableRules.size()) {
                throw new InvalidPatchException("rules cannot be aggregated");
            } else {
                toBeMergedRules.addAll(unMergableRules);
                unMergableRules.clear();
            }
        }

        Patch patch = new Patch(definition);
        patch.rules.addAll(rules);
        return patch;
    }

    public Rule canAggregate(Rule rule) {
        return definition.canAggregate(rule);
    }

    public void setDefinition(Rule definition) {
        this.definition = definition;
    }

    @Override
    public String toString() {
        return definition.toString();
    }

    public Patch canAggregate(Patch p) {
        Rule newPatchDefinition = canAggregate(p.definition);
        if (newPatchDefinition != null) {
            Patch patch = new Patch(newPatchDefinition);
            patch.rules.addAll(this.rules);
            patch.rules.addAll(p.rules);
            return patch;
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Patch patch = (Patch) o;

        if (definition != null ? !definition.equals(patch.definition) : patch.definition != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return definition != null ? definition.hashCode() : 0;
    }

    public Rule getDefinition() {
        return definition;
    }

    public boolean contains(Rule rule) {
        return rules.contains(rule);
    }

    public List<Rule> getRules() {
        return rules;
    }
}
