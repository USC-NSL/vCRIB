package edu.usc.enl.cacheflow.algorithms.partition.decisiontree;

import edu.usc.enl.cacheflow.model.Flow;
import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 11/27/11
 * Time: 11:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class PartitionTree2 {
    List<DimensionInfo> permutation;
    List<Integer> permutationIndex;
    private TreeNodeObject rootNode;

    List<RangeDimensionRange> defaultRanges;
    public static int mergeWithChild = 0;

    private long time;
    private List<Set<Rule>> dimRule;
    private boolean saveOnlyMinRule = false;
    private boolean actionAggregate = true;

    public PartitionTree2() {
        this(false, true);
    }

    public PartitionTree2(boolean saveOnlyMinRule, boolean actionAggregate) {
        this.saveOnlyMinRule = saveOnlyMinRule;
        this.actionAggregate = actionAggregate;
    }

    public void semigridAndMerge(Collection<Rule> rules, List<DimensionInfo> dimensionInfos, List<Integer> permutation) {
        semigridAndMerge(rules, dimensionInfos, permutation, permutation.size());
    }

    public void semigridAndMerge(Collection<Rule> rules, List<DimensionInfo> dimensionInfos, List<Integer> permutation, int maxLevel) {
        time = System.currentTimeMillis();
        //System.out.println("Start");
        semigrid(rules, dimensionInfos, permutation, maxLevel);
        //System.out.println("merging");
        mergeTree((InternalTreeNodeObject) rootNode, true);
        //System.out.println("tree merge done");
    }

    public void semigrid(Collection<Rule> rules, List<DimensionInfo> dimensionInfos, List<Integer> permutation) {
        semigrid(rules, dimensionInfos, permutation, permutation.size());
    }

    public void semigrid(Collection<Rule> rules, List<DimensionInfo> dimensionInfos, List<Integer> permutation, int maxLevel) {
        setPermutation(dimensionInfos, permutation);
        rootNode = new InternalTreeNodeObject(null, -1, this);
        createTreeforDimensionSemiGrid(rules, 0, (InternalTreeNodeObject) rootNode, maxLevel);
    }

    public void grid0(Collection<Rule> rules, List<DimensionInfo> dimensionInfos, List<Integer> permutation) {
        setPermutation(dimensionInfos, permutation);
        rootNode = new InternalTreeNodeObject(null, -1, this);
        List<List<RangeDimensionRange>> predefinedRanges = new LinkedList<List<RangeDimensionRange>>();
        for (Integer treeLevel : permutation) {
            predefinedRanges.add(findRanges(rules, Util.getDimensionInfoIndex(dimensionInfos.get(treeLevel))));
        }

        createTreeforDimensionGrid(rules, 0, (InternalTreeNodeObject) rootNode, predefinedRanges);
    }

    public void grid0AndMerge(Collection<Rule> rules, List<DimensionInfo> dimensionInfos, List<Integer> permutation) {
        grid0(rules, dimensionInfos, permutation);
        mergeTree((InternalTreeNodeObject) rootNode, true);
    }

    public void semigridAndMergeTogether(Collection<Rule> rules, List<DimensionInfo> dimensionInfos, List<Integer> permutation) {
        setPermutation(dimensionInfos, permutation);
        createDimRules(rules.size(), permutation);

        rootNode = new InternalTreeNodeObject(null, -1, this);
        rootNode = integratedChildCreationAndMerge(rules, 0, (InternalTreeNodeObject) rootNode);
    }

    public void createDimRules(int ruleSize, List<Integer> permutation) {
        dimRule = new ArrayList<Set<Rule>>(permutation.size());
        int setNumbers = permutation.size() - 1;
        if (saveOnlyMinRule) {
            setNumbers++;
        }
        for (int i = 0; i < setNumbers; i++) {
            dimRule.add(new HashSet<Rule>(ruleSize, 1));
        }
    }

    public void setPermutation(List<DimensionInfo> dimensionInfos, List<Integer> permutation) {
        this.permutation = new ArrayList<DimensionInfo>(permutation.size());
        for (Integer index : permutation) {
            this.permutation.add(dimensionInfos.get(index));
        }

        defaultRanges = new ArrayList<RangeDimensionRange>(permutation.size());
        for (DimensionInfo info : this.permutation) {
            defaultRanges.add(info.getDimensionRange());
        }
        permutationIndex = new ArrayList<Integer>(permutation.size());
        for (DimensionInfo info : this.permutation) {
            permutationIndex.add(Util.getDimensionInfoIndex(info));
        }
    }

    public List<RangeDimensionRange> getDefaultRanges() {
        return defaultRanges;
    }

    public static List<Integer> findPermutation(Collection<Rule> rules, List<DimensionInfo> dimensionsName) {
        List<tuple> edgesDimension = new ArrayList<tuple>(dimensionsName.size());
        int i = 0;
        for (DimensionInfo dimensionName : dimensionsName) {
            List<RangeDimensionRange> childrenRanges = findRanges(rules, i);
            edgesDimension.add(new tuple(childrenRanges.size(), i));
            i++;
        }

        Collections.sort(edgesDimension);

        List<Integer> outputPermutation = new ArrayList<Integer>(dimensionsName.size());
        for (tuple tuple : edgesDimension) {
            outputPermutation.add((Integer) tuple.s);
        }
//        if (edgesDimension.get(0).i<2){
//            outputPermutation.add(outputPermutation.remove(0));
//        }
        //       Collections.reverse(outputPermutation);
        return outputPermutation;
    }

    private static class tuple implements Comparable<tuple> {
        int i;
        Object s;

        private tuple(int i, Object s) {
            this.i = i;
            this.s = s;
        }

        public int compareTo(tuple o) {
            return i - o.i;
        }
    }

    private void createTreeforDimensionGrid(Collection<Rule> applicableRules, int treeLevel, InternalTreeNodeObject treeNode, List<List<RangeDimensionRange>> predefinedRanges) {
        int dimensionNameIndex = permutationIndex.get(treeLevel);
        List<RangeDimensionRange> childrenRanges = predefinedRanges.get(treeLevel);
        for (RangeDimensionRange childRange : childrenRanges) {
            //find applicable rules
            Set<Rule> childApplicableRules = getMatchedRules2(applicableRules, dimensionNameIndex, treeLevel, childRange);
            if (childApplicableRules.size() == 0) {//no applicable rule!
                continue;
            }

            //go to next level
            if (treeLevel < permutation.size() - 1) {
                //create child node
                InternalTreeNodeObject childNode = new InternalTreeNodeObject(childRange, treeLevel, this);
                treeNode.add(childNode);
                createTreeforDimensionGrid(childApplicableRules, treeLevel + 1, childNode, predefinedRanges);
            } else {
                //its a leaf node
                TreeNodeObject childNode = createLeaf(treeLevel, childRange, childApplicableRules);
                treeNode.add(childNode);
            }
        }
    }

    private TreeNodeObject createLeaf(int treeLevel, RangeDimensionRange childRange, Set<Rule> childApplicableRules) {
        if (saveOnlyMinRule) {
            if (actionAggregate) {
                return new OneRuleLeafTreeNodeObject(childRange, childApplicableRules, treeLevel, this);
            } else {
                return new OneRuleLeafTreeNodeObjectRuleMerge(childRange, childApplicableRules, treeLevel, this);
            }
        } else {
            return new MultiRuleLeafTreeNodeObject(childRange, childApplicableRules, treeLevel, this);
        }
    }

    private void createTreeforDimensionSemiGrid(Collection<Rule> applicableRules, int treeLevel, InternalTreeNodeObject treeNode, int maxLevel) {
        int dimensionNameIndex = permutationIndex.get(treeLevel);
        List<RangeDimensionRange> childrenRanges = findRanges(applicableRules, dimensionNameIndex);
        //System.out.println("dname"+dimensionName);
        //System.out.println(childrenRanges.size()+" ranges");
        for (RangeDimensionRange childRange : childrenRanges) {
            //find applicable rules
            Set<Rule> childApplicableRules = getMatchedRules2(applicableRules, dimensionNameIndex, treeLevel, childRange);
            if (childApplicableRules.size() == 0) {//no applicable rule!
                continue;
            }

            //go to next level
            if (treeLevel < maxLevel - 1) {
                //create child node
                InternalTreeNodeObject childNode = new InternalTreeNodeObject(childRange, treeLevel, this);
                treeNode.add(childNode);
                createTreeforDimensionSemiGrid(childApplicableRules, treeLevel + 1, childNode, maxLevel);
            } else {
                //its a leaf node
                //create child node
                TreeNodeObject childNode = createLeaf(treeLevel, childRange, childApplicableRules);
                treeNode.add(childNode);
            }
        }
    }


    Set<Rule> getMatchedRules2(Collection<Rule> applicableRules, int dim, int treeLevel, RangeDimensionRange childrenRange) {
        Set<Rule> childApplicableRules;
        if (treeLevel == permutation.size() - 1 && !saveOnlyMinRule) {//last level
            childApplicableRules = new HashSet<Rule>();
        } else {
            childApplicableRules = dimRule.get(treeLevel);
            childApplicableRules.clear();
        }
        //final int dim = Util.getDimensionInfoIndex(dimensionName);
        for (Rule applicableRule : applicableRules) {
            if (applicableRule.getProperty(dim).hasIntersect(childrenRange)) {
                childApplicableRules.add(applicableRule);
            }
        }
        return childApplicableRules;
    }

    public static Set<Rule> getMatchedRules(Collection<Rule> applicableRules, int dim, RangeDimensionRange childrenRange) {
        Set<Rule> childApplicableRules = new HashSet<Rule>();
        //final int dim = Util.getDimensionInfoIndex(dimensionName);
        for (Rule applicableRule : applicableRules) {
            if (applicableRule.getProperty(dim) == null) {
                throw new RuntimeException("Null");
            }
            if (applicableRule.getProperty(dim).hasIntersect(childrenRange)) {
                childApplicableRules.add(applicableRule);
            }
        }
        return childApplicableRules;
    }

    public TreeNodeObject getNode(Long[] properties, int level) {
        TreeNodeObject matchChildren = rootNode.getMatchChildren(properties);
        while (!matchChildren.isLeaf() && matchChildren.getLevel() < level) {
            matchChildren = matchChildren.getMatchChildren(properties);
        }
        return matchChildren;
    }

    public Collection<Rule> getRules(Flow flow) {
        final TreeNodeObject node = getNode(flow.getProperties(), permutation.size());
        return ((MultiRuleLeafTreeNodeObject) node).getRules();
    }

    public Rule getRule(Flow flow, boolean exactRule) {
        final TreeNodeObject matchChildren = getNode(flow.getProperties(), permutation.size());
        if (exactRule) {
            return ((MultiRuleLeafTreeNodeObject) matchChildren).getExactRule(flow);
        }
        return ((LeafTreeNodeObject) matchChildren).getMinRule();
    }


    /**
     * integrates semigrid and merge to decrease memory usage
     *
     * @param applicableRules
     * @param treeLevel
     * @param treeNode
     */
    private TreeNodeObject integratedChildCreationAndMerge(Collection<Rule> applicableRules, int treeLevel, InternalTreeNodeObject treeNode) {
        int dimensionNameIndex = permutationIndex.get(treeLevel);
        List<RangeDimensionRange> childrenRanges = findRanges(applicableRules, dimensionNameIndex);

        TreeNodeObject current = null;
        int r = 0;
        for (RangeDimensionRange childRange : childrenRanges) {
            //find applicable rules
            Set<Rule> childApplicableRules = getMatchedRules2(applicableRules, dimensionNameIndex, treeLevel, childRange);
            if (childApplicableRules.size() == 0) {//no applicable rule!
                throw new RuntimeException("Invalid state");
                //continue;
            }
            TreeNodeObject childNode;
            if (childApplicableRules.size() == 1 || treeLevel == permutation.size() - 1) {
                if (current == null || !current.isLeaf() ||
                        !((LeafTreeNodeObject) current).aggregate(childRange, childApplicableRules)) {
                    current = createLeaf(treeLevel, childRange, childApplicableRules);
                    treeNode.add(current);
                }
            } else {
                childNode = new InternalTreeNodeObject(childRange, treeLevel, this);
                childNode = integratedChildCreationAndMerge(childApplicableRules, treeLevel + 1, (InternalTreeNodeObject) childNode);
                if (current == null || !current.aggregate(childNode)) {
                    current = childNode;
                    treeNode.add(current);
                }
            }
        }
        //check if it has only one child merge it
        if (treeNode.getChildCount() == 1) {

            mergeWithChild++;
            TreeNodeObject child = treeNode.getFirst();
            //put the child in place of me! it is hard to put child instead of me in parent so
            //load child info into me! no child can be a leaf! :(( lets find myself in parent!
            //parent is not set yet. Good I will return the child instead of myself!
            child.level = treeNode.level;//boost up the child
            child.range = treeNode.getRange();
            return child;
        }
        return treeNode;
    }

    public static int getLeafAgg() {
        return LeafTreeNodeObject.aggregate;
    }

    public static int getIntAgg() {
        return InternalTreeNodeObject.aggregate;
    }

    private TreeNodeObject mergeTree(InternalTreeNodeObject treeNode, boolean deep) {
        boolean parentOfLeaves = true;
        {
            Iterator<TreeNodeObject> childEnum = treeNode.getChildren();
            while (childEnum.hasNext()) {
                TreeNodeObject child = childEnum.next();
                if (!child.isLeaf()) {
                    parentOfLeaves = false;
                    break;
                }
            }
        }
        if (parentOfLeaves || !deep) {
            //if (parentOfLeaves) {
            List<TreeNodeObject> newChildren = new ArrayList<TreeNodeObject>(treeNode.getChildCount());
            boolean anyMerge = false;
            {
                final Iterator<TreeNodeObject> children = treeNode.getChildren();
                TreeNodeObject current = children.next();

                newChildren.add(current);

                int currentI = 0;

                int i = 1;
                while (children.hasNext()) {

                    TreeNodeObject otherChild = children.next();
                    if (current.aggregate(otherChild)) {
                        anyMerge = true;
                    } else {
                        current = otherChild;
                        newChildren.add(current);
                    }
                    i++;
                }
            }

            {
                if (anyMerge) {
                    treeNode.removeAllChildren();
                    for (TreeNodeObject newChild : newChildren) {
                        treeNode.add(newChild);
                    }
                }
                //if only one child, merge it with myself
                if (treeNode.getChildCount() == 1) {
                    TreeNodeObject child = treeNode.getFirst();
                    child.level = treeNode.level;
                    return child;
                }
                return treeNode;
            }
        } else {
            //internal node
            int i = 1;
            Iterator<TreeNodeObject> childEnum2 = treeNode.getChildren();
            while (childEnum2.hasNext()) {
                TreeNodeObject child = childEnum2.next();
                TreeNodeObject child2 = mergeTree((InternalTreeNodeObject) child, true);//I am not parent of leaves so can do this.
                if (child2 != child) {
                    treeNode.add(child2);//replace
                }
                i = i + 1;
            }
            return mergeTree(treeNode, false);
        }
    }

    public List<Rule> getRules() {
        List<Rule> outputRules = new LinkedList<Rule>();
        Iterator<TreeNodeObject> e = rootNode.depthFirstEnumeration();
        while (e.hasNext()) {
            TreeNodeObject treeNode = e.next();
            if (treeNode.isLeaf()) {
                outputRules.add(((LeafTreeNodeObject) treeNode).generateRule());
            }
        }
        return outputRules;
    }

    private void recursiveRunAction(ActionOnLeaf actionOnLeaf, List<RangeDimensionRange> properties, InternalTreeNodeObject node) {
        final Iterator<TreeNodeObject> children = node.getChildren();
        int childLevel = -1;//if no children so it is a leaf
        while (children.hasNext()) {
            TreeNodeObject child = children.next();
            childLevel = child.getLevel();
            properties.set(permutationIndex.get(childLevel), child.getRange());
            if (child instanceof LeafTreeNodeObject) {
                actionOnLeaf.doAction(properties, (LeafTreeNodeObject) child);
            } else {
                recursiveRunAction(actionOnLeaf, properties, (InternalTreeNodeObject) child);
            }
        }
        properties.set(permutationIndex.get(childLevel), defaultRanges.get(childLevel));// revert what my children changed
    }

    public void runActionOnTree(ActionOnLeaf actionOnLeaf) {
        runActionOnTree(rootNode, actionOnLeaf);
    }

    public void runActionOnTree(TreeNodeObject rootNode, ActionOnLeaf actionOnLeaf) {
        List<RangeDimensionRange> properties = new ArrayList<RangeDimensionRange>(defaultRanges);
        int i=0;
        for (RangeDimensionRange defaultRange : defaultRanges) {
            properties.set(permutationIndex.get(i),defaultRange);
            i++;
        }
        if (rootNode instanceof LeafTreeNodeObject) {
            actionOnLeaf.doAction(properties, (LeafTreeNodeObject) rootNode);
        } else {
            recursiveRunAction(actionOnLeaf, properties, (InternalTreeNodeObject) rootNode);
        }
    }

    public static List<RangeDimensionRange> findRanges(Collection<Rule> rules, int dimensionIndex) {
        List<RangeDimensionRange> ranges = new LinkedList<RangeDimensionRange>();
        for (Rule rule : rules) {
            ranges.add(rule.getProperty(dimensionIndex));
        }
        return RangeDimensionRange.grid(ranges);
    }

    public static interface ActionOnLeaf {
        public void doAction(List<RangeDimensionRange> properties, LeafTreeNodeObject node);
    }

}






