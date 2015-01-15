package edu.usc.enl.cacheflow.algorithms.partition.decisiontree;

import edu.usc.enl.cacheflow.model.dimension.DimensionInfo;
import edu.usc.enl.cacheflow.model.dimension.RangeDimensionRange;
import edu.usc.enl.cacheflow.model.rule.Rule;
import edu.usc.enl.cacheflow.util.Util;

import java.sql.Statement;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 5/25/12
 * Time: 7:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class PersistentPartitionTree2 {

    List<Integer> permutationIndex;
    private Persistanter persistanter;
    private static int nodeID = 1;

    public static int mergeWithChild = 0;

    public final PartitionTree2 pt;
    List<PTreeNodeObject> currentNodes = new ArrayList<PTreeNodeObject>();
    List<PTreeNodeObject> tempNodes = new ArrayList<PTreeNodeObject>();
    public int[] path;

    public PersistentPartitionTree2() {
        this(false);
    }

    public PersistentPartitionTree2(boolean createDB) {
        persistanter = new Persistanter(createDB?5:0);
        pt = new PartitionTree2(true, false);
        if (createDB) {
            persistanter.createDB();
        }
    }

    public void semigridAndMergeTogether(Collection<Rule> rules, List<DimensionInfo> dimensionInfos, List<Integer> permutation) {
        init(rules.size(), dimensionInfos, permutation);

        for (int i = 0; i < permutation.size(); i++) {
            currentNodes.add(new PTreeNodeObject());
            tempNodes.add(new PTreeNodeObject());
        }
        PTreeNodeObject rootNode = new PTreeNodeObject();
        rootNode.init(Util.SRC_PORT_INFO.getDimensionRange(), null, false);
        integratedChildCreationAndMerge(rules, 0, new ArrayList<RangeDimensionRange>(pt.getDefaultRanges()),
                rootNode);
        if (rootNode.isLeaf()) {
            persistanter.save(rootNode.getId(), rootNode.getMinRule(), pt.defaultRanges);
        }
        System.out.println("create finished");
        persistanter.finishSave();

    }

    public void init(int ruleSize, List<DimensionInfo> dimensionInfos, List<Integer> permutation) {
        pt.setPermutation(dimensionInfos, permutation);
        pt.createDimRules(ruleSize, permutation);
        permutationIndex = permutation;
        path = new int[permutation.size() - 1];
        for (int i = 0; i < permutation.size() - 1; i++) {
            path[i] = 0;
        }
    }

    private void integratedChildCreationAndMerge(Collection<Rule> applicableRules, int treeLevel, List<RangeDimensionRange> ranges,
                                                 PTreeNodeObject treeNode) {
        int dimensionNameIndex = permutationIndex.get(treeLevel);
        List<RangeDimensionRange> childrenRanges = PartitionTree2.findRanges(applicableRules, dimensionNameIndex);

        PTreeNodeObject current = currentNodes.get(treeLevel);
        PTreeNodeObject temp = tempNodes.get(treeLevel);
        int childrenCount = 0;
        int i=0;
        for (RangeDimensionRange childRange : childrenRanges) {
            System.out.println(1.0*i/childrenRanges.size()+": "+childRange);

            //find applicable rules
            Set<Rule> childApplicableRules = pt.getMatchedRules2(applicableRules, dimensionNameIndex, treeLevel, childRange);
            if (childApplicableRules.size() == 0) {//no applicable rule!
                throw new RuntimeException("Invalid state");
                //continue;
            }
            if (childApplicableRules.size() == 1 || treeLevel == permutationIndex.size() - 1) {
                if (childrenCount > 0 && current.isLeaf() &&
                        current.getMinRule() == Collections.min(childApplicableRules, Rule.PRIORITY_COMPARATOR).getId()) {
                    current.setRange(current.getRange().canAggregate(childRange));
                    ranges.set(treeLevel, current.getRange());
                } else {
                    //persist current then
                    if (childrenCount > 0 && current.isLeaf()) {
                        persistanter.save(current.getId(), current.getMinRule(), ranges);
                    }
                    current.init(childRange, childApplicableRules, true);
                    ranges.set(treeLevel, current.getRange());
                    childrenCount++;
                }
            } else {
                RangeDimensionRange oldCurrentRange = null;
                if (childrenCount > 0) {
                    oldCurrentRange = current.getRange();
                }
                temp.init(childRange, null, false);
                ranges.set(treeLevel, temp.getRange());//as children need it
                integratedChildCreationAndMerge(childApplicableRules, treeLevel + 1, ranges, temp);
                if (childrenCount == 0) {
                    //first no need for aggregation
                    current.fill(temp);
                    childrenCount++;
                } else if (current.isLeaf() && temp.isLeaf() && current.getMinRule() == temp.getMinRule()) {//aggregate
                    current.setRange(current.getRange().canAggregate(childRange));
                } else {
                    if (current.isLeaf()) {
                        ranges.set(treeLevel, oldCurrentRange);
                        persistanter.save(current.getId(), current.getMinRule(), ranges);
                    }
                    current.fill(temp);
                    childrenCount++;
                }
                ranges.set(treeLevel, current.getRange());//to make sure they are the same

                //TODO cannot do this unless keep the whole subtree and remove it also if aggregate
            }
        }

        //check if it has only one child merge it
        if (childrenCount == 1) {
            mergeWithChild++;

            treeNode.setLeaf(current.isLeaf());
            treeNode.setMinRule(current.getMinRule());
            treeNode.setId(current.getId());
        } else {
            //save current
            if (current.isLeaf()) {
                persistanter.save(current.getId(), current.getMinRule(), ranges);
            }
        }
        ranges.set(treeLevel, pt.getDefaultRanges().get(treeLevel));
    }


    private List<Persistanter.BlockEntry> createBufferForGatherBlock(int size) {
        List<Persistanter.BlockEntry> buffer = new ArrayList<Persistanter.BlockEntry>(size);
        for (int i = 0; i < size; i++) {
            buffer.add(new Persistanter.BlockEntry(0, null));
        }
//        }
        return buffer;
    }

    public Statement getMatchStatementSrc() {
        return persistanter.getNewMatchChildSrc();
    }


    public void runActionOn(SmallPActionOnLeaf<Persistanter.BlockEntry> actionOnLeaf, final List<Integer> ids, final int loadSize) {
        final BlockingQueue<Persistanter.BlockEntry> q = new LinkedBlockingDeque<Persistanter.BlockEntry>();
        new Thread() {
            @Override
            public void run() {
                int offset = 0;
                while (true) {
                    if (offset >= ids.size()) {
                        q.offer(new Persistanter.FinishBlock(0, null));
                        break;
                    }
                    int loadedInBuffer = persistanter.load(ids.subList(offset, Math.min(ids.size(), offset + loadSize)), q, loadSize);
                    System.out.println(loadedInBuffer + " records loaded");
                    offset += loadedInBuffer;
                }
            }
        }.start();

        try {
            while (true) {
                final Persistanter.BlockEntry take = q.take();
                if (take instanceof Persistanter.FinishBlock) {
                    break;
                }
                actionOnLeaf.doAction(take);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void runActionOnLeavesRuleBased2(SmallPActionOnLeaf<Persistanter.BlockEntry> actionOnLeaf, int loadSize) {
        List<Persistanter.BlockEntry> buffer = createBufferForGatherBlock(loadSize);

        int rule = 0;
        while (true) {
            int loadedInBuffer = persistanter.getLeavesRule(buffer, rule, loadSize);
            for (int i = 0; i < loadedInBuffer; i++) {
                actionOnLeaf.doAction(buffer.get(i));
            }
            if (loadedInBuffer < loadSize) {
                break;
            }
            final int newRuleID = buffer.get(loadSize - 1).getRuleID();
            if (newRuleID == rule) {
                while (true) {
                    loadedInBuffer = persistanter.getLeavesSpecificRule(buffer, rule, buffer.get(loadSize - 1).getId(), loadSize);
                    for (int i = 0; i < loadedInBuffer; i++) {
                        actionOnLeaf.doAction(buffer.get(i));
                    }
                    if (loadedInBuffer < loadSize) {
                        break;
                    }
                }
                rule++;
            } else {
                rule = newRuleID;
                actionOnLeaf.emptyBuffer();
            }
        }
    }

    public void close() {
        persistanter.close();
    }

    public void runOnMatchSrc(Long srcIP, List<Persistanter.BlockEntry> buffer,
                              SmallPActionOnLeaf<Persistanter.BlockEntry> actionOnLeaf, Statement statement, int loadSize) {
        //load size is so big that does not need to reload
        int loadedInBuffer = persistanter.getMatchChildSrc(srcIP, buffer, loadSize, statement);
        for (int i = 0; i < loadedInBuffer; i++) {
            boolean toContinue = actionOnLeaf.doAction(buffer.get(i));
            if (!toContinue) {
                return;
            }
        }
        if (loadedInBuffer == 0) {
            System.out.println("Zero tuple match  " + srcIP);
        }
    }

    public static interface SmallPActionOnLeaf<T> {
        public boolean doAction(T node);

        public void emptyBuffer();
    }


    public class PTreeNodeObject {
        private int id = -1;
        private int minRule;
        private boolean leaf;
        private RangeDimensionRange range;


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PTreeNodeObject that = (PTreeNodeObject) o;

            if (id != that.id) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public boolean isLeaf() {
            return leaf;
        }

        public void init(RangeDimensionRange childRange, Set<Rule> childApplicableRules, boolean leaf) {
            this.range = childRange;
            if (childApplicableRules == null) {
                minRule = 0;
            } else {
                this.minRule = Collections.min(childApplicableRules, Rule.PRIORITY_COMPARATOR).getId();
            }
            this.leaf = leaf;
            id = nodeID++;
        }

        public int getMinRule() {
            return minRule;
        }

        public RangeDimensionRange getRange() {
            return range;
        }

        public void fill(PTreeNodeObject current2) {
            leaf = current2.leaf;
            range = current2.range;
            minRule = current2.minRule;
            id = current2.id;
        }

        public void setMinRule(int minRule) {
            this.minRule = minRule;
        }

        public void setRange(RangeDimensionRange rangeDimensionRange) {
            range = rangeDimensionRange;
        }

        public void setLeaf(boolean leaf) {
            this.leaf = leaf;
        }
    }

}
