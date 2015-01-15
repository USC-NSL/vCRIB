package edu.usc.enl.cacheflow.model.rule;

import edu.usc.enl.cacheflow.util.Util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/9/12
 * Time: 9:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class MatrixRuleSet implements Set<Rule>, Cloneable {
    private long[] matrix;
    private int size;

    private static List<Rule> ruleSet;

    public static void setRuleSet(List<Rule> ruleList) {
        if (ruleSet == null) {
            ruleSet = ruleList;
        } else {
            Util.logger.warning("RuleSet has been set before");
            ruleSet = ruleList;
            //throw new UnsupportedOperationException("RuleSet has been set before");
        }
    }

    public MatrixRuleSet() {
        matrix = new long[getLongNums(ruleSet.size())];
        size = 0;
    }

    public MatrixRuleSet(long[] matrix) {
        this.matrix = matrix;
        size = getSize(matrix);
    }

    public void extend(int newSize) {
        long[] newMatrix = new long[getLongNums(newSize)];
        System.arraycopy(matrix, 0, newMatrix, 0, Math.min(matrix.length, newMatrix.length));
        matrix = newMatrix;
    }

    public static int getLongNums(int rulesNum) {
        return (int) Math.ceil(1.0 * rulesNum / 64);
    }

    public static long[] convertToMatrix(long[] partitionRuleMatrix, Collection<Rule> rules) {
        Arrays.fill(partitionRuleMatrix, 0l);
        for (Rule rule : rules) {
            int longIndex = (rule.getId() - 1) / 64;
            int intraIndex = 63 - (rule.getId() - 1) % 64;
            partitionRuleMatrix[longIndex] |= (1l << intraIndex);
        }
        return partitionRuleMatrix;
    }

    public static void setUnSet(long[] matrix, int index, boolean set) {
        int longIndex = index / 64;
        int intraIndex = 63 - index % 64;
        if (set) {
            matrix[longIndex] |= (1l << intraIndex);
        } else {
            matrix[longIndex] &= ~(1l << intraIndex);
        }
    }

    public static int getSize(long[] union) {
        int unionSize = 0;
        for (long u : union) {
            unionSize += Long.bitCount(u);
        }
        return unionSize;
    }

    public static void union(long[] union, long[] longs) {
        for (int j = 0; j < longs.length; j++) {
            union[j] |= longs[j];
        }
    }

    public static void subtract(long[] union, long[] longs) {
        for (int j = 0; j < longs.length; j++) {
            union[j] &= ~longs[j];
        }
    }

    public static boolean hasOneAt(long[] longs, int i) {
        int longIndex = i / 64;
        int intraIndex = 63 - i % 64;
        if (longIndex>=longs.length){
            System.out.println();
        }
        return (longs[longIndex] & (1l << intraIndex)) != 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof Rule) {
            return hasOneAt(matrix, ((Rule) o).getId() - 1);
        }
        return false;
    }

    @Override
    public MatrixRuleSet clone() {
        MatrixRuleSet output = new MatrixRuleSet();
        System.arraycopy(matrix, 0, output.matrix, 0, matrix.length);
        output.size = size;
        return output;
    }

    @Override
    public boolean add(Rule rule) {
        if (rule == null) {
            return false;
        }
        final int index = rule.getId() - 1;
        boolean alreadyThere = hasOneAt(matrix, index);
        if (!alreadyThere) {
            setUnSet(matrix, index, true);
            size++;
        }
        return !alreadyThere;
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) {
            return false;
        }
        final int index = ((Rule) o).getId() - 1;
        boolean alreadyThere = hasOneAt(matrix, index);
        if (alreadyThere) {
            setUnSet(matrix, index, false);
            size--;
        }
        return alreadyThere;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        if (c instanceof MatrixRuleSet) {
            for (int i = 0; i < matrix.length; i++) {
                long l = matrix[i] | ((MatrixRuleSet) c).matrix[i];
                if (Long.bitCount(l) > Long.bitCount(matrix[i])) {
                    return false;
                }
            }
        } else {
            for (Rule rule : (Collection<? extends Rule>) c) {
                if (rule == null) {
                    return false;
                }
                if (!hasOneAt(matrix, rule.getId() - 1)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    /**
     * Note that it returns always true because of performance
     */
    public boolean addAll(Collection<? extends Rule> c) {
        boolean output = false;
        if (c instanceof MatrixRuleSet) {
            int oldSize = size;
            union(matrix, ((MatrixRuleSet) c).matrix);
            size = getSize(matrix);
            output = size != oldSize;
        } else {
            for (Rule rule : c) {
                output = add(rule) || output;
            }
        }
        return output;
    }

    public void copy(MatrixRuleSet src) {
        System.arraycopy(src.matrix, 0, matrix, 0, matrix.length);
        size = src.size;
    }

    public int getSimilarity(MatrixRuleSet other) {
        int sum = 0;
        for (int i = 0; i < matrix.length; i++) {
            sum += Long.bitCount(matrix[i] & other.matrix[i]);
        }
        return sum;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean output = false;
        if (c instanceof MatrixRuleSet) {
            int oldSize = size;
            subtract(matrix, ((MatrixRuleSet) c).matrix);
            output = size != oldSize;
        } else {
            for (Rule rule : (Collection<? extends Rule>) c) {
                output = remove(rule) || output;
            }
        }
        return output;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        int oldSize = size;
        if (c instanceof MatrixRuleSet) {
            for (int i = 0; i < matrix.length; i++) {
                matrix[i] &= ((MatrixRuleSet) c).matrix[i];
            }
        } else {
            for (int i = 0; i < c.size(); i++) {
                boolean modified = false;
                Iterator<Rule> it = iterator();
                while (it.hasNext()) {
                    if (!c.contains(it.next())) {
                        it.remove();
                        modified = true;
                    }
                }
                return modified;
            }
        }
        size = getSize(matrix);
        return size != oldSize;
    }

    @Override
    public void clear() {
        Arrays.fill(matrix, 0l);
        size = 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatrixRuleSet rules = (MatrixRuleSet) o;
        if (size != rules.size()) {
            return false;
        }

        if (!Arrays.equals(matrix, rules.matrix)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(matrix);
    }

    @Override
    public Iterator<Rule> iterator() {
        //System.out.println("A rule iterator on Matrix ruleset created!!!");
        return new RuleIterator();
    }

    @Override
    public Object[] toArray() {
        Rule[] output = new Rule[size()];
        final Iterator<Rule> iterator = iterator();
        int i = 0;
        while (iterator.hasNext()) {
            output[i] = iterator.next();
        }
        return output;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        final int size = size();
        if (a.length < size) {
            return (T[]) toArray();
        }
        final Iterator<Rule> iterator = iterator();
        int i = 0;
        while (iterator.hasNext()) {
            a[i] = (T) iterator.next();
        }
        a[size] = null;
        return a;
    }

    private class RuleIterator implements Iterator<Rule> {
        private static final int INITIAL_VALUE = -2;
        private static final int INVALID_VALUE = -1;

        int nextIndex;
        int currentIndex = INITIAL_VALUE;

        private RuleIterator() {
            nextIndex = findNextIndex();
        }

        @Override
        public boolean hasNext() {
            return nextIndex != INVALID_VALUE;
        }

        @Override
        public Rule next() {
            if (nextIndex == INVALID_VALUE) {
                throw new NoSuchElementException();
            }
            currentIndex = nextIndex;
            nextIndex++;
            nextIndex = findNextIndex();
            return ruleSet.get(currentIndex);
        }

        @Override
        public void remove() {
            if (currentIndex >= 0) {
                final int index = currentIndex;
                boolean alreadyThere = hasOneAt(matrix, index);
                if (alreadyThere) {
                    setUnSet(matrix, index, false);
                    size--;
                }
                currentIndex = -1;
            }
        }

        private int findNextIndex() {
            for (int i = Math.max(nextIndex, 0); i < matrix.length * 64; i++) {
                if (hasOneAt(matrix, i)) {
                    return i;
                }
            }
            return INVALID_VALUE;
        }
    }
}
