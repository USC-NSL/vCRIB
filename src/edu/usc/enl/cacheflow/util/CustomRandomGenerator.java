package edu.usc.enl.cacheflow.util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 8/14/12
 * Time: 11:41 AM
 * To change this template use File | Settings | File Templates.
 */
public class CustomRandomGenerator<T> {
    private T[] objects;
    private double[] cumWeight;
    private Object[] tempArray = new Object[2];

    public CustomRandomGenerator(List<T> objects, List<Double> weights) throws AllZeroWeightException {
        this.objects = (T[]) objects.toArray();
        cumWeight = new double[objects.size()];
        double sum = 0;
        int i = 0;
        for (Double weight : weights) {
            sum += weight;
            cumWeight[i] = sum;
            i++;
        }
        cumWeight[cumWeight.length - 1] = 1;
        if (cumWeight[cumWeight.length - 1] == 0) {
            throw new AllZeroWeightException();
        }
    }

    public CustomRandomGenerator(Map<T, Double> objectWeights) throws AllZeroWeightException {
        this.objects = (T[]) objectWeights.keySet().toArray();
        cumWeight = new double[objects.length];
        double sum = 0;
        int i = 0;
        for (T object : objects) {
            sum += objectWeights.get(object);
            cumWeight[i] = sum;
            i++;
        }
        if (cumWeight[cumWeight.length - 1] == 0) {
            throw new AllZeroWeightException();
        }
    }

    public CustomRandomGenerator(T[] objects, double[] weights) throws AllZeroWeightException {
        this.objects = Arrays.copyOf(objects, objects.length);
        cumWeight = new double[objects.length];
        double sum = 0;
        int i = 0;
        for (Double weight : weights) {
            sum += weight;
            cumWeight[i] = sum;
            i++;
        }
        if (cumWeight[cumWeight.length - 1] == 0) {
            throw new AllZeroWeightException();
        }
    }

    public T getRandom(double index) {
        getRandom(index, tempArray);
        return (T) tempArray[0];
    }

    public void getRandom(double index, Object[] objectIndex) {

        int randomIndex = Arrays.binarySearch(cumWeight, index * cumWeight[cumWeight.length - 1]);
        if (randomIndex < 0) {
            randomIndex = -randomIndex - 1;
        } else {
            //we need the earliest of equal items
            for (; randomIndex > 0; randomIndex--) {
                if (cumWeight[randomIndex] != cumWeight[randomIndex - 1]) {
                    break;
                }
            }
        }
        objectIndex[0] = objects[randomIndex];
        objectIndex[1] = randomIndex;
    }

    public void updateWeight(int index, double newWeight) throws AllZeroWeightException {
        double oldWeight = cumWeight[index];
        cumWeight[index] = newWeight;
        for (int j = index + 1; j < cumWeight.length; j++) {
            cumWeight[j] += newWeight - oldWeight;
            if (j > 0 && cumWeight[j] < cumWeight[j - 1]) {
                throw new RuntimeException("Hey!!! double overloading!");
            }
        }
        if (cumWeight[cumWeight.length - 1] == 0) {
            throw new AllZeroWeightException();
        }
    }

    public Map<T, Double> getObjectWeights() {
        Map<T, Double> output = new HashMap<T, Double>();
        for (int i = 0; i < objects.length; i++) {
            output.put(objects[i], (cumWeight[i] - (i == 0 ? 0 : cumWeight[i - 1]))
                    / cumWeight[cumWeight.length - 1]);
        }
        return output;
    }

    public static class AllZeroWeightException extends Exception {

    }
}
