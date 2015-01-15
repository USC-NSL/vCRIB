package edu.usc.enl.cacheflow.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 6/18/12
 * Time: 10:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateRandomNum {
    public static void main(String[] args) {
        String outputFile = "input/osdi/random.txt";
        int num=100;
        Random random = new Random(System.currentTimeMillis());
        try {
            PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
            for (int i = 0; i < num; i++) {
                writer.println(random.nextLong());
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
