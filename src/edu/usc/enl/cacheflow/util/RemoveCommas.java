package edu.usc.enl.cacheflow.util;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 1/1/12
 * Time: 12:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class RemoveCommas {
    public static void main(String[] args) {
        File inputFolder = new File("input/hotcloud/flows/original2");
        for (File file : inputFolder.listFiles()) {
            file.renameTo(new File(inputFolder+"/"+file.getName().replaceAll(",", "_")));
        }
    }
}
