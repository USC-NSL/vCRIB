package edu.usc.enl.cacheflow.model.factory;

import edu.usc.enl.cacheflow.model.rule.Partition;

import java.io.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: masoud
 * Date: 7/24/12
 * Time: 11:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class BinaryPartitionFactory {
    public Collection<Partition> load(ObjectInputStream ois,Map<String,Object>parameters) throws IOException, ClassNotFoundException {
        //ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
        Map<String,Object>parameters2= (Map<String, Object>) ois.readObject();
        parameters.putAll(parameters2);
        return (Collection<Partition>) ois.readObject();
    }
}
