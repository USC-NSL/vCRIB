package edu.usc.enl.cacheflow.model.factory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/8/12
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class FileFactory<T> {
    protected StopCondition stopCondition;
    private Class<Collection<T>> collectionClass;

    public FileFactory(StopCondition stopCondition) {
        this.stopCondition = stopCondition;
    }

    public void parseHeaderLine(BufferedReader reader, Map<String, Object> parameters) throws IOException {
        // FORMAT COMES FROM STATISTICS PARAMETERLINE PRINT AND DOES THAT VIA WRITERSEIALIZABLEUTIL
        String s = reader.readLine();
        StringTokenizer st = new StringTokenizer(s, ",");
        while (st.hasMoreTokens()) {
            String s1 = st.nextToken();
            int level2Delimeter = s1.indexOf("=");
            String value = s1.substring(level2Delimeter + 1);
            Object valueObject;
            try {
                valueObject = new Long(value);
            } catch (NumberFormatException e) {
                try {
                    valueObject = new Double(value);
                } catch (NumberFormatException e1) {
                    valueObject = value;
                }
            }
            parameters.put(s1.substring(0, level2Delimeter), valueObject);
        }
    }

    public <C extends Collection<T>> C create(BufferedReader reader, Map<String, Object> parameters, C toFill) throws IOException {
        parseHeaderLine(reader,parameters);
        return createBody(reader,toFill);
    }

    public <C extends Collection<T>> C createBody(BufferedReader reader,C toFill) throws IOException {
        while (!stopCondition.stop(reader)) {
            toFill.add(create(reader.readLine()));
        }
        return toFill;
    }

    protected abstract T create(String s);

    public static abstract class StopCondition {
        public abstract boolean stop(BufferedReader reader) throws IOException;
    }

    public static class EndOfFileCondition extends StopCondition {
        public boolean stop(BufferedReader reader) throws IOException {
            if (!reader.ready()) {
                return true;
            }
            reader.mark(100000);
            final String s = reader.readLine();
            final boolean output = s == null || s.isEmpty() && !reader.ready();
            reader.reset();
            return output;
        }
    }

    public static class EmptyLineStopCondition extends StopCondition {
        @Override
        public boolean stop(BufferedReader reader) throws IOException {
            if (!reader.ready()) {
                return true;
            }
            reader.mark(100000);
            final String s = reader.readLine();
            final boolean output = s.isEmpty();
            reader.reset();
            return output;
        }
    }


}
