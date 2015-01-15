package edu.usc.enl.cacheflow.model;

import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Masoud
 * Date: 5/13/12
 * Time: 11:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class WriterSerializableUtil {
    public static void writeFile(WriterSerializable o, File file, boolean append, Map<String, Object> parameters) throws IOException {
        writeFile(Collections.singleton(o), file, append, parameters);
    }

    public static void writeFile(Collection<? extends WriterSerializable> os, File file, boolean append, Map<String, Object> parameters) throws IOException {
        final PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file, append)));
        write(os, writer, parameters);
    }

    public static String getString(Collection<? extends WriterSerializable> os, Map<String, Object> parameters) {
        StringWriter out = new StringWriter();
        try {
            write(os, new PrintWriter(new BufferedWriter(out)), parameters);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return out.toString();
    }

    public static void write(Collection<? extends WriterSerializable> os, PrintWriter writer, Map<String, Object> parameters) throws IOException {
        if (os.size() > 0) {
            if (parameters != null) {
                writer.println(Statistics.getParameterLine(parameters));
            } else {
                writer.println();
            }
            os.iterator().next().headerToString(writer);
            for (WriterSerializable o : os) {
                o.toString(writer);
            }
        }
        writer.close();
    }
}
