package edu.usc.enl.cacheflow.processor.file;

import edu.usc.enl.cacheflow.processor.Processor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/13/11
 * Time: 3:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class SaveFileProcessor<I> extends Processor<I, File> {
    private File file;
    private boolean append;

    public SaveFileProcessor(I input, File file, boolean append) {
        super(input);
        this.file = file;
        this.append = append;
    }

    public SaveFileProcessor(Processor<?, I> processorInput, File file, boolean append) {
        super(processorInput);
        this.file = file;
        this.append = append;
    }

    @Override
    public File process(I input) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(file, append));
        if (append){
            writer.write("\n");
        }
        writer.write(input.toString());
        writer.close();
        return file;
    }
}
