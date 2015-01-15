package edu.usc.enl.cacheflow.processor;

/**
 * Created by IntelliJ IDEA.
 * User: Masoud
 * Date: 12/11/11
 * Time: 7:35 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Processor<I, O> {
    protected I input;
    protected Processor<?, I> processorInput;


    public Processor(I input) {
        this.input = input;
    }

    public Processor(Processor<?, I> processorInput) {
        this.processorInput = processorInput;
    }

    public O run() throws Exception {
        if (input != null) {
            return process(input);
        } else {
            return process(processorInput.run());
        }
    }

    public abstract O process(I input) throws Exception;

    public void setTailInput(Object input) {
        if (this.processorInput == null) {
            this.input = (I) input;
        } else {
            this.processorInput.setTailInput(input);
        }
    }

    public void setProcessorInput(Processor<?, I> processorInput) {
        this.processorInput = processorInput;
    }

}
