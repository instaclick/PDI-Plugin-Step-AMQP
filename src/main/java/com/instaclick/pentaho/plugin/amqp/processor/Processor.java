package com.instaclick.pentaho.plugin.amqp.processor;

import java.io.IOException;
import org.pentaho.di.core.exception.KettleStepException;

public interface Processor
{
    public boolean process(final Object[] r) throws KettleStepException, IOException;
    public void start() throws KettleStepException, IOException;
    public void shutdown() throws KettleStepException, IOException;
    public void onSuccess() throws KettleStepException, IOException;
    public void onFailure() throws KettleStepException, IOException;
    public void cancel() throws KettleStepException, IOException;
}
