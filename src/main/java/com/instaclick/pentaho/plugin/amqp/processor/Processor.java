package com.instaclick.pentaho.plugin.amqp.processor;

public interface Processor
{
    public boolean process(final Object[] r) throws Exception;
    public void shutdown() throws Exception;
    public void onSuccess() throws Exception;
    public void onFailure() throws Exception;
}
