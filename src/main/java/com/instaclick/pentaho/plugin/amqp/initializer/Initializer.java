package com.instaclick.pentaho.plugin.amqp.initializer;

import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.pentaho.di.core.exception.KettleStepException;

public interface Initializer
{
    public void initialize(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data) throws KettleStepException, IOException;
}
