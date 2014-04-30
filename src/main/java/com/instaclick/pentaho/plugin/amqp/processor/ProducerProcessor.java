package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import java.io.IOException;

public class ProducerProcessor extends AbstractProcessor
{
    public ProducerProcessor(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data) 
    {
        super(channel, plugin, data);
    }

    @Override
    public boolean process(Object[] r) throws Exception
    {
        if (r == null) {
            plugin.setOutputDone();

            return false;
        }
        
        if (data.isTransactional && ! data.isTxOpen) {
            channel.txSelect();

            data.isTxOpen = true;
        }

        data.body    = getAmqpBody(r);
        data.routing = (data.routingIndex != null)
            ? getAmqpRoutingKey(r)
            : "";
        
        if (data.body == null) {
            return false;
        }

        // publish the current message
        channel.basicPublish(data.target, data.routing, null, data.body.getBytes());

        // put the row to the output row stream
        plugin.putRow(data.outputRowMeta, r);

        return true;
    }

    @Override
    public void onSuccess() throws IOException
    {
        if ( ! data.isTransactional || ! data.isTxOpen) {
            return;
        }

        plugin.logMinimal("Commit channel transaction");
        channel.txCommit();

        data.isTxOpen = false;
    }
    
    @Override
    public void onFailure() throws IOException
    {
        if ( ! data.isTransactional || ! data.isTxOpen) {
            return;
        }

        channel.txRollback();
        data.isTxOpen = false;
    }
}
