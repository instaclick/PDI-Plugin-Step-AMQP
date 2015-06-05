package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.util.List;

public class BasicConsumerProcessor extends BaseConsumerProcessor
{
    public BasicConsumerProcessor(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data, final List<Initializer> initializers)
    {
        super(channel, plugin, data, initializers);
    }

    @Override
    protected boolean consume() throws IOException
    {
        final GetResponse response = channel.basicGet(data.target, false);

        if (response == null) {
            return false;
        }

        final byte[] body       = response.getBody();
        final Envelope envelope = response.getEnvelope();
        final long tag          = envelope.getDeliveryTag();

        data.routing = envelope.getRoutingKey();
        data.body    = new String(body);
        data.amqpTag = tag;
        data.count ++;

        this.plugin.incrementLinesInput();

        return true;
    }
}
