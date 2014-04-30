package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.AMQPException;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginMeta;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class ProcessorFactory
{
    private final ConnectionFactory factory = new ConnectionFactory();

    private Channel channelFor(final AMQPPluginData data) throws IOException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException
    {
        factory.setUri(data.uri);

        final Connection conn = factory.newConnection();
        final Channel channel = conn.createChannel();

        channel.basicQos(0);

        if ( ! conn.isOpen()) {
            throw new AMQPException("Unable to open a AMQP connection");
        }

        if ( ! channel.isOpen()) {
            throw new AMQPException("Unable to open an AMQP channel");
        }

        return channel;
    }

    public Processor processorFor(final AMQPPlugin step,final AMQPPluginData data, final AMQPPluginMeta meta) throws IOException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException
    {
        final Channel channel = channelFor(data);

        if (data.isConsumer) {
            return new ConsumerProcessor(channel, step, data);
        }
        
        if (data.isProducer) {
            return new ProducerProcessor(channel, step, data);
        }

        throw new RuntimeException("Unknown mode");
    }
}
