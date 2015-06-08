package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.initializer.ProducerDeclareInitializer;
import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.instaclick.pentaho.plugin.amqp.initializer.ConsumerDeclareInitializer;
import com.instaclick.pentaho.plugin.amqp.initializer.ActiveConvirmationInitializer;
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
import java.util.ArrayList;
import java.util.List;
import org.pentaho.di.core.Const;

public class ProcessorFactory
{
    private final ConnectionFactory factory = new ConnectionFactory();

    protected Connection connectionFor(final AMQPPluginData data) throws IOException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException
    {
        if ( ! Const.isEmpty(data.uri)) {
            factory.setUri(data.uri);

            return factory.newConnection();
        }

        factory.setHost(data.host);
        factory.setPort(data.port);
        factory.setUsername(data.username);
        factory.setPassword(data.password);

        if (data.vhost != null) {
            factory.setVirtualHost(data.vhost);
        }

        if (data.useSsl) {
            factory.useSslProtocol();
        }

        return factory.newConnection();
    }

    protected Channel channelFor(final AMQPPluginData data) throws IOException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException
    {
        final Connection conn = connectionFor(data);
        final Channel channel = conn.createChannel();
        final int qos         = data.isConsumer ? data.prefetchCount : 0;

        channel.basicQos(qos);

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
        final Channel channel                 = channelFor(data);
        final List<Initializer> initializers  = new ArrayList<Initializer>();

        if (data.activeConfirmation) {
            initializers.add(ActiveConvirmationInitializer.INSTANCE);
        }

        if (data.isConsumer && data.isDeclare) {
            initializers.add(ConsumerDeclareInitializer.INSTANCE);
        }

        if (data.isProducer && data.isDeclare) {
            initializers.add(ProducerDeclareInitializer.INSTANCE);
        }

        if (data.isConsumer && data.isWaitingConsumer) {
            return new WaitingConsumerProcessor(channel, step, data, initializers);
        }

        if (data.isConsumer) {
            return new BasicConsumerProcessor(channel, step, data, initializers);
        }

        if (data.isProducer) {
            return new ProducerProcessor(channel, step, data, initializers);
        }

        throw new RuntimeException("Unknown mode");
    }
}
