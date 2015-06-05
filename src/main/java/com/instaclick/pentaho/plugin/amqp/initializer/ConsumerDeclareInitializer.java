package com.instaclick.pentaho.plugin.amqp.initializer;

import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginMeta;
import com.rabbitmq.client.Channel;
import java.io.IOException;

public class ConsumerDeclareInitializer implements Initializer
{
    public static final ConsumerDeclareInitializer INSTANCE = new ConsumerDeclareInitializer();

    @Override
    public void initialize(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data) throws IOException
    {
        plugin.logMinimal(String.format("Declaring Queue '%s' { durable:%s, exclusive:%s, auto_delete:%s}", data.target,  data.isDurable, data.isExclusive, data.isAutodel));
        channel.queueDeclare(data.target, data.isDurable, data.isExclusive, data.isAutodel, null);

        for (final AMQPPluginMeta.Binding item : data.bindings) {
            final String queueName    = data.target;
            final String exchangeName = plugin.environmentSubstitute(item.getTarget());
            final String routingKey   = plugin.environmentSubstitute(item.getRouting());

            plugin.logMinimal(String.format("Binding Queue '%s' to Exchange '%s' using routing key '%s'", queueName, exchangeName, routingKey));
            channel.queueBind(queueName, exchangeName, routingKey);
        }
    }
}
