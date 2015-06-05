package com.instaclick.pentaho.plugin.amqp.initializer;

import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginMeta;
import com.rabbitmq.client.Channel;
import java.io.IOException;

public class ProducerDeclareInitializer implements Initializer
{
    public static final ProducerDeclareInitializer INSTANCE = new ProducerDeclareInitializer();

    @Override
    public void initialize(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data) throws IOException
    {
        plugin.logMinimal(String.format("Declaring Exchange '%s' {type:%s, durable:%s, auto_delete:%s}", data.target, data.exchtype, data.isDurable, data.isAutodel));
        channel.exchangeDeclare(data.target, data.exchtype, data.isDurable, data.isAutodel, null);

        for (AMQPPluginMeta.Binding item : data.bindings) {
            final String exchangeName = data.target;
            final String targetName   = plugin.environmentSubstitute(item.getTarget());
            final String routingKey   = plugin.environmentSubstitute(item.getRouting());
            final String targetType   = plugin.environmentSubstitute(item.getTargetType());

            if (AMQPPluginData.TARGET_TYPE_QUEUE.equals(targetType))  {
                plugin.logMinimal(String.format("Binding Queue '%s' to Exchange '%s' using routing key '%s'", targetName, exchangeName, routingKey));
                channel.queueBind(targetName, exchangeName, routingKey);

                continue;
            }

            plugin.logMinimal(String.format("Binding dest Exchange '%s' to Exchange '%s' using routing key '%s'", targetName, exchangeName, routingKey));
            channel.exchangeBind(targetName, exchangeName, routingKey);
        }
    }
}
