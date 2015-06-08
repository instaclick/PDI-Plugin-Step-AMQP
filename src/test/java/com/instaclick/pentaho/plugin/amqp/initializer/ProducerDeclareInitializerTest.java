package com.instaclick.pentaho.plugin.amqp.initializer;

import com.google.common.collect.Lists;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginMeta.Binding;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.junit.Test;
import org.junit.Before;
import static org.mockito.Mockito.*;

public class ProducerDeclareInitializerTest
{
    AMQPPluginData data;
    AMQPPlugin plugin;
    Channel channel;

    @Before
    public void setUp()
    {
        channel     = mock(Channel.class, RETURNS_MOCKS);
        data        = mock(AMQPPluginData.class);
        plugin      = mock(AMQPPlugin.class);
    }

    @Test
    public void testInitialize() throws IOException
    {
        final ProducerDeclareInitializer instance = ProducerDeclareInitializer.INSTANCE;
        final Binding binding1 = mock(Binding.class);
        final Binding binding2 = mock(Binding.class);

        when(binding1.getRouting()).thenReturn("ex.#");
        when(binding1.getTarget()).thenReturn("target_ex");
        when(binding1.getTargetType()).thenReturn(AMQPPluginData.TARGET_TYPE_EXCHANGE);

        when(binding2.getRouting()).thenReturn("queue.#");
        when(binding2.getTarget()).thenReturn("target_queue");
        when(binding2.getTargetType()).thenReturn(AMQPPluginData.TARGET_TYPE_QUEUE);

        when(plugin.environmentSubstitute(eq("ex.#"))).thenReturn("ex.#");
        when(plugin.environmentSubstitute(eq("target_ex"))).thenReturn("target_ex");
        when(plugin.environmentSubstitute(eq(AMQPPluginData.TARGET_TYPE_EXCHANGE))).thenReturn(AMQPPluginData.TARGET_TYPE_EXCHANGE);

        when(plugin.environmentSubstitute(eq("queue.#"))).thenReturn("queue.#");
        when(plugin.environmentSubstitute(eq("target_queue"))).thenReturn("target_queue");
        when(plugin.environmentSubstitute(eq(AMQPPluginData.TARGET_TYPE_QUEUE))).thenReturn(AMQPPluginData.TARGET_TYPE_QUEUE);

        data.target    = "ex_name";
        data.exchtype  = "topic";
        data.isDeclare = true;
        data.isDurable = true;
        data.isAutodel = false;
        data.bindings  = Lists.newArrayList(binding1, binding2);

        instance.initialize(channel, plugin, data);

        verify(plugin).logMinimal(eq("Declaring Exchange 'ex_name' {type:topic, durable:true, auto_delete:false}"));
        verify(plugin).logMinimal(eq("Binding Exchange 'target_ex' to Exchange 'ex_name' using routing key 'ex.#'"));
        verify(plugin).logMinimal(eq("Binding Queue 'target_queue' to Exchange 'ex_name' using routing key 'queue.#'"));

        verify(channel).exchangeDeclare(data.target, data.exchtype, data.isDurable, data.isAutodel, null);
        verify(channel).exchangeBind(eq("target_ex"), eq("ex_name"), eq("ex.#"));
        verify(channel).queueBind(eq("target_queue"), eq("ex_name"), eq("queue.#"));
    }
}
