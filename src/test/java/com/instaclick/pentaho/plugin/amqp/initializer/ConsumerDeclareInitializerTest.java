package com.instaclick.pentaho.plugin.amqp.initializer;

import com.google.common.collect.Lists;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginMeta.Binding;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.Map;
import org.junit.Test;
import org.junit.Before;
import static org.mockito.Mockito.*;

public class ConsumerDeclareInitializerTest
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
        final ConsumerDeclareInitializer instance = ConsumerDeclareInitializer.INSTANCE;
        final Binding binding1 = mock(Binding.class);
        final Binding binding2 = mock(Binding.class);

        when(binding1.getRouting()).thenReturn("ex.#");
        when(binding1.getTarget()).thenReturn("target_ex");
        when(binding1.getTargetType()).thenReturn(AMQPPluginData.TARGET_TYPE_EXCHANGE);

        when(plugin.environmentSubstitute(eq("ex.#"))).thenReturn("ex.#");
        when(plugin.environmentSubstitute(eq("target_ex"))).thenReturn("target_ex");
        when(plugin.environmentSubstitute(eq(AMQPPluginData.TARGET_TYPE_EXCHANGE))).thenReturn(AMQPPluginData.TARGET_TYPE_EXCHANGE);

        data.isDeclare = true;
        data.isDurable = true;
        data.isAutodel = false;
        data.target    = "queue_name";
        data.bindings  = Lists.newArrayList(binding1, binding2);

        instance.initialize(channel, plugin, data);

        verify(plugin).logMinimal(eq("Declaring Queue 'queue_name' {durable:true, exclusive:false, auto_delete:false}"));
        verify(plugin).logMinimal(eq("Binding Queue 'queue_name' to Exchange 'target_ex' using routing key 'ex.#'"));

        verify(channel).queueDeclare(eq(data.target), eq(data.isDurable), eq(data.isExclusive), eq(data.isAutodel), isNull(Map.class));
        verify(channel).queueBind(eq("queue_name"), eq("target_ex"), eq("ex.#"));
    }
}
