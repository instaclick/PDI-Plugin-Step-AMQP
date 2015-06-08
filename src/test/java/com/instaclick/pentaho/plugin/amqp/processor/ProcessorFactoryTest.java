package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginMeta;
import com.instaclick.pentaho.plugin.amqp.initializer.ActiveConvirmationInitializer;
import com.instaclick.pentaho.plugin.amqp.initializer.ConsumerDeclareInitializer;
import com.instaclick.pentaho.plugin.amqp.initializer.ProducerDeclareInitializer;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import static org.hamcrest.CoreMatchers.instanceOf;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import static org.mockito.Mockito.*;

public class ProcessorFactoryTest
{
    AMQPPluginMeta meta;
    AMQPPluginData data;
    AMQPPlugin plugin;
    Channel channel;

    @Before
    public void setUp()
    {
        channel     = mock(Channel.class, RETURNS_MOCKS);
        meta        = mock(AMQPPluginMeta.class);
        data        = mock(AMQPPluginData.class);
        plugin      = mock(AMQPPlugin.class);
        data.target = "queue_name";
    }

    @Test
    public void testProcessorForActiveConfirmationDeclaringConsumer() throws Exception
    {
        final ProcessorFactory instance = new ProcessorFactory() {
            @Override
            protected Channel channelFor(AMQPPluginData data)
            {
                return channel;
            }
        };

        data.activeConfirmation = true;
        data.isWaitingConsumer  = true;
        data.isDeclare          = true;
        data.isConsumer         = true;

        final Processor processor = instance.processorFor(plugin, data, meta);

        assertThat(processor, instanceOf(WaitingConsumerProcessor.class));

        final BaseProcessor baseProcessor = (BaseProcessor) processor;

        assertEquals(2, baseProcessor.initializers.size());
        assertTrue(baseProcessor.initializers.contains(ConsumerDeclareInitializer.INSTANCE));
        assertTrue(baseProcessor.initializers.contains(ActiveConvirmationInitializer.INSTANCE));
    }

    @Test
    public void testProcessorForDeclaringPoducer() throws Exception
    {
        final ProcessorFactory instance = new ProcessorFactory() {
            @Override
            protected Channel channelFor(AMQPPluginData data)
            {
                return channel;
            }
        };

        data.isDeclare  = true;
        data.isProducer = true;

        final Processor processor = instance.processorFor(plugin, data, meta);

        assertThat(processor, instanceOf(ProducerProcessor.class));

        final BaseProcessor baseProcessor = (BaseProcessor) processor;

        assertEquals(1, baseProcessor.initializers.size());
        assertTrue(baseProcessor.initializers.contains(ProducerDeclareInitializer.INSTANCE));
    }

    @Test
    public void testProcessorForBasicConsumer() throws Exception
    {
        final ProcessorFactory instance = new ProcessorFactory() {
            @Override
            protected Channel channelFor(AMQPPluginData data)
            {
                return channel;
            }
        };

        data.isConsumer = true;

        final Processor processor = instance.processorFor(plugin, data, meta);

        assertThat(processor, instanceOf(BasicConsumerProcessor.class));

        final BaseProcessor baseProcessor = (BaseProcessor) processor;

        assertEquals(0, baseProcessor.initializers.size());
    }
}
