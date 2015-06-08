package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.junit.Before;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

public class WaitingConsumerProcessorTest
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
        data.target = "queue_name";
    }

    @Test
    public void testStart() throws Exception
    {
        final WaitingConsumerProcessor instance = new WaitingConsumerProcessor(channel, plugin, data, new ArrayList<Initializer>(0));

        instance.start();

        verify(channel).basicConsume(eq("queue_name"), eq(false), eq(instance.consumer));
    }

    @Test
    public void testConsume() throws Exception
    {
        final long tag                          = 1L;
        final String routingKey                 = "#";
        final byte[] body                       = new byte[0];
        final Envelope envelope                 = mock(Envelope.class);
        final Delivery delivery                 = mock(Delivery.class);
        final QueueingConsumer consumer         = mock(QueueingConsumer.class);
        final WaitingConsumerProcessor instance = new WaitingConsumerProcessor(channel, plugin, data, new ArrayList<Initializer>(0), consumer);

        when(consumer.nextDelivery(anyLong()))
            .thenReturn(delivery)
            .thenReturn(null);

        when(delivery.getBody())
            .thenReturn(body);

        when(delivery.getEnvelope())
            .thenReturn(envelope);

        when(envelope.getDeliveryTag())
            .thenReturn(tag);

        when(envelope.getRoutingKey())
            .thenReturn(routingKey);

        data.count          = 0;
        data.routingIndex   = 0;
        data.bodyFieldIndex = 1;
        data.limit          = 10L;
        data.waitTimeout    = 100L;

        assertTrue(instance.consume());
        assertFalse(instance.consume());

        verify(plugin, times(1)).incrementLinesInput();
        verify(consumer, times(2)).nextDelivery(eq(data.waitTimeout));

        assertEquals(1, data.count);
        assertEquals(tag, data.amqpTag);
    }
}
