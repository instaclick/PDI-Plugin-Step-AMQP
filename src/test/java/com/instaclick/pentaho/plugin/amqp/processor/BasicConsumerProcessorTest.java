package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.junit.Before;
import org.pentaho.di.core.row.RowMetaInterface;

public class BasicConsumerProcessorTest
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
    public void testBasicGetNull() throws Exception
    {
        final Object[] row                = new Object[] {};
        final RowMetaInterface meta       = mock(RowMetaInterface.class);
        final BasicConsumerProcessor processor = new BasicConsumerProcessor(channel, plugin, data, new ArrayList<Initializer>(0));

        when(channel.basicGet(eq(data.target), eq(false))).thenReturn(null);

        data.routingIndex   = 0;
        data.bodyFieldIndex = 1;
        data.outputRowMeta  = meta;

        assertFalse(processor.process(row));
        verify(plugin, never()).putRow(eq(data.outputRowMeta), any(Object[].class));
    }

    @Test
    public void testBasicGet() throws Exception
    {
        final long tag                         = 1L;
        final String routingKey                = "#";
        final byte[] body                      = new byte[0];
        final Object[] row                     = new Object[] {};
        final Envelope envelope                = mock(Envelope.class);
        final GetResponse response             = mock(GetResponse.class);
        final RowMetaInterface meta            = mock(RowMetaInterface.class);
        final BasicConsumerProcessor processor = new BasicConsumerProcessor(channel, plugin, data, new ArrayList<Initializer>(0));

        when(channel.basicGet(eq(data.target), eq(false)))
            .thenReturn(response)
            .thenReturn(null);

        when(response.getBody())
            .thenReturn(body);

        when(response.getEnvelope())
            .thenReturn(envelope);

        when(envelope.getDeliveryTag())
            .thenReturn(tag);

        when(envelope.getRoutingKey())
            .thenReturn(routingKey);

        data.count          = 0;
        data.routingIndex   = 0;
        data.bodyFieldIndex = 1;
        data.limit          = 10L;
        data.outputRowMeta  = meta;

        assertTrue(processor.process(row));
        assertFalse(processor.process(row));

        verify(plugin, times(1)).incrementLinesInput();
        verify(channel, times(1)).basicAck(eq(tag), eq(true));
        verify(plugin, times(1)).logDebug(eq("basicAck : " + tag));
        verify(plugin, times(1)).putRow(eq(data.outputRowMeta), any(Object[].class));

        assertEquals(1, data.count);
        assertEquals(tag, data.amqpTag);
    }
}
