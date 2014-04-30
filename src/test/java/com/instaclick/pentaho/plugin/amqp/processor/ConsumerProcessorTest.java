package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.junit.Before;
import org.pentaho.di.core.row.RowMetaInterface;

public class ConsumerProcessorTest
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
        final ConsumerProcessor processor = new ConsumerProcessor(channel, plugin, data);
        
        when(channel.basicGet(eq(data.target), eq(false))).thenReturn(null);

        data.routingIndex   = 0;
        data.bodyFieldIndex = 1;
        data.outputRowMeta  = meta;

        assertFalse(processor.process(row));
        verify(plugin, never()).putRow(eq(data.outputRowMeta), any(Object[].class));
    }
}
