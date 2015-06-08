package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.junit.Before;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;

public class BaseConsumerProcessorTest
{
    BaseConsumerProcessor instance;
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

        instance    = new BaseConsumerProcessor(channel, plugin, data, new ArrayList<Initializer>(0)) {
            @Override
            protected boolean consume() throws IOException, KettleStepException
            {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
    }

    @Test
    public void testOnSuccessTransactional() throws IOException
    {
        data.isTransactional = true;
        data.amqpTag         = 10L;

        instance.onSuccess();

        assertTrue(data.isTransactional);

        verify(channel, times(1)).basicAck(eq(data.amqpTag), eq(true));
        verify(plugin, times(1)).logMinimal(eq("Ack All messages : " + data.amqpTag));
    }

    @Test
    public void testOnSuccessNonTransactional() throws IOException
    {
        data.isTransactional = false;

        instance.onSuccess();

        assertFalse(data.isTransactional);

        verify(channel, times(0)).basicAck(anyLong(), anyBoolean());
    }

    @Test
    public void testOnFailureTransactional() throws IOException
    {
        data.isTransactional = true;
        data.amqpTag         = 10L;

        instance.onFailure();

        assertTrue(data.isTransactional);
        assertEquals(-1L, data.amqpTag);
    }

    @Test
    public void testOnFailureNonTransactional() throws IOException
    {
        data.isTransactional = false;
        data.amqpTag         = 10L;

        instance.onFailure();

        assertFalse(data.isTransactional);
        assertEquals(10L, data.amqpTag);
    }
}
