package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ProducerProcessorTest
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
    public void testOnSuccessTransactional() throws IOException
    {
        final ProducerProcessor instance = new ProducerProcessor(channel, plugin, data, new ArrayList<Initializer>(0));

        data.isTransactional = true;
        data.isTxOpen        = true;

        instance.onSuccess();

        assertFalse(data.isTxOpen);
        assertTrue(data.isTransactional);

        verify(channel, times(1)).txCommit();
        verify(plugin, times(1)).logMinimal(eq("Commit channel transaction"));
    }

    @Test
    public void testOnSuccessNonTransactional() throws IOException
    {
        final ProducerProcessor instance = new ProducerProcessor(channel, plugin, data, new ArrayList<Initializer>(0));

        data.isTransactional = false;
        data.isTxOpen        = false;

        instance.onSuccess();

        assertFalse(data.isTxOpen);
        assertFalse(data.isTransactional);

        verify(channel, times(0)).txCommit();
    }

    @Test
    public void testOnFailureTransactional() throws IOException
    {
        final ProducerProcessor instance = new ProducerProcessor(channel, plugin, data, new ArrayList<Initializer>(0));

        data.isTransactional = true;
        data.isTxOpen        = true;

        instance.onFailure();

        assertFalse(data.isTxOpen);
        assertTrue(data.isTransactional);

        verify(channel, times(1)).txRollback();
    }

    @Test
    public void testOnFailureNonTransactional() throws IOException
    {
        final ProducerProcessor instance = new ProducerProcessor(channel, plugin, data, new ArrayList<Initializer>(0));

        data.isTransactional = false;
        data.isTxOpen        = false;

        instance.onFailure();

        assertFalse(data.isTxOpen);
        assertFalse(data.isTransactional);

        verify(channel, times(0)).txRollback();
    }
}
