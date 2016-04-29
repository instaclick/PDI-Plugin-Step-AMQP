
package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

public class AbstractProcessorTest
{
    AMQPPluginData data;
    AMQPPlugin plugin;
    Channel channel;
    Connection connection;

    @Before
    public void setUp()
    {
        channel     = mock(Channel.class, RETURNS_MOCKS);
        connection = mock(Connection.class, RETURNS_MOCKS);
        data        = mock(AMQPPluginData.class);
        plugin      = mock(AMQPPlugin.class);
    }

    @Test
    public void testGetAmqpRoutingKey() throws Exception
    {
        final String value                  = null;
        final String key                    = "amqp_routing_key";
        final Object[] row                  = new Object[] {key, value};
        final BaseProcessor processor   = new AbstractProcessorImpl(channel, plugin, data);

        data.routingIndex   = 0;
        data.bodyFieldIndex = 1;

        assertEquals(key, processor.getAmqpRoutingKey(row));
    }

    @Test
    public void testGetAmqpBody() throws Exception
    {
        final String key                    = "amqp_routing_key";
        final String value                  = "amqp_body";
        final Object[] row                  = new Object[] {key, value};
        final BaseProcessor processor   = new AbstractProcessorImpl(channel, plugin, data);

        data.routingIndex   = 0;
        data.bodyFieldIndex = 1;

        assertEquals(value, processor.getAmqpBody(row));
    }

    @Test
    public void testGetInvalidAmqpRoutingKey() throws Exception
    {
        final String key                    = "amqp_routing_key";
        final Object[] row                  = new Object[] {key};
        final RowMetaInterface meta         = mock(RowMetaInterface.class);
        final BaseProcessor processor   = new AbstractProcessorImpl(channel, plugin, data);

        data.routingIndex   = 3;
        data.bodyFieldIndex = 1;

        when(plugin.isDebug()).thenReturn(true);
        when(plugin.getLinesRead()).thenReturn(1L);
        when(plugin.getInputRowMeta()).thenReturn(meta);

        assertNull(processor.getAmqpRoutingKey(row));
        verify(plugin).putError(eq(meta), eq(row), eq(1L), eq("1 - Invalid routing key"), isNull(String.class), eq("ICAmqpPlugin001"));
    }

    public void testGetInvalidAmqpBody() throws Exception
    {
        final String key                    = "amqp_routing_key";
        final String value                  = "amqp_body";
        final Object[] row                  = new Object[] {key, value};
        final RowMetaInterface meta         = mock(RowMetaInterface.class);
        final BaseProcessor processor   = new AbstractProcessorImpl(channel, plugin, data);

        data.routingIndex   = 0;
        data.bodyFieldIndex = 2;

        when(plugin.isDebug()).thenReturn(true);
        when(plugin.getLinesRead()).thenReturn(1L);
        when(plugin.getInputRowMeta()).thenReturn(meta);

        assertNull(processor.getAmqpBody(row));
        verify(plugin).putError(eq(meta), eq(row), eq(1L), eq("1 - Invalid value row"), isNull(String.class), eq("ICRiakPlugin002"));
    }

    @Test
    public void testGetNullKeyValue() throws Exception
    {
        final Object[] row                  = new Object[] {null, null};
        final RowMetaInterface meta         = mock(RowMetaInterface.class);
        final BaseProcessor processor   = new AbstractProcessorImpl(channel, plugin, data);

        data.routingIndex   = 0;
        data.bodyFieldIndex = 1;

        when(plugin.isDebug()).thenReturn(true);
        when(plugin.getLinesRead()).thenReturn(1L);
        when(plugin.getInputRowMeta()).thenReturn(meta);

        assertNull(processor.getAmqpRoutingKey(row));
        assertEquals("", processor.getAmqpBody(row));
        verify(plugin, never()).putError(any(RowMetaInterface.class), any(Object[].class), any(Long.class), any(String.class), any(String.class), any(String.class));
    }

    @Test
    public void testShutdownClosesChannel() throws IOException
    {
        final BaseProcessor processor = new AbstractProcessorImpl(channel, plugin, data);

        when(channel.isOpen()).thenReturn(true);
        when(channel.getConnection()).thenReturn(connection);
        when(connection.isOpen()).thenReturn(true);
        processor.shutdown();
        verify(channel, times(1)).getConnection();
        verify(connection, times(1)).close();
    }

    @Test
    public void testStartInvockInitializers() throws IOException, KettleStepException
    {
        final Initializer initializer        = mock(Initializer.class);
        final List<Initializer> initializers = new ArrayList<Initializer>();
        final BaseProcessor processor        = new AbstractProcessorImpl(channel, plugin, data, initializers);

        initializers.add(initializer);

        processor.start();

        verify(initializer, times(1)).initialize(eq(channel), eq(plugin), eq(data));
    }

    public class AbstractProcessorImpl extends BaseProcessor
    {
        public AbstractProcessorImpl(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data)
        {
            this(channel, plugin, data, new ArrayList<Initializer>(0));
        }

        public AbstractProcessorImpl(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data, final List<Initializer> initializers)
        {
            super(channel, plugin, data, initializers);
        }

        @Override
        public boolean process(Object[] r)
        {
            return true;
        }

        @Override
        public void onSuccess() { }

        @Override
        public void onFailure() { }
    }
}
