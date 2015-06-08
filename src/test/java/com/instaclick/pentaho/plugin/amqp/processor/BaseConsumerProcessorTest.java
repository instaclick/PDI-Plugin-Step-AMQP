package com.instaclick.pentaho.plugin.amqp.processor;

import com.google.common.collect.Lists;
import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
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

    @Test
    public void testShutdownLog() throws IOException
    {
        data.ack      = 2;
        data.count    = 6;
        data.rejected = 3;

        instance.shutdown();

        verify(plugin).logMinimal(eq("Queue messages received : ack=2, rejected=3, requeue=1"));
    }

    @Test
    public void testFlushActiveConfirmation() throws IOException
    {
        data.ack                      = 0;
        data.rejected                 = 0;
        data.ackMsgInTransaction      = Lists.newArrayList(1L, 3L);
        data.rejectedMsgInTransaction = Lists.newArrayList(2L, 4L);

        instance.flushActiveConfirmation();

        assertEquals(2, data.ack);
        assertEquals(2, data.rejected);
        assertTrue(data.ackMsgInTransaction.isEmpty());
        assertTrue(data.rejectedMsgInTransaction.isEmpty());

        verify(plugin).logMinimal(eq("Acknowledged messages : 2"));
        verify(plugin).logMinimal(eq("Rejected messages : 2"));

        verify(channel).basicAck(eq(1L), eq(false));
        verify(channel).basicAck(eq(3L), eq(false));
        verify(channel).basicNack(eq(2L), eq(false), eq(false));
        verify(channel).basicNack(eq(4L), eq(false), eq(false));
    }

    @Test
    public void testConsume() throws IOException, KettleStepException
    {
        final Object[] row          = new Object[] {};
        final AtomicBoolean flag    = new AtomicBoolean(true);
        final RowMetaInterface meta = mock(RowMetaInterface.class);
        instance                    = new BaseConsumerProcessor(channel, plugin, data, new ArrayList<Initializer>(0)) {
            @Override
            protected boolean consume() throws IOException, KettleStepException
            {
                return flag.get();
            }
        };

        data.routingIndex     = 0;
        data.bodyFieldIndex   = 1;
        data.deliveryTagIndex = 2;
        data.routingIndex     = 0;
        data.bodyFieldIndex   = 1;
        data.deliveryTagIndex = 2;

        data.ack                = 0;
        data.count              = 0;
        data.rejected           = 0;
        data.limit              = 5L;
        data.amqpTag            = 10L;
        data.outputRowMeta      = meta;
        data.isTransactional    = false;
        data.isRequeue          = false;
        data.activeConfirmation = false;

        assertTrue(instance.process(row));
        assertEquals(1, data.count);

        verify(plugin).logDebug(eq("basicAck : 10"));
        verify(plugin, times(1)).incrementLinesInput();
        verify(channel, times(1)).basicAck(eq(data.amqpTag), eq(true));
        verify(plugin, times(1)).putRow(eq(data.outputRowMeta), any(Object[].class));
    }

    @Test
    public void testConsumeTransactionalWithLimit() throws IOException, KettleStepException
    {
        final Object[] row          = new Object[] {};
        final AtomicBoolean flag    = new AtomicBoolean(true);
        final RowMetaInterface meta = mock(RowMetaInterface.class);
        instance                    = new BaseConsumerProcessor(channel, plugin, data, new ArrayList<Initializer>(0)) {
            @Override
            protected boolean consume() throws IOException, KettleStepException
            {
                return flag.get();
            }
        };

        data.routingIndex     = 0;
        data.bodyFieldIndex   = 1;
        data.deliveryTagIndex = 2;
        data.routingIndex     = 0;
        data.bodyFieldIndex   = 1;
        data.deliveryTagIndex = 2;

        data.ack                = 0;
        data.count              = 0;
        data.rejected           = 0;
        data.limit              = 1L;
        data.amqpTag            = 10L;
        data.outputRowMeta      = meta;
        data.isTransactional    = true;
        data.isRequeue          = false;
        data.activeConfirmation = false;

        assertFalse(instance.process(row));

        assertEquals(1, data.count);
        verify(plugin).logBasic(eq("Message limit 1"));
        verify(plugin, times(1)).incrementLinesInput();
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verify(plugin, times(1)).putRow(eq(data.outputRowMeta), any(Object[].class));
    }
}
