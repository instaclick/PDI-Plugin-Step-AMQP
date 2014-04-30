package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import java.io.IOException;

abstract class AbstractProcessor implements Processor
{
    protected final AMQPPluginData data;
    protected final AMQPPlugin plugin;
    protected final Channel channel;
    
    public AbstractProcessor(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data)
    {
        this.plugin  = plugin;
        this.channel = channel;
        this.data    = data;
    }

    protected String getAmqpRoutingKey(final Object[] r) throws Exception
    {
        if (hasAmqpRoutingKey(r)) {
            return (r[data.routingIndex] == null) ? null : r[data.routingIndex].toString();
        }

        logUndefinedRow(r, "Invalid routing key", "ICAmqpPlugin001");

        return null;
    }

    protected String getAmqpBody(final Object[] r) throws Exception
    {
        if (hasAmqpBody(r)) {
            return (r[data.bodyFieldIndex] == null) ? null : r[data.bodyFieldIndex].toString();
        }

        logUndefinedRow(r, "Invalid body", "ICAmqpPlugin002");

        return null;
    }

    protected boolean hasAmqpRoutingKey(final Object[] r) throws Exception
    {
        return rowContains(r, data.routingIndex);
    }

    protected boolean hasAmqpBody(final Object[] r) throws Exception
    {
        return rowContains(r, data.bodyFieldIndex);
    }

    protected boolean rowContains(final Object[] r, final Integer index) throws Exception
    {
        if (index == null || index < 0) {
            return false;
        }

        return (r.length > index);
    }

    protected void logUndefinedRow(final Object[] r, final String log, final String code) throws Exception
    {
        final String msg = plugin.getLinesRead() + " - " + log;

        if (plugin.isDebug()) {
            plugin.logDebug(msg);
        }

        plugin.putError(plugin.getInputRowMeta(), r, 1, msg, null, code);
    }

    @Override
    public void shutdown() throws IOException
    {
        channel.close();
    }
}
