package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.initializer.Initializer;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.List;
import org.pentaho.di.core.exception.KettleStepException;

abstract class BaseProcessor implements Processor
{
    final List<Initializer> initializers;
    final AMQPPluginData data;
    final AMQPPlugin plugin;
    final Channel channel;

    public BaseProcessor(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data, final List<Initializer> initializers)
    {
        this.initializers = initializers;
        this.channel      = channel;
        this.plugin       = plugin;
        this.data         = data;
    }

    protected String getAmqpRoutingKey(final Object[] r) throws KettleStepException
    {
        if (hasAmqpRoutingKey(r)) {
            return (r[data.routingIndex] == null) ? null : r[data.routingIndex].toString();
        }

        logUndefinedRow(r, "Invalid routing key", "ICAmqpPlugin001");

        return null;
    }

    protected String getAmqpBody(final Object[] r) throws KettleStepException
    {
        if (hasAmqpBody(r)) {
            return (r[data.bodyFieldIndex] == null) ? "" : r[data.bodyFieldIndex].toString();
        }

        logUndefinedRow(r, "Invalid body", "ICAmqpPlugin002");

        return null;
    }

    protected boolean hasAmqpRoutingKey(final Object[] r)
    {
        return rowContains(r, data.routingIndex);
    }

    protected boolean hasAmqpBody(final Object[] r)
    {
        return rowContains(r, data.bodyFieldIndex);
    }

    protected boolean rowContains(final Object[] r, final Integer index)
    {
        if (index == null || index < 0) {
            return false;
        }

        return (r.length > index);
    }

    protected void logUndefinedRow(final Object[] r, final String log, final String code) throws KettleStepException
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
        if (channel.isOpen()) {
            channel.getConnection().close();
        }
    }

    @Override
    public void start() throws KettleStepException, IOException
    {
        for (final Initializer initializer : initializers) {
            initializer.initialize(channel, plugin, data);
        }
    }

    @Override
    public void cancel() throws IOException
    {
    }


}
