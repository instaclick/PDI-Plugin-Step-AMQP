package com.instaclick.pentaho.plugin.amqp.processor;

import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;

public class ConsumerProcessor extends AbstractProcessor
{
    public ConsumerProcessor(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data) 
    {
        super(channel, plugin, data);
    }

    @Override
    public boolean process(final Object[] r) throws IOException, KettleStepException
    {
        GetResponse response;

        do {
            response = channel.basicGet(data.target, false);

            if (response == null) {
                plugin.setOutputDone();
                return false;
            }

            final byte[] body = response.getBody();
            final long tag    = response.getEnvelope().getDeliveryTag();

            data.body    = new String(body);
            data.routing = response.getEnvelope().getRoutingKey();
            data.count ++;

            // safely add the unique field at the end of the output row
            Object[] row = RowDataUtil.allocateRowData(data.outputRowMeta.size());

            row[data.bodyFieldIndex] = data.body;
            row[data.routingIndex]   = data.routing;

            // put the row to the output row stream
            plugin.putRow(data.outputRowMeta, row);

            if ( ! data.isTransactional) {
                plugin.logDebug("basicAck : " + tag);
                channel.basicAck(tag, true);
            }

            data.amqpTag = tag;

            if (data.count >= data.limit) {
                plugin.logBasic(String.format("Message limit %s", data.count));
                plugin.setOutputDone();

                return false;
            }

        } while (response != null);

        plugin.setOutputDone();

        return false;
    }

    @Override
    public void onSuccess() throws IOException
    {
        if ( ! data.isTransactional) {
            return;
        }

        plugin.logMinimal("Ack messages : " + data.amqpTag);
        channel.basicAck(data.amqpTag, true);
    }
    
    @Override
    public void onFailure() throws IOException
    {
        if ( ! data.isTransactional) {
            return;
        }

        plugin.logMinimal("Ignoring messages : " + data.amqpTag);
        data.amqpTag = -1;
    }
}
