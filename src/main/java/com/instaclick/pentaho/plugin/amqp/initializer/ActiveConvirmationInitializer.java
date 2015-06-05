package com.instaclick.pentaho.plugin.amqp.initializer;

import com.instaclick.pentaho.plugin.amqp.listener.ConfirmationAckListener;
import com.instaclick.pentaho.plugin.amqp.listener.ConfirmationRejectListener;
import com.instaclick.pentaho.plugin.amqp.AMQPPlugin;
import com.instaclick.pentaho.plugin.amqp.AMQPPluginData;
import com.instaclick.pentaho.plugin.amqp.listener.ConfirmationRowStepListener;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.ArrayList;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.trans.step.StepInterface;

public class ActiveConvirmationInitializer implements Initializer
{
    public static final ActiveConvirmationInitializer INSTANCE = new ActiveConvirmationInitializer();

    protected ConfirmationAckListener ackDelivery(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data)
    {
        return new ConfirmationAckListener() {
            @Override
            public void ackDelivery(long deliveryTag) throws IOException
            {
                if ( ! data.isTransactional) {
                    plugin.logDebug("Immidiate ack message   " + deliveryTag);
                    channel.basicAck(deliveryTag, false);
                    data.ack++;

                    return;
                }

                plugin.logDebug("Postponed ack message   " + deliveryTag);
                data.ackMsgInTransaction.add(deliveryTag);
            }
        };
    }

    protected ConfirmationRejectListener rejectDelivery(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data)
    {
        return new ConfirmationRejectListener() {
            @Override
            public void rejectDelivery(long deliveryTag) throws IOException
            {
                plugin.incrementLinesRejected();

                if ( ! data.isTransactional) {
                    plugin.logDebug("Immidiate reject message   " + deliveryTag);
                    channel.basicNack(deliveryTag, false, false);
                    data.rejected++;

                    return;
                }

                plugin.logDebug("Postponed reject message   " + deliveryTag);
                data.rejectedMsgInTransaction.add(deliveryTag);
            }
        };
    }

    @Override
    public void initialize(final Channel channel, final AMQPPlugin plugin, final AMQPPluginData data) throws IOException, KettleStepException
    {
        //bind to step with acknowledge rows on input stream
        if ( ! Const.isEmpty(data.ackStepName) ) {

            final StepInterface si = plugin.getTrans().getStepInterface( data.ackStepName, 0);

            if (si == null) {
                throw new KettleStepException("Can not find step : " + data.ackStepName );
            }

            if (plugin.getTrans().getStepInterface( data.ackStepName, 1 ) != null) {
                throw new KettleStepException("Only SINGLE INSTANCE Steps supported : " + data.ackStepName );
            }

            si.addRowListener(new ConfirmationRowStepListener(data.ackStepDeliveryTagField, ackDelivery(channel, plugin, data)));

            if (data.isTransactional) {
                data.ackMsgInTransaction = new ArrayList<Long>();
            }
        }

        //bind to step with rejected rows on input stream
        if ( ! Const.isEmpty(data.rejectStepName) ) {

            final StepInterface si = plugin.getTrans().getStepInterface( data.rejectStepName, 0 );

            if (si == null) {
                throw new KettleStepException("Can not find step : " + data.rejectStepName );
            }

            if (plugin.getTrans().getStepInterface( data.rejectStepName, 1 ) != null) {
                throw new KettleStepException("Only SINGLE INSTANCE Steps supported : " + data.rejectStepName );
            }

            si.addRowListener(new ConfirmationRowStepListener(data.rejectStepDeliveryTagField, rejectDelivery(channel, plugin, data)));

            if (data.isTransactional) {
                data.rejectedMsgInTransaction = new ArrayList<Long>();
            }
        }
    }
}
