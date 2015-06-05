package com.instaclick.pentaho.plugin.amqp;

import java.io.IOException;

public interface AMQPConfirmationAck
{
    public void ackDelivery(long deliveryTag) throws IOException;
}
