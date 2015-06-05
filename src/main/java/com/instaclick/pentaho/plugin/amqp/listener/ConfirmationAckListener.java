package com.instaclick.pentaho.plugin.amqp.listener;

import java.io.IOException;

public interface ConfirmationAckListener
{
    public void ackDelivery(long deliveryTag) throws IOException;
}
