package com.instaclick.pentaho.plugin.amqp;

import java.io.IOException;

public interface AMQPConfirmationReject
{
    public void rejectDelivery(long deliveryTag) throws IOException;
}
