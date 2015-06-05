package com.instaclick.pentaho.plugin.amqp;

import java.io.IOException;


public interface AMQPConfirmation {
    public void ackDelivery(long deliveryTag) throws IOException;
    public void rejectDelivery(long deliveryTag) throws IOException;
}