package com.instaclick.pentaho.plugin.amqp.listener;

import java.io.IOException;

public interface ConfirmationRejectListener
{
    public void rejectDelivery(long deliveryTag) throws IOException;
}
