package com.instaclick.pentaho.plugin.amqp.listener;

import java.io.IOException;
import org.junit.Test;
import org.junit.Before;
import static org.mockito.Mockito.*;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;

public class ConfirmationRowStepListenerTest
{
    String deliveryTagName;
    RowMetaInterface rowMeta;
    ConfirmationAckListener ackDelegate;
    ConfirmationRejectListener rejectDelegate;

    @Before
    public void setUp()
    {
        rejectDelegate  = mock(ConfirmationRejectListener.class);
        ackDelegate     = mock(ConfirmationAckListener.class);
        rowMeta         = mock(RowMetaInterface.class);
        deliveryTagName = "delivery_tag";
    }

    @Test
    public void testRowReadEventAck() throws KettleStepException, KettleValueException, IOException
    {
        final Object[] row = new Object[] {"body", 1L};
        final ConfirmationRowStepListener instance = new ConfirmationRowStepListener(deliveryTagName, ackDelegate);

        when(rowMeta.indexOfValue(eq(deliveryTagName)))
            .thenReturn(1);

        when(rowMeta.getInteger(eq(row), eq(1)))
            .thenReturn(10L);

        instance.rowReadEvent(rowMeta, row);

        verify(rowMeta).getInteger(eq(row), eq(1));
        verify(rowMeta).indexOfValue(eq(deliveryTagName));
        verify(ackDelegate).ackDelivery(eq(10L));
    }

    @Test
    public void testRowReadEventNoAck() throws KettleStepException, KettleValueException, IOException
    {
        final Object[] row = new Object[] {"body", 1L};
        final ConfirmationRowStepListener instance = new ConfirmationRowStepListener(deliveryTagName, rejectDelegate);

        when(rowMeta.indexOfValue(eq(deliveryTagName)))
            .thenReturn(1);

        when(rowMeta.getInteger(eq(row), eq(1)))
            .thenReturn(10L);

        instance.rowReadEvent(rowMeta, row);

        verify(rowMeta).getInteger(eq(row), eq(1));
        verify(rowMeta).indexOfValue(eq(deliveryTagName));
        verify(rejectDelegate).rejectDelivery(eq(10L));
    }
}
