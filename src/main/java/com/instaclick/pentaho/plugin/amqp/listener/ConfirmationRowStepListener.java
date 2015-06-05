package com.instaclick.pentaho.plugin.amqp.listener;

import java.io.IOException;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;

public class ConfirmationRowStepListener implements RowListener
{
    int rowsRead = 0;
    final String deliveryTagName;
    final ConfirmationAckListener ackDelegate;
    final ConfirmationRejectListener rejectDelegate;

    public ConfirmationRowStepListener(final String deliveryTagName, final ConfirmationAckListener ackDelegate)
    {
        this.deliveryTagName = deliveryTagName;
        this.ackDelegate     = ackDelegate;
        this.rejectDelegate  = null;
    }

    public ConfirmationRowStepListener(final String deliveryTagName, final ConfirmationRejectListener rejectDelegate)
    {
        this.deliveryTagName = deliveryTagName;
        this.rejectDelegate  = rejectDelegate;
        this.ackDelegate     = null;
    }

    @Override
    public void rowReadEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException
    {
        try {
            final Long deliveryTag = rowMeta.getInteger(row, rowMeta.indexOfValue(deliveryTagName));

            rowsRead++;

            if (ackDelegate != null) {
                synchronized (this) {
                    ackDelegate.ackDelivery(deliveryTag);
                }
            }

            if (rejectDelegate != null) {
                synchronized (this) {
                    rejectDelegate.rejectDelivery(deliveryTag);
                }
            }

        } catch (KettleValueException e) {
            throw new KettleStepException(e);
        } catch (IOException e) {
            throw new KettleStepException(e);
        }
    }

    @Override
    public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] row)
    {

    }

    @Override
    public void errorRowWrittenEvent(RowMetaInterface rowMeta, Object[] row)
    {

    }
}
