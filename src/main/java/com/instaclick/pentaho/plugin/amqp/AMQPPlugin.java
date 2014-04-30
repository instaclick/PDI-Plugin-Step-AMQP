package com.instaclick.pentaho.plugin.amqp;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import static com.instaclick.pentaho.plugin.amqp.Messages.getString;
import com.instaclick.pentaho.plugin.amqp.processor.Processor;
import com.instaclick.pentaho.plugin.amqp.processor.ProcessorFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.TransListener;

public class AMQPPlugin extends BaseStep implements StepInterface
{
    final private ProcessorFactory factory = new ProcessorFactory();
    private Processor processor;
    private AMQPPluginData data;
    private AMQPPluginMeta meta;

    private final TransListener transListener = new TransListener() {
        @Override
        public void transFinished(Trans trans) throws KettleException
        {
            if ( ! data.isTransactional) {
                return;
            }

            if (trans.getErrors() > 0) {
                logMinimal(String.format("Transformation failure, ignoring changes", trans.getErrors()));
                onFailure();

                return;
            }

            onSuccess();
        }
    };

    public AMQPPlugin(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
    {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException, KettleStepException
    {
        meta        = (AMQPPluginMeta) smi;
        data        = (AMQPPluginData) sdi;
        Object[] r  = getRow();

        if (first) {
            first = false;

            try {
                initPlugin();
            } catch (Exception e) {
                throw new AMQPException(e.getMessage(), e);
            }
        }

        // log progress
        if (checkFeedback(getLinesWritten())) {
            logBasic(String.format("liner %s", getLinesWritten()));
        }

        try {
            return processor.process(r);
        } catch (Exception ex) {
            throw new AMQPException(ex.getMessage(), ex);
        }
    }

    private void onSuccess()
    {
        logMinimal("On success invoked");

        if (processor == null) {
            return;
        }

        try {
            processor.onSuccess();
        } catch (Exception ex) {
            logError(ex.getMessage());
        }

        this.shutdown();
    }

    private void onFailure()
    {
        logMinimal("On failure invoked");

        if (processor == null) {
            return;
        }

        try {
            processor.onFailure();
        } catch (Exception ex) {
            logError(ex.getMessage());
        }

        this.shutdown();
    }

    private void shutdown()
    {
        try {
            processor.shutdown();
        } catch (Exception ex) {
            logError(ex.getMessage());
        }
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi)
    {
        meta = (AMQPPluginMeta) smi;
        data = (AMQPPluginData) sdi;

        return super.init(smi, sdi);
    }

    /**
     * Initialize filter
     */
    private void initPlugin() throws Exception
    {
        RowMetaInterface rowMeta = (getInputRowMeta() != null)
            ? (RowMetaInterface) getInputRowMeta().clone()
            : new RowMeta();

        // clone the input row structure and place it in our data object
        data.outputRowMeta = rowMeta;
        // use meta.getFields() to change it, so it reflects the output row structure
        meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

        final String body     = environmentSubstitute(meta.getBodyField());
        final String routing  = environmentSubstitute(meta.getRouting());
        final String uri      = environmentSubstitute(meta.getUri());

        if (body == null) {
            throw new AMQPException("Unable to retrieve field : " + meta.getBodyField());
        }

        if (uri == null) {
            throw new AMQPException("Unable to retrieve connection uri");
        }

        // get field index
        data.bodyFieldIndex  = data.outputRowMeta.indexOfValue(body);
        data.target          = environmentSubstitute(meta.getTarget());
        data.limit           = meta.getLimit();
        data.uri             = uri;

        data.isTransactional = meta.isTransactional();
        data.isConsumer      = AMQPPluginData.MODE_CONSUMER.equals(meta.getMode());
        data.isProducer      = AMQPPluginData.MODE_PRODUCER.equals(meta.getMode());

        if ( ! Const.isEmpty(routing)) {
            data.routingIndex = data.outputRowMeta.indexOfValue(routing);

            if (data.routingIndex < 0) {
                throw new AMQPException("Unable to retrieve routing key field : " + meta.getRouting());
            }
        }

        if (data.bodyFieldIndex < 0) {
            throw new AMQPException("Unable to retrieve body field : " + body);
        }

        if (data.target == null) {
            throw new AMQPException("Unable to retrieve queue/exchange name");
        }

        logMinimal(getString("AmqpPlugin.URI.Label")       + " : " + uri);
        logMinimal(getString("AmqpPlugin.Body.Label")      + " : " + body);
        logMinimal(getString("AmqpPlugin.Routing.Label")   + " : " + routing);
        logMinimal(getString("AmqpPlugin.Target.Label")    + " : " + data.target);
        logMinimal(getString("AmqpPlugin.Limit.Label")     + " : " + data.limit);

        processor = factory.processorFor(this, data, meta);

        if (data.isTransactional) {
            getTrans().addTransListener(transListener);
        }
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi)
    {
        meta = (AMQPPluginMeta) smi;
        data = (AMQPPluginData) sdi;

        if ( ! data.isTransactional) {
            onSuccess();
        }

        super.dispose(smi, sdi);
    }
}