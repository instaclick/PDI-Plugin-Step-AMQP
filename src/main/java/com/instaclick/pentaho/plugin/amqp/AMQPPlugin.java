package com.instaclick.pentaho.plugin.amqp;

import java.io.IOException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
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
import static org.pentaho.di.core.encryption.KettleTwoWayPasswordEncoder.decryptPasswordOptionallyEncrypted;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.TransListener;
import org.pentaho.di.core.row.RowMetaInterface;

public class AMQPPlugin extends BaseStep implements StepInterface
{
    private AMQPPluginData data;
    private AMQPPluginMeta meta;

    final private ProcessorFactory factory = new ProcessorFactory();
    private Processor processor;

    private final TransListener transListener = new TransListener()
    {
        @Override
        public void transStarted(Trans trans) throws KettleException { }

        @Override
        public void transActive(Trans trans) { }

        @Override
        public void transFinished(Trans trans) throws KettleException
        {
            logMinimal("Trans Finished - transactional=" + data.isTransactional);

            if ( ! data.isTransactional && ! data.activeConfirmation ) {
                return;
            }

            if (trans.getErrors() > 0) {
                logMinimal(String.format("Transformation failure, ignoring changes", trans.getErrors()));
                onFailure();

                return;
            }

            if (isStopped()) {
                logMinimal("Transformation STOPPED, ignoring changes");
                shutdown();
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
        meta       = (AMQPPluginMeta) smi;
        data       = (AMQPPluginData) sdi;
        Object[] r = getRow();

        if (first) {
            first = false;

            try {
                initPluginStep();
            } catch (Exception e) {
                throw new AMQPException(e.getMessage(), e);
            }

            try {
                processor.start();
            } catch (IOException e) {
                setOutputDone();
                throw new AMQPException(e.getMessage(), e);
            }
        }

        while ( ! isStopped()) {

            try {
                if ( ! processor.process(r)) {

                    setOutputDone();

                    return false;
                }
            } catch (IOException e) {
                throw new AMQPException(e.getMessage(), e);
            }

            // log progress
            if (checkFeedback(getLinesWritten())) {
                logBasic(String.format("liner %s", getLinesWritten()));
            }

            return true;
        }

        setOutputDone();

        return false;
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi)
    {
        meta = (AMQPPluginMeta) smi;
        data = (AMQPPluginData) sdi;

        return super.init(smi, sdi);
    }

    /**
     * Initialize
     */
    private void initPluginStep() throws Exception
    {
        RowMetaInterface rowMeta = (getInputRowMeta() != null)
            ? (RowMetaInterface) getInputRowMeta().clone()
            : new RowMeta();

        // clone the input row structure and place it in our data object
        data.outputRowMeta = rowMeta;
        // use meta.getFields() to change it, so it reflects the output row structure
        meta.getFields(rowMeta, getStepname(), null, null, this, repository, metaStore);

        Integer port    = null;
        String body     = environmentSubstitute(meta.getBodyField());
        String contentTypeField = environmentSubstitute(meta.getContentTypeField());
        String routing  = environmentSubstitute(meta.getRouting());
        String uri      = environmentSubstitute(meta.getUri());
        String host     = environmentSubstitute(meta.getHost());
        String vhost    = environmentSubstitute(meta.getVhost());
        String username = environmentSubstitute(meta.getUsername());
        String deliveryTagField = environmentSubstitute(meta.getDeliveryTagField());
        String password = decryptPasswordOptionallyEncrypted(environmentSubstitute(meta.getPassword()));

        if ( ! Const.isEmpty(meta.getPort())) {
            port = Integer.parseInt(environmentSubstitute(meta.getPort()));
        }

        if (body == null) {
            throw new AMQPException("Unable to retrieve field : " + meta.getBodyField());
        }

        if ((username == null || password == null || host == null || port == null) && (uri == null)) {
            throw new AMQPException("Unable to retrieve connection information");
        }

        // get field index
        data.bodyFieldIndex = data.outputRowMeta.indexOfValue(body);
        data.target         = environmentSubstitute(meta.getTarget());
        data.bindings       = meta.getBindings();
        data.limit          = meta.getLimit();
        data.prefetchCount  = meta.getPrefetchCount();
        data.waitTimeout    = meta.getWaitTimeout();

        data.uri       = uri;
        data.host      = host;
        data.port      = port;
        data.vhost     = vhost;
        data.username  = username;
        data.password  = password;

        data.useSsl            = meta.isUseSsl();
        data.isTransactional   = meta.isTransactional();
        data.isWaitingConsumer = meta.isWaitingConsumer();
        data.isRequeue         = meta.isRequeue();
        data.isConsumer        = meta.isConsumer();
        data.isProducer        = meta.isProducer();

        //Confirmation sniffers
        data.ackStepName                = environmentSubstitute(meta.getAckStepName());
        data.ackStepDeliveryTagField    = environmentSubstitute(meta.getAckStepDeliveryTagField());
        data.rejectStepName             = environmentSubstitute(meta.getRejectStepName());
        data.rejectStepDeliveryTagField = environmentSubstitute(meta.getRejectStepDeliveryTagField());

        //init Producer
        data.exchtype    = meta.getExchtype();
        data.isAutodel   = meta.isAutodel();
        data.isDurable   = meta.isDurable();
        data.isDeclare   = meta.isDeclare();
        data.isExclusive = meta.isExclusive();

        if ( ! Const.isEmpty(routing)) {
            data.routingIndex = data.outputRowMeta.indexOfValue(routing);

            if (data.routingIndex < 0) {
                throw new AMQPException("Unable to retrieve routing key field : " + meta.getRouting());
            }
        }

        if ( ! Const.isEmpty(deliveryTagField)) {
            data.deliveryTagIndex = data.outputRowMeta.indexOfValue(deliveryTagField);

            if (data.deliveryTagIndex < 0) {
                throw new AMQPException("Unable to retrieve DeliveryTag field : " + deliveryTagField);
            }
        }

        if ( ! Const.isEmpty(contentTypeField)) {
            data.contentTypeIndex = data.outputRowMeta.indexOfValue(contentTypeField);

            if (data.contentTypeIndex < 0) {
                throw new AMQPException("Unable to retrieve ContentType field : " + contentTypeField);
            }
        }

        if (data.bodyFieldIndex < 0) {
            throw new AMQPException("Unable to retrieve body field : " + body);
        }

        if (data.target == null) {
            throw new AMQPException("Unable to retrieve queue/exchange name");
        }

        logMinimal(getString("AmqpPlugin.Body.Label")     + " : " + body);
        logMinimal(getString("AmqpPlugin.Routing.Label")  + " : " + routing);
        logMinimal(getString("AmqpPlugin.Target.Label")   + " : " + data.target);
        logMinimal(getString("AmqpPlugin.Limit.Label")    + " : " + data.limit);
        logMinimal(getString("AmqpPlugin.UseSsl.Label")   + " : " + data.useSsl);
        logMinimal(getString("AmqpPlugin.URI.Label")      + " : " + uri);
        logMinimal(getString("AmqpPlugin.Username.Label") + " : " + username);
        logDebug(getString("AmqpPlugin.Password.Label")   + " : " + password);
        logMinimal(getString("AmqpPlugin.Host.Label")     + " : " + host);
        logMinimal(getString("AmqpPlugin.Port.Label")     + " : " + port);
        logMinimal(getString("AmqpPlugin.Vhost.Label")    + " : " + vhost);

        if ( data.isConsumer && data.isTransactional && data.prefetchCount > 0 ) {
            throw new AMQPException(getString("AmqpPlugin.Error.PrefetchCountAndTransactionalNotSupported"));
        }

        if ( data.isConsumer && data.isRequeue) {
            logMinimal(getString("AmqpPlugin.Info.RequeueDevelopmentUsage"));
        }

        if  (data.isConsumer && ( ! Const.isEmpty(data.ackStepName) || ! Const.isEmpty(data.rejectStepName))) {
            // we start active confirmation , need not to Ack messages in batches
            data.activeConfirmation = true;
        }

        processor = factory.processorFor(this, data, meta);

        // hook to transListener, to make final flush to transactional case, or in case ack/nack steps should finish.
        if (data.isTransactional || data.activeConfirmation ) {
            getTrans().addTransListener(transListener);
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
        } catch (KettleStepException ex) {
            logError(ex.getMessage());
        } catch (IOException ex) {
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
        } catch (KettleStepException ex) {
            logError(ex.getMessage());
        } catch (IOException ex) {
            logError(ex.getMessage());
        }

        this.shutdown();
    }

    private void shutdown()
    {
        try {
            processor.shutdown();
        } catch (KettleStepException ex) {
            logError(ex.getMessage());
        } catch (IOException ex) {
            logError(ex.getMessage());
        }
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi)
    {
        meta = (AMQPPluginMeta) smi;
        data = (AMQPPluginData) sdi;

        logDebug("Dispose invoked");

        if ( ! data.isTransactional && ! data.activeConfirmation ) {
            onSuccess();
        }

        super.dispose(smi, sdi);
    }

    @Override
    public void stopRunning( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
        try {
            processor.cancel();;
        } catch (KettleStepException ex) {
            logError(ex.getMessage());
        } catch (IOException ex) {
            logError(ex.getMessage());
        }
        
        super.stopRunning(smi, sdi);
    }

}
