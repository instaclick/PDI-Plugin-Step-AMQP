package com.instaclick.pentaho.plugin.amqp;

import com.instaclick.pentaho.plugin.amqp.AMQPPluginMeta.Binding;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.ArrayList;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepListener;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import static com.instaclick.pentaho.plugin.amqp.Messages.getString;
import static org.pentaho.di.core.encryption.KettleTwoWayPasswordEncoder.decryptPasswordOptionallyEncrypted;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.TransListener;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.core.exception.KettleValueException;

public class AMQPPlugin extends BaseStep implements StepInterface, AMQPConfirmation

{
    private AMQPPluginData data;
    private AMQPPluginMeta meta;

    private ConnectionFactory factory = new ConnectionFactory();
    private QueueingConsumer consumer = null;
    private Connection conn = null;
    private Channel channel = null;

    private final StepListener confirmStepListener = new StepListener()
    {
        @Override
        public void stepActive( Trans trans, StepMeta stepMeta, StepInterface step )
        {
        }
        
        @Override
        public void stepFinished( Trans trans, StepMeta stepMeta, StepInterface step )
        {
            synchronized(this) {
                data.watchedConfirmStep.remove(step);
            };
            logDebug("DETECT FINISHED STEP : " + step.getStepMeta().getName() + ". need wait for steps yet : " + data.watchedConfirmStep.size());
        }
    };

    private final TransListener transListener = new TransListener()
    {
        @Override
        public void transStarted(Trans trans) throws KettleException
        {
        }

        @Override
        public void transActive(Trans trans)
        {
        }

        @Override
        public void transFinished(Trans trans) throws KettleException
        {
            logMinimal("Trans Finished - transactional=" + data.isTransactional);

            if ( ! data.isTransactional) {
                return;
            }

            if (trans.getErrors() > 0) {
                logMinimal(String.format("Transformation failure, ignoring changes", trans.getErrors()));
                closeAmqp();

                return;
            }

            flush();
        }
    };

    //implement delivery confrimation for interface AMQPConfirmation
    // when we use transaction, just store deliveryTag to ack it on finish
    // if use transaction but not use tag sniffers, transaction will Ack all messages in pre 2.2.1 manner
    @Override
    public void ackDelivery(long deliveryTag) throws IOException
    {
        if (data.isConsumer) {
            if (!data.isTransactional) {
                logBasic("IMMIDIATE ACK MESSAGE   " + deliveryTag);
                channel.basicAck(deliveryTag, false);
            }  else {
                logBasic("POSTPONED ACK MESSAGE   " + deliveryTag);
                data.ackMsgInTransaction.add(deliveryTag);
            }
        }
    }

    //implement delivery confrimation for interface AMQPConfirmation
    // when we use transaction, just store deliveryTag to reject it on finish
    // if use transaction but not use tag sniffers, transaction will Ack all messages in pre 2.2.1 manner
    @Override
    public void rejectDelivery(long deliveryTag) throws IOException
    {

        if (data.isConsumer) {
            if (!data.isTransactional) {
                logBasic("IMMIDIATE REJECT MESSAGE   " + deliveryTag);
                channel.basicNack(deliveryTag, false, false);
            }  else {
                logBasic("POSTPONED REJECT MESSAGE   " + deliveryTag);
                data.rejectedMsgInTransaction.add(deliveryTag);
            }
        }

    }


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

            if (AMQPPluginData.MODE_PRODUCER.equals(meta.getMode()) && r == null) {
                setOutputDone();
                return false;
            }

            try {
                initPluginStep();
            } catch (Exception e) {
                throw new AMQPException(e.getMessage(), e);
            }
        }

        if (data.isConsumer) {
            try {
                consume();

                setOutputDone();

                return false;
            } catch (IOException e) {
                throw new AMQPException(e.getMessage(), e);
            } catch (InterruptedException e) {
                throw new AMQPException(e.getMessage(), e);
            }
        }

        if (r == null) {
            setOutputDone();

            return false;
        }

        if (data.isProducer) {
            try {
                produce(r);
            } catch (IOException e) {
                throw new AMQPException(e.getMessage(), e);
            }
        }

        return true;
    }

    private void produce(Object[] r) throws IOException, KettleStepException
    {
        if (data.isTransactional && ! data.isTxOpen) {
            channel.txSelect();

            data.isTxOpen = true;
        }

        if (r.length < data.bodyFieldIndex || r[data.bodyFieldIndex] == null) {
            String putErrorMessage = getLinesRead() + " - Ignore invalid message data row";

            if (isDebug()) {
                logDebug(putErrorMessage);
            }

            putError(getInputRowMeta(), r, 1, putErrorMessage, null, "ICAmqpPlugin001");

            return;
        }

        if (data.routingIndex != null && (r.length < data.routingIndex || r[data.routingIndex] == null)) {
            String putErrorMessage = getLinesRead() + " - Ignore invalid message routing row";

            if (isDebug()) {
                logDebug(putErrorMessage);
            }

            putError(getInputRowMeta(), r, 1, putErrorMessage, null, "ICAmqpPlugin002");

            return;
        }

        data.body    = String.valueOf(r[data.bodyFieldIndex]);
        data.routing = (data.routingIndex != null)
                ? String.valueOf(r[data.routingIndex])
                : "";

        // publish the current message
        channel.basicPublish(data.target, data.routing, null, data.body.getBytes());

        // put the row to the output row stream
        putRow(data.outputRowMeta, r);

        // log progress
        if (checkFeedback(getLinesRead())) {
            logBasic(String.format("linenr %s", getLinesRead()));
        }
    }

    private boolean consumeData(long timeout) throws IOException, InterruptedException
    {
        if (data.isWaitingConsumer) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery(timeout);

            if (delivery == null) {
                return false;
            }

            final byte[] body = delivery.getBody();
            final long tag    = delivery.getEnvelope().getDeliveryTag();

            data.routing = delivery.getEnvelope().getRoutingKey();
            data.body    = new String(body);
            data.amqpTag = tag;
            data.count ++;

            return true;
        }

        GetResponse response = channel.basicGet(data.target, false);

        if (response == null) {
            return false;
        }

        final byte[] body = response.getBody();
        final long tag    = response.getEnvelope().getDeliveryTag();

        data.routing = response.getEnvelope().getRoutingKey();
        data.body    = new String(body);
        data.amqpTag = tag;
        data.count ++;

        return true;
    }

    private void consume() throws IOException, KettleStepException, InterruptedException
    {
        if (data.isWaitingConsumer) {
            logMinimal("WAITING FOR MESSAGES : " + data.waitTimeout);
            channel.basicConsume(data.target, false, consumer);
        }

        do {
            if ( ! consumeData(data.waitTimeout)) {
                return;
            }

            // safely add the unique field at the end of the output row
            Object[] row = RowDataUtil.allocateRowData(data.outputRowMeta.size());

            row[data.bodyFieldIndex] = data.body;
            if ( data.routingIndex != null ) row[data.routingIndex]   = data.routing;
            if ( data.deliveryTagIndex != null) row[data.deliveryTagIndex] = data.amqpTag;

            // put the row to the output row stream
            putRow(data.outputRowMeta, row);

            // log progress
            if (checkFeedback(getLinesWritten())) {
                logBasic(String.format("liner %s", getLinesWritten()));
            }

            if ( ! data.isTransactional && ! data.isRequeue && ! data.activeConfirmation) {
                logDebug("basicAck : " + data.amqpTag);
                channel.basicAck(data.amqpTag, true);
            }

            if (data.count >= data.limit) {
                logBasic(String.format("Message limit %s", data.count));
                return;
            }

        } while ( !isStopped());
    }

    private void flush()
    {
        logMinimal("Flush invoked");

        if (data.isConsumer  && ! data.isRequeue && channel.isOpen() ) {
            try {
                if ( data.activeConfirmation && data.isTransactional )  { 
                    //ack all good
                    logMinimal("Ack All Good messages, total count  : " + data.ackMsgInTransaction.size());
                    for (Long ampqTag : data.ackMsgInTransaction )  channel.basicAck(ampqTag, false);
                    //reject all with errors
                    logMinimal("Ack All Bad messages, total count  : " + data.rejectedMsgInTransaction.size());
                    for (Long ampqTag : data.rejectedMsgInTransaction )  channel.basicNack(ampqTag, false, false);
                } 

                if ( !data.activeConfirmation && data.isTransactional )  {
                    logMinimal("Ack All messages : " + data.amqpTag);
                    channel.basicAck(data.amqpTag, true);
                }

            } catch (IOException ex) {
                logError(ex.getMessage());
            } catch (com.rabbitmq.client.AlreadyClosedException ex ) {
                logError(ex.getMessage());
            }
        }


        if (data.isProducer && data.isTransactional && data.isTxOpen && channel.isOpen() ) {
            try {

                logMinimal("Commit channel transaction");

                channel.txCommit();

                data.isTxOpen = false;
            } catch (IOException ex) {
                logError(ex.getMessage());
            }
        }

        closeAmqp();
    }

    private void closeAmqp()
    {
        if ( channel != null && channel.isOpen() ) {

            if (data.isProducer && data.isTransactional && data.isTxOpen) {
                try {
                    logMinimal("Rollback channel transaction");
                    channel.txRollback();

                    data.isTxOpen = false;
                } catch (IOException ex) {
                    logError(ex.getMessage());
                }
            }

            try 
            {
                logMinimal("Closing AMQP channel");
                channel.close();
            } catch (IOException ex) {
                logError(ex.getMessage());
            }
        }

        if ( conn != null && conn.isOpen() ) {
            try {
                logMinimal("Closing AMQP connection");
                conn.close();
            } catch (IOException ex) {
                logError(ex.getMessage());
            }
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
        meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

        Integer port    = null;
        String body     = environmentSubstitute(meta.getBodyField());
        String routing  = environmentSubstitute(meta.getRouting());
        String deliveryTagField = environmentSubstitute(meta.getDeliveryTagField());
        String uri      = environmentSubstitute(meta.getUri());
        String host     = environmentSubstitute(meta.getHost());
        String vhost    = environmentSubstitute(meta.getVhost());
        String username = environmentSubstitute(meta.getUsername());
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

        data.isTransactional   = meta.isTransactional();
        data.isWaitingConsumer = meta.isWaitingConsumer();
        data.isRequeue         = meta.isRequeue();
        data.isConsumer        = meta.isConsumer();
        data.isProducer        = meta.isProducer();

        //Confirmation sniffers

        data.ackStepName = environmentSubstitute(meta.getAckStepName());
        data.ackStepDeliveryTagField = environmentSubstitute(meta.getAckStepDeliveryTagField());

        data.rejectStepName = environmentSubstitute(meta.getRejectStepName());
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
        logMinimal(getString("AmqpPlugin.UseSsl.Label")   + " : " + meta.isUseSsl());

        if ( ! Const.isEmpty(uri)) {
            logMinimal(getString("AmqpPlugin.URI.Label")      + " : " + uri);

            factory.setUri(uri);
        } else {
            logMinimal(getString("AmqpPlugin.Username.Label") + " : " + username);
            logDebug(getString("AmqpPlugin.Password.Label") + " : " + password);
            logMinimal(getString("AmqpPlugin.Host.Label")     + " : " + host);
            logMinimal(getString("AmqpPlugin.Port.Label")     + " : " + port);
            logMinimal(getString("AmqpPlugin.Vhost.Label")    + " : " + vhost);

            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);

            if (vhost != null) {
                factory.setVirtualHost(vhost);
            }

            if (meta.isUseSsl()) {
                factory.useSslProtocol();
            }
        }

        conn    = factory.newConnection();
        channel = conn.createChannel();
        int qos = data.isConsumer ? data.prefetchCount : 0;

        channel.basicQos(qos);

        if ( data.isConsumer && data.isTransactional && data.prefetchCount > 0 ) {
            throw new AMQPException(getString("AmqpPlugin.Error.PrefetchCountAndTransactionalNotSupported"));
        }

        if ( data.isConsumer && data.isRequeue ) {
            logMinimal(getString("AmqpPlugin.Info.RequeueDevelopmentUsage"));
        }

        if ( ! conn.isOpen()) {
            throw new AMQPException("Unable to open a AMQP connection");
        }

        if ( ! channel.isOpen()) {
            throw new AMQPException("Unable to open an AMQP channel");
        }

        if (data.isTransactional) {
            getTrans().addTransListener(transListener);
        }

        if (data.isWaitingConsumer) {
            consumer = new QueueingConsumer(channel) {
                @Override
                public void handleCancel(String consumerTag) throws IOException
                {
                    logBasic(consumerTag + "Canceled");
                }

                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig)
                {
                    logDebug(consumerTag + " :SHUTDOWN: " + sig.getMessage());
                }
            };
        }

        if  (data.isConsumer) {

            if ( ! Const.isEmpty(data.ackStepName) || ! Const.isEmpty(data.rejectStepName) )            {
                // we start active confirmation , need not to Ack messages in batches
                data.activeConfirmation = true;
                data.watchedConfirmStep = new ArrayList<StepInterface>();
            }

            //bind to step with acknowledge rows on input stream
            if ( ! Const.isEmpty(data.ackStepName) ) {

            	StepInterface si = getTrans().getStepInterface( data.ackStepName, 0 );
                if (si == null) throw new KettleException ("Can not find step : " + data.ackStepName );

            	StepInterface siTest = getTrans().getStepInterface( data.ackStepName, 1 );
                if (siTest != null) throw new KettleException ("Only SINGLE INSTANCE Steps supported : " + data.ackStepName );

        	    ConfirmationRowStepWatcher rsw = new ConfirmationRowStepWatcher(data.ackStepDeliveryTagField);
                rsw.setAckDelegate(this);
            	si.addRowListener(rsw);

                si.addStepListener(confirmStepListener);
                data.watchedConfirmStep.add(si);

                if (data.isTransactional)  data.ackMsgInTransaction = new ArrayList<Long>();
            }

            //bind to step with rejected rows on input stream
            if ( ! Const.isEmpty(data.rejectStepName) ) {

            	StepInterface si = getTrans().getStepInterface( data.rejectStepName, 0 );
                if (si == null) throw new KettleException ("Can not find step : " + data.rejectStepName );

            	StepInterface siTest = getTrans().getStepInterface( data.rejectStepName, 1 );
                if (siTest != null) throw new KettleException ("Only SINGLE INSTANCE Steps supported : " + data.rejectStepName );

        	    ConfirmationRowStepWatcher rsw = new ConfirmationRowStepWatcher(data.rejectStepDeliveryTagField);
                rsw.setRejectDelegate(this);
            	si.addRowListener(rsw);

                si.addStepListener(confirmStepListener);
                data.watchedConfirmStep.add(si);

                if (data.isTransactional)  data.rejectedMsgInTransaction = new ArrayList<Long>();
            }

        }

        //Consumer Delcare Queue/Exchanges and Binding
        if (data.isConsumer && data.isDeclare) {
            logMinimal(String.format("Declaring Queue '%s' { durable:%s, exclusive:%s, auto_delete:%s}", data.target,  data.isDurable, data.isExclusive, data.isAutodel));
            channel.queueDeclare(data.target, data.isDurable, data.isExclusive, data.isAutodel, null);

            for (Binding item : data.bindings) {
                String queueName    = data.target;
                String exchangeName = environmentSubstitute(item.getTarget());
                String routingKey   = environmentSubstitute(item.getRouting());

                logMinimal(String.format("Binding Queue '%s' to Exchange '%s' using routing key '%s'", queueName, exchangeName, routingKey));
                channel.queueBind(queueName, exchangeName, routingKey);
            }
        }

        // Producer Declare
        if (data.isProducer && data.isDeclare) {
            logMinimal(String.format("Declaring Exchange '%s' {type:%s, durable:%s, auto_delete:%s}", data.target, data.exchtype, data.isDurable, data.isAutodel));
            channel.exchangeDeclare(data.target, data.exchtype, data.isDurable, data.isAutodel, null);

            for (Binding item : data.bindings) {
                String exchangeName = data.target;
                String targetName   = environmentSubstitute(item.getTarget());
                String routingKey   = environmentSubstitute(item.getRouting());
                String targetType   = environmentSubstitute(item.getTargetType());

                if (AMQPPluginData.TARGET_TYPE_QUEUE.equals(targetType))  {
                    logMinimal(String.format("Binding Queue '%s' to Exchange '%s' using routing key '%s'", targetName, exchangeName, routingKey));
                    channel.queueBind(targetName, exchangeName, routingKey);

                    continue;
                }

                logMinimal(String.format("Binding dest Exchange '%s' to Exchange '%s' using routing key '%s'", targetName, exchangeName, routingKey));
                channel.exchangeBind(targetName, exchangeName, routingKey);
            }
        }
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi)
    {
        meta = (AMQPPluginMeta) smi;
        data = (AMQPPluginData) sdi;

        if ( ! data.isTransactional) {
            flush();
        }

        super.dispose(smi, sdi);
    }
}
