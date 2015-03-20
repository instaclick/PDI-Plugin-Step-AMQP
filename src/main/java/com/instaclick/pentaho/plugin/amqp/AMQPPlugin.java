package com.instaclick.pentaho.plugin.amqp;

import com.instaclick.pentaho.plugin.amqp.AMQPPluginMeta.Binding;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
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
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.TransListener;

public class AMQPPlugin extends BaseStep implements StepInterface
{
    private AMQPPluginData data;
    private AMQPPluginMeta meta;

    private ConnectionFactory factory = new ConnectionFactory();
    private Connection conn = null;
    private Channel channel = null;

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
            if (!data.isTransactional) {
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

    public AMQPPlugin(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
    {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException, KettleStepException
    {
        meta = (AMQPPluginMeta) smi;
        data = (AMQPPluginData) sdi;
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
        if (data.isTransactional && !data.isTxOpen) {
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

    private void consume() throws IOException, KettleStepException, InterruptedException
    {
        GetResponse response 		= null;
	QueueingConsumer consumer 	= null;
        if (data.isWaitingConsumer) { 
		logMinimal("WAITING FOR MESSAGES");
		consumer = new QueueingConsumer(channel) {
		   @Override
		  public void handleCancel(String consumerTag) throws IOException {
			logMinimal(consumerTag + "Canceled");
		  }
		  public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
			logMinimal(consumerTag + " :SHUTDOWN: " + sig.getMessage() );
		  }
		};
		channel.basicConsume(data.target, false, consumer);
	}
        do {
	    if (data.isWaitingConsumer ) {
		    QueueingConsumer.Delivery delivery = null;
		    while (!isStopped() && delivery == null && !isStepTimedoutAsConsumer() ) {  delivery = consumer.nextDelivery(10000); } ;
	            
		    if (delivery == null  ) {
		        return;
		    }

	            final byte[] body = delivery.getBody();
	            final long tag    = delivery.getEnvelope().getDeliveryTag();

	            data.body    = new String(body);
	            data.routing = delivery.getEnvelope().getRoutingKey();
	            data.count ++;		    
	            data.amqpTag = tag;
		    logRowlevel("Delivery tag: " + data.amqpTag, data);

	    } else {
	            response = channel.basicGet(data.target, false);

        	    if (response == null) {
	                return;
	            }

	            final byte[] body = response.getBody();
	            final long tag    = response.getEnvelope().getDeliveryTag();

	            data.body    = new String(body);
	            data.routing = response.getEnvelope().getRoutingKey();
	            data.count ++;
	            data.amqpTag = tag;

	    }

            // safely add the unique field at the end of the output row
            Object[] r = RowDataUtil.allocateRowData(data.outputRowMeta.size());

            r[data.bodyFieldIndex] = data.body;
            r[data.routingIndex]   = data.routing;

            // put the row to the output row stream
            putRow(data.outputRowMeta, r);

            // log progress
            if (checkFeedback(getLinesWritten())) {
                logBasic(String.format("liner %s", getLinesWritten()));
            }

            if ( ! data.isTransactional) {
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

        if (data.isConsumer && data.isTransactional) {
            try {

                logMinimal("Ack messages : " + data.amqpTag);

                channel.basicAck(data.amqpTag, true);
            } catch (IOException ex) {
                logError(ex.getMessage());
            }
        }

        if (data.isProducer && data.isTransactional && data.isTxOpen) {
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
        if (channel != null) {

            if (data.isProducer && data.isTransactional && data.isTxOpen) {
                try {
                    logMinimal("Rollback channel transaction");
                    channel.txRollback();

                    data.isTxOpen = false;
                } catch (IOException ex) {
                    logError(ex.getMessage());
                }
            }

            try {
                logMinimal("Closing AMQP channel");
                channel.close();
            } catch (IOException ex) {
                logError(ex.getMessage());
            }
        }

        if (conn != null) {
            try {
                logMinimal("Closing AMQP connection");
                conn.close();
            } catch (IOException ex) {
                logError(ex.getMessage());
            }
        }
    }

    public boolean isStepTimedoutAsConsumer() 
    {
     	return data.waitTimeout < getRuntime() && data.waitTimeout != 0 && data.isConsumer;
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

        String body     = environmentSubstitute(meta.getBodyField());
        String routing  = environmentSubstitute(meta.getRouting());
        String uri      = environmentSubstitute(meta.getUri());
        String username = environmentSubstitute(meta.getUsername());
        String password = org.pentaho.di.core.encryption.KettleTwoWayPasswordEncoder.decryptPasswordOptionallyEncrypted(environmentSubstitute(meta.getPassword()));
        String host     = environmentSubstitute(meta.getHost());
        String vhost    = environmentSubstitute(meta.getVhost());
        Integer port    = null;

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

        data.isTransactional = meta.isTransactional();
        data.isWaitingConsumer = meta.isWaitingConsumer();
        data.isConsumer      = meta.isConsumer();
        data.isProducer      = meta.isProducer();

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
            logMinimal(getString("AmqpPlugin.Password.Label") + " : " + password);
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

	if (data.isConsumer) {
		channel.basicQos(data.prefetchCount);
	} else {
	        channel.basicQos(0);
	}

	if ( data.isConsumer && data.isTransactional && data.prefetchCount > 0 ) {
            throw new AMQPException("AmqpPlugin.Error.PrefetchCountAndTransactionalNotSupported");
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

        //Consumer Delcare Queue/Exchanges and Binding
        if (data.isConsumer && data.isDeclare) {
            logMinimal(String.format("Declaring Queue '%s' { durable:%s, exclusive:%s, auto_delete:%s}", data.target,  data.isDurable, data.isExclusive, data.isAutodel));
            channel.queueDeclare(data.target, data.isDurable, data.isExclusive, data.isAutodel, null);

            for (Binding item : data.bindings) {
                String queueName   = data.target;
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
                String targetName    = environmentSubstitute(item.getTarget());
                String targetType    = environmentSubstitute(item.getTargetType());
                String routingKey   = environmentSubstitute(item.getRouting());

	        if ( AMQPPluginData.TARGET_TYPE_QUEUE.equals(targetType) )  {
			logMinimal(String.format("Binding Queue '%s' to Exchange '%s' using routing key '%s'", targetName, exchangeName, routingKey));
                	channel.queueBind(targetName, exchangeName, routingKey);
		} else {
                	logMinimal(String.format("Binding dest Exchange '%s' to Exchange '%s' using routing key '%s'", targetName, exchangeName, routingKey));
                	channel.exchangeBind(targetName, exchangeName, routingKey);
		}

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
