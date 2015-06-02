package com.instaclick.pentaho.plugin.amqp;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import java.util.List;

public class AMQPPluginData extends BaseStepData implements StepDataInterface
{
    public static final String MODE_PRODUCER = "producer";
    public static final String MODE_CONSUMER = "consumer";

    public static final String TARGET_TYPE_EXCHANGE = "exchange";
    public static final String TARGET_TYPE_QUEUE = "queue";

    public static final String EXCHTYPE_FANOUT = "fanout";
    public static final String EXCHTYPE_DIRECT = "direct";
    public static final String EXCHTYPE_HEADERS = "headers";
    public static final String EXCHTYPE_TOPIC = "topic";

    public List<AMQPPluginMeta.Binding> bindings;
    public RowMetaInterface outputRowMeta;
    public Integer bodyFieldIndex = null;
    public Integer routingIndex = null;

    public boolean isTransactional = false;
    public boolean isProducer = false;
    public boolean isConsumer = false;
    public boolean isTxOpen = false;
    public boolean isDeclare = false;
    public boolean isDurable = true;
    public boolean isAutodel = false;
    public boolean isExclusive = false;
    public boolean isWaitingConsumer = false;
    public boolean isRequeue = false;
    public String exchtype = "";
    public String body = null;
    public String routing;
    public String target;
    public long amqpTag;
    public Long limit;
    public Long waitTimeout;
    public int prefetchCount=0;
    public long count = 0;

    // Delayed confirmation realted
    public Integer deliveryTagIndex = null;

    public String ackStepName = null;
    public String ackStepDeliveryTagField = null;

    public String rejectStepName = null;
    public String rejectStepDeliveryTagField = null;
    
    public String requeueStepName = null;
    public String requeueStepDeliveryTagField = null;


    public AMQPPluginData()
    {
        super();
    }
}
