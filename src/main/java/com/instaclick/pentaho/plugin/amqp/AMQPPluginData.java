package com.instaclick.pentaho.plugin.amqp;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class AMQPPluginData extends BaseStepData implements StepDataInterface
{
    public static final String MODE_PRODUCER = "producer";
    public static final String MODE_CONSUMER = "consumer";

    public static final String EXCHTYPE_FANOUT = "fanout";
    public static final String EXCHTYPE_DIRECT = "direct";
    public static final String EXCHTYPE_HEADERS = "headers";
    public static final String EXCHTYPE_TOPIC = "topic";


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
    public String exchtype = "";
    public String body = null;
    public String routing;
    public String target;
    public long amqpTag;
    public Long limit;
    public long count = 0;


    public AMQPPluginMeta.Binding bindings[];

    public void allocateBinding(int count)
    {
	this.bindings  = new AMQPPluginMeta.Binding[count];
    }

    public AMQPPluginData()
    {
        super();
    }
}
