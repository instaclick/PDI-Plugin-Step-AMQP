package com.instaclick.pentaho.plugin.amqp;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class AMQPPluginData extends BaseStepData implements StepDataInterface
{
    public static final String MODE_PRODUCER = "producer";
    public static final String MODE_CONSUMER = "consumer";

    public RowMetaInterface outputRowMeta;
    public Integer bodyFieldIndex = null;
    public Integer routingIndex = null;
    public boolean isTransactional = false;
    public boolean isProducer = false;
    public boolean isConsumer = false;
    public boolean isTxOpen = false;
    public String body = null;
    public String routing;
    public String target;
    public String uri;
    public long amqpTag;
    public Long limit;
    public long count = 0;

    public AMQPPluginData()
    {
        super();
    }
}
