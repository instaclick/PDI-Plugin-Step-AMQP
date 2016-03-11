package com.instaclick.pentaho.plugin.amqp;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepInjectionMetaEntry;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

public class AMQPPluginMeta extends BaseStepMeta implements StepMetaInterface
{
    private static final String FIELD_TRANSACTIONAL = "transactional";
    private static final String FIELD_BODY_FIELD = "body_field";
    private static final String FIELD_ROUTING = "routing";
    private static final String FIELD_DELIVERYTAG_FIELD = "deliverytag_field";
    private static final String FIELD_LIMIT = "limit";
    private static final String FIELD_PREFETCHCOUNT = "prefetchCount";
    private static final String FIELD_TARGET = "target";
    private static final String FIELD_MODE = "mode";
    private static final String FIELD_URI = "uri";

    private static final String FIELD_USERNAME = "username";
    private static final String FIELD_PASSWORD = "password";
    private static final String FIELD_HOST = "host";
    private static final String FIELD_PORT = "port";
    private static final String FIELD_VHOST = "vhost";
    private static final String FIELD_USESSL = "usessl";
    private static final String FIELD_BINDING = "binding";
    private static final String FIELD_BINDING_LINE = "line";
    private static final String FIELD_BINDING_LINE_TARGET = "target_value";
    private static final String FIELD_BINDING_LINE_TARGET_TYPE = "target_type_value";
    private static final String FIELD_BINDING_LINE_ROUTING = "routing_value";
    private static final String FIELD_DECLARE = "declare";
    private static final String FIELD_DURABLE = "durable";
    private static final String FIELD_AUTODEL = "autodel";
    private static final String FIELD_EXCHTYPE = "exchtype";
    private static final String FIELD_EXCLUSIVE = "exclusive";
    private static final String FIELD_WAITINGCONSUMER = "waiting_consumer";
    private static final String FIELD_REQUEUE = "requeue";
    private static final String FIELD_WAITTIMEOUT = "waiting_timout";


    private static final String FIELD_ACKSTEPNAME = "ack_stepname";
    private static final String FIELD_ACKDELIVERYTAG_FIELD = "ack_deliverytag_field";
    private static final String FIELD_REJECTSTEPNAME = "reject_stepname";
    private static final String FIELD_REJECTSTEPDELIVERYTAG_FIELD = "reject_deliverytag_field";

    private static final String DEFAULT_BODY_FIELD = "message";
    private static final String DEFAULT_DELIVERYTAG_FIELD = "amqpdeliverytag";
    private static final String DEFAULT_EXCHTYPE   = AMQPPluginData.EXCHTYPE_DIRECT;

    private String uri;
    private String routing;
    private String target;
    private Long limit;
    private Long waitTimeout;
    private int prefetchCount;
    private String username;
    private String password;
    private String host;
    private String port;
    private String vhost;
    private String exchtype;
    private boolean usessl          = false;
    private boolean declare         = false;
    private boolean durable         = true;
    private boolean autodel         = false;
    private boolean requeue         = false;
    private boolean exclusive       = false;
    private boolean waitingConsumer = false;   
    private boolean transactional   = false;
    private String bodyField        = DEFAULT_BODY_FIELD;
    private String deliveryTagField = DEFAULT_DELIVERYTAG_FIELD;
    public String ackStepName                = null;
    public String ackStepDeliveryTagField    = null;
    public String rejectStepName             = null;
    public String rejectStepDeliveryTagField = null;
    private String mode                      = AMQPPluginData.MODE_CONSUMER;

    private final List<AMQPPluginMeta.Binding> bindings = new ArrayList<AMQPPluginMeta.Binding>();

    public static class Binding
    {
        private final String target;
        private final String routing;
        private final String target_type;

        Binding(final String target, final String target_type, final String routing)
        {
            this.target      = target;
            this.routing     = routing;
            this.target_type = target_type;
        }

        public String getTarget()
        {
            return target;
        }

        public String getRouting()
        {
            return routing;
        }
        public String getTargetType()
        {
            return target_type;
        }
    }

    public AMQPPluginMeta()
    {
        super();
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name)
    {
        return new AMQPPluginDialog(shell, meta, transMeta, name);
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp)
    {
        return new AMQPPlugin(stepMeta, stepDataInterface, cnr, transMeta, disp);
    }

    @Override
    public StepDataInterface getStepData()
    {
        return new AMQPPluginData();
    }

    @Override
    public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore ims) throws KettleStepException
    {
        if (AMQPPluginData.MODE_CONSUMER.equals(mode)) {
            // a value meta object contains the meta data for a field
            final ValueMetaInterface b = new ValueMeta(space.environmentSubstitute(getBodyField()), ValueMeta.TYPE_STRING);
            // the name of the step that adds this field
            b.setOrigin(name);
            // modify the row structure and add the field this step generates
            inputRowMeta.addValueMeta(b);

            if ( ! Const.isEmpty(getRouting())) {
                final ValueMetaInterface r = new ValueMeta(space.environmentSubstitute(getRouting()), ValueMeta.TYPE_STRING);
                r.setOrigin(name);
                inputRowMeta.addValueMeta(r);
            }

            if ( ! Const.isEmpty(deliveryTagField)) {
                final ValueMetaInterface r = new ValueMeta(space.environmentSubstitute(getDeliveryTagField()), ValueMeta.TYPE_INTEGER);
                r.setOrigin(name);
                inputRowMeta.addValueMeta(r);
            }
        }
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace vs, Repository rpstr, IMetaStore ims)
    {
        final CheckResult prevSizeCheck = (prev == null || prev.isEmpty())
                ? new CheckResult(CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!", stepMeta)
                : new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is connected to previous one, receiving " + prev.size() + " fields", stepMeta);

        /// See if we have input streams leading to this step!
        final CheckResult inputLengthCheck = (input.length > 0)
                ? new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta)
                : new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta);

        remarks.add(prevSizeCheck);
        remarks.add(inputLengthCheck);
    }

    @Override
    public String getXML()
    {
        final StringBuilder bufer = new StringBuilder();

        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_TRANSACTIONAL, isTransactional()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_BODY_FIELD, getBodyField()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_DELIVERYTAG_FIELD, getDeliveryTagField()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_LIMIT, getLimitString()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_PREFETCHCOUNT, getPrefetchCountString()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_WAITTIMEOUT, getWaitTimeoutString()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_ROUTING, getRouting()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_TARGET, getTarget()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_MODE, getMode()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_URI, getUri()));

        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_USERNAME, getUsername()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_PASSWORD, getPassword()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_HOST, getHost()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_PORT, getPort()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_VHOST, getVhost()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_USESSL, isUseSsl()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_DECLARE, isDeclare()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_DURABLE, isDurable()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_AUTODEL, isAutodel()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_EXCLUSIVE, isExclusive()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_WAITINGCONSUMER, isWaitingConsumer()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_REQUEUE, isRequeue()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_EXCHTYPE, getExchtype()));

        bufer.append("    <" + FIELD_BINDING + ">").append(Const.CR);
        for (Binding item : getBindings()) {
            bufer.append("      <" + FIELD_BINDING_LINE + ">").append(Const.CR);
            bufer.append("        ").append(XMLHandler.addTagValue(FIELD_BINDING_LINE_TARGET, item.getTarget()));
            bufer.append("        ").append(XMLHandler.addTagValue(FIELD_BINDING_LINE_TARGET_TYPE, item.getTargetType()));
            bufer.append("        ").append(XMLHandler.addTagValue(FIELD_BINDING_LINE_ROUTING, item.getRouting()));
            bufer.append("      </" + FIELD_BINDING_LINE + ">").append(Const.CR);
        }
        bufer.append("    </" + FIELD_BINDING + ">").append(Const.CR);


        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_ACKSTEPNAME, getAckStepName()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_ACKDELIVERYTAG_FIELD, getAckStepDeliveryTagField()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_REJECTSTEPNAME, getRejectStepName()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_REJECTSTEPDELIVERYTAG_FIELD, getRejectStepDeliveryTagField()));

        return bufer.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore ims) throws KettleXMLException
    {
        try {
            setTransactional(XMLHandler.getTagValue(stepnode, FIELD_TRANSACTIONAL));
            setBodyField(XMLHandler.getTagValue(stepnode, FIELD_BODY_FIELD));
            setDeliveryTagField(XMLHandler.getTagValue(stepnode, FIELD_DELIVERYTAG_FIELD));
            setRouting(XMLHandler.getTagValue(stepnode, FIELD_ROUTING));
            setTarget(XMLHandler.getTagValue(stepnode, FIELD_TARGET));
            setLimit(XMLHandler.getTagValue(stepnode, FIELD_LIMIT));
            setPrefetchCount(XMLHandler.getTagValue(stepnode, FIELD_PREFETCHCOUNT));
            setWaitTimeout(XMLHandler.getTagValue(stepnode, FIELD_WAITTIMEOUT));
            setMode(XMLHandler.getTagValue(stepnode, FIELD_MODE));
            setUri(XMLHandler.getTagValue(stepnode, FIELD_URI));

            setUsername(XMLHandler.getTagValue(stepnode, FIELD_USERNAME));
            setPassword(XMLHandler.getTagValue(stepnode, FIELD_PASSWORD));
            setHost(XMLHandler.getTagValue(stepnode, FIELD_HOST));
            setPort(XMLHandler.getTagValue(stepnode, FIELD_PORT));
            setVhost(XMLHandler.getTagValue(stepnode, FIELD_VHOST));
            setUseSsl(XMLHandler.getTagValue(stepnode, FIELD_USESSL));
            setDeclare(XMLHandler.getTagValue(stepnode, FIELD_DECLARE));
            setDurable(XMLHandler.getTagValue(stepnode, FIELD_DURABLE));
            setAutodel(XMLHandler.getTagValue(stepnode, FIELD_AUTODEL));
            setExclusive(XMLHandler.getTagValue(stepnode, FIELD_EXCLUSIVE));
            setWaitingConsumer(XMLHandler.getTagValue(stepnode, FIELD_WAITINGCONSUMER));
            setRequeue(XMLHandler.getTagValue(stepnode, FIELD_REQUEUE));
            setExchtype(XMLHandler.getTagValue(stepnode, FIELD_EXCHTYPE));

            Node binding = XMLHandler.getSubNode(stepnode, FIELD_BINDING);
            int count    = XMLHandler.countNodes(binding, FIELD_BINDING_LINE);

            clearBindings();

            for (int i = 0; i < count; i++) {
                Node lnode         = XMLHandler.getSubNodeByNr(binding, FIELD_BINDING_LINE, i);
                String targetItem  = XMLHandler.getTagValue(lnode, FIELD_BINDING_LINE_TARGET);
                String typeItem    = XMLHandler.getTagValue(lnode, FIELD_BINDING_LINE_TARGET_TYPE);
                String routingItem = XMLHandler.getTagValue(lnode, FIELD_BINDING_LINE_ROUTING);
                addBinding(targetItem, typeItem, routingItem);
            }


            setAckStepName(XMLHandler.getTagValue(stepnode, FIELD_ACKSTEPNAME));
            setAckStepDeliveryTagField(XMLHandler.getTagValue(stepnode, FIELD_ACKDELIVERYTAG_FIELD));
            setRejectStepName(XMLHandler.getTagValue(stepnode, FIELD_REJECTSTEPNAME));
            setRejectStepDeliveryTagField(XMLHandler.getTagValue(stepnode, FIELD_REJECTSTEPDELIVERYTAG_FIELD));


        } catch (Exception e) {
            throw new KettleXMLException("Unable to read step info from XML node", e);
        }
    }

    @Override
    public void readRep(Repository rep, IMetaStore ims, ObjectId idStep, List<DatabaseMeta> databases) throws KettleException
    {
        try {
            setTransactional(rep.getStepAttributeString(idStep, FIELD_TRANSACTIONAL));
            setBodyField(rep.getStepAttributeString(idStep, FIELD_BODY_FIELD));
            setDeliveryTagField(rep.getStepAttributeString(idStep, FIELD_DELIVERYTAG_FIELD));
            setRouting(rep.getStepAttributeString(idStep, FIELD_ROUTING));
            setTarget(rep.getStepAttributeString(idStep, FIELD_TARGET));
            setLimit(rep.getStepAttributeString(idStep, FIELD_LIMIT));
            setPrefetchCount(rep.getStepAttributeString(idStep, FIELD_PREFETCHCOUNT));
            setWaitTimeout(rep.getStepAttributeString(idStep, FIELD_WAITTIMEOUT));
            setMode(rep.getStepAttributeString(idStep, FIELD_MODE));
            setUri(rep.getStepAttributeString(idStep, FIELD_URI));

            setUsername(rep.getStepAttributeString(idStep, FIELD_USERNAME));
            setPassword(rep.getStepAttributeString(idStep, FIELD_PASSWORD));
            setHost(rep.getStepAttributeString(idStep, FIELD_HOST));
            setPort(rep.getStepAttributeString(idStep, FIELD_PORT));
            setVhost(rep.getStepAttributeString(idStep, FIELD_VHOST));
            setUseSsl(rep.getStepAttributeString(idStep, FIELD_USESSL));
            setDurable(rep.getStepAttributeString(idStep, FIELD_DURABLE));
            setDeclare(rep.getStepAttributeString(idStep, FIELD_DECLARE));
            setAutodel(rep.getStepAttributeString(idStep, FIELD_AUTODEL));
            setExclusive(rep.getStepAttributeString(idStep, FIELD_EXCLUSIVE));
            setWaitingConsumer(rep.getStepAttributeString(idStep, FIELD_WAITINGCONSUMER));
            setRequeue(rep.getStepAttributeString(idStep, FIELD_REQUEUE));
            setExchtype(rep.getStepAttributeString(idStep, FIELD_EXCHTYPE));

            int nrbindingLines = rep.countNrStepAttributes(idStep, FIELD_BINDING_LINE_TARGET);

            clearBindings();

            for (int i = 0; i < nrbindingLines; i++) {
                String targetItem  = rep.getStepAttributeString(idStep, i, FIELD_BINDING_LINE_TARGET);
                String typeItem    = rep.getStepAttributeString(idStep, i, FIELD_BINDING_LINE_TARGET_TYPE);
                String routingItem = rep.getStepAttributeString(idStep, i, FIELD_BINDING_LINE_ROUTING);
                addBinding(targetItem, typeItem, routingItem);
            }

            setAckStepName(rep.getStepAttributeString(idStep, FIELD_ACKSTEPNAME));
            setAckStepDeliveryTagField(rep.getStepAttributeString(idStep, FIELD_ACKDELIVERYTAG_FIELD));
            setRejectStepName(rep.getStepAttributeString(idStep, FIELD_REJECTSTEPNAME));
            setRejectStepDeliveryTagField(rep.getStepAttributeString(idStep, FIELD_REJECTSTEPDELIVERYTAG_FIELD));


        } catch (KettleDatabaseException dbe) {
            throw new KettleException("error reading step with id_step=" + idStep + " from the repository", dbe);
        } catch (KettleException e) {
            throw new KettleException("Unexpected error reading step with id_step=" + idStep + " from the repository", e);
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore ims, ObjectId idTransformation, ObjectId idStep) throws KettleException
    {
        try {
            rep.saveStepAttribute(idTransformation, idStep, FIELD_TRANSACTIONAL, isTransactional());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_BODY_FIELD, getBodyField());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_DELIVERYTAG_FIELD, getDeliveryTagField());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_LIMIT, getLimitString());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_PREFETCHCOUNT, getPrefetchCountString());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_WAITTIMEOUT, getWaitTimeoutString());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_ROUTING, getRouting());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_TARGET, getTarget());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_MODE, getMode());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_URI, getUri());

            rep.saveStepAttribute(idTransformation, idStep, FIELD_USERNAME, getUsername());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_PASSWORD, getPassword());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_HOST, getHost());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_PORT, getPort());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_VHOST, getVhost());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_USESSL, isUseSsl());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_DECLARE, isDeclare());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_DURABLE, isDurable());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_AUTODEL, isAutodel());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_EXCLUSIVE, isExclusive());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_WAITINGCONSUMER, isWaitingConsumer());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_REQUEUE, isRequeue());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_EXCHTYPE, getExchtype());

            int i = 0;

            for (Binding item : getBindings()) {
                rep.saveStepAttribute(idTransformation, idStep, i, FIELD_BINDING_LINE_TARGET, item.getTarget());
                rep.saveStepAttribute(idTransformation, idStep, i, FIELD_BINDING_LINE_TARGET_TYPE, item.getTargetType());
                rep.saveStepAttribute(idTransformation, idStep, i, FIELD_BINDING_LINE_ROUTING, item.getRouting());
                i++;
            }

            rep.saveStepAttribute(idTransformation, idStep, FIELD_ACKSTEPNAME, getAckStepName());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_ACKDELIVERYTAG_FIELD, getAckStepDeliveryTagField());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_REJECTSTEPNAME, getRejectStepName());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_REJECTSTEPDELIVERYTAG_FIELD, getRejectStepDeliveryTagField());


        } catch (KettleDatabaseException dbe) {
            throw new KettleException("Unable to save step information to the repository, id_step=" + idStep, dbe);
        }
    }

    @Override
    public void setDefault()
    {
        this.uri             = "amqp://guest:guest@localhost:5672";
        this.mode            = AMQPPluginData.MODE_CONSUMER;
        this.bodyField       = DEFAULT_BODY_FIELD;
        this.deliveryTagField = DEFAULT_DELIVERYTAG_FIELD;
        this.exchtype        = DEFAULT_EXCHTYPE;
        this.username        = "";
        this.password        = "";
        this.host            = "";
        this.port            = "";
        this.vhost           = "";
        this.usessl          = false;
        this.declare         = false;
        this.durable         = false;
        this.autodel         = false;
        this.requeue         = false;
        this.exclusive       = false;
        this.transactional   = false;
        this.waitingConsumer = false;
        this.prefetchCount   = 0;
        this.waitTimeout     = 60000L;
        this.ackStepName = "";
        this.ackStepDeliveryTagField = "";
        this.rejectStepName = "";
        this.rejectStepDeliveryTagField = "";

        bindings.clear();
    }

    @Override
    public boolean supportsErrorHandling()
    {
        return false;
    }

    public String getUri()
    {
        return uri;
    }

    public void setUri(String uri)
    {
        this.uri = uri;
    }

    public String getUsername()
    {
        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public String getPort()
    {
        return port;
    }

    public void setPort(String port)
    {
        this.port = port;
    }

    public String getVhost()
    {
        return vhost;
    }

    public void setVhost(String vhost)
    {
        this.vhost = vhost;
    }

    public String getMode()
    {
        return mode;
    }

    public void setMode(String filter)
    {
        this.mode = filter;
    }


    public boolean isConsumer() 
    { 
    return AMQPPluginData.MODE_CONSUMER.equals(getMode()); 
    }

    public boolean isProducer() 
    {
    return AMQPPluginData.MODE_PRODUCER.equals(getMode());
    }


    public String getExchtype()
    {
        return exchtype;
    }

    public void setExchtype(String exchtype)
    {
        this.exchtype = exchtype;
    }

    public boolean isTransactional()
    {
        return transactional;
    }

    public void setTransactional(String transactional)
    {
        this.transactional = Boolean.TRUE.toString().equals(transactional) || "Y".equals(transactional);
    }

    public void setTransactional(boolean transactional)
    {
        this.transactional = transactional;
    }

    public boolean isUseSsl()
    {
        return usessl;
    }

    public void setUseSsl(String usessl)
    {
        this.usessl = Boolean.TRUE.toString().equals(usessl) || "Y".equals(usessl);
    }

    public void setUseSsl(boolean usessl)
    {
        this.usessl = usessl;
    }

    public boolean isDeclare()
    {
        return declare;
    }

    public void setDeclare(String declare)
    {
        this.declare = Boolean.TRUE.toString().equals(declare) || "Y".equals(declare);
    }

    public void setDeclare(boolean declare)
    {
        this.declare = declare;
    }

    public boolean isDurable()
    {
        return durable;
    }

    public void setDurable(String durable)
    {
        this.durable = Boolean.TRUE.toString().equals(durable) || "Y".equals(durable);
    }

    public void setDurable(boolean durable)
    {
        this.durable = durable;
    }

    public boolean isAutodel()
    {
        return autodel;
    }

    public void setAutodel(String autodel)
    {
        this.autodel = Boolean.TRUE.toString().equals(autodel) || "Y".equals(autodel);
    }

    public void setAutodel(boolean autodel)
    {
        this.autodel = autodel;
    }

    public boolean isExclusive()
    {
        return exclusive;
    }

    public void setExclusive(String exclusive)
    {
        this.exclusive = Boolean.TRUE.toString().equals(exclusive) || "Y".equals(exclusive);
    }

    public void setExclusive(boolean exclusive)
    {
        this.exclusive = exclusive;
    }

    public boolean isWaitingConsumer()
    {
        return waitingConsumer;
    }


    public void setWaitingConsumer(String val)
    {
        this.waitingConsumer = Boolean.TRUE.toString().equals(val) || "Y".equals(val);
    }

    public void setWaitingConsumer(boolean val)
    {
        this.waitingConsumer = val;
    }

    public boolean isRequeue()
    {
        return requeue;
    }


    public void setRequeue(String val)
    {
        this.requeue = Boolean.TRUE.toString().equals(val) || "Y".equals(val);
    }

    public void setRequeue(boolean val)
    {
        this.requeue = val;
    }

    public String getBodyField()
    {
        return bodyField;
    }

    public void setBodyField(String uniqueRowName)
    {
        this.bodyField = uniqueRowName;
    }

    public String getTarget()
    {
        if (Const.isEmpty(target) && AMQPPluginData.MODE_CONSUMER.equals(mode)) {
            target = "queue_name";
        }

        if (Const.isEmpty(target) && AMQPPluginData.MODE_PRODUCER.equals(mode)) {
            target = "";
        }

        return target;
    }

    public void setTarget(String target)
    {
        this.target = target;
    }

    public String getRouting()
    {
        return routing;
    }

    public void setRouting(String routing)
    {
        this.routing = routing;
    }


    public String getDeliveryTagField()
    {
        return deliveryTagField;
    }

    public void setDeliveryTagField(String val)
    {
        this.deliveryTagField = val;
    }

    public String getAckStepName()
    {
        return ackStepName;
    }

    public void setAckStepName(String val)
    {
        this.ackStepName = val;
    }


    public String getAckStepDeliveryTagField()
    {
        return ackStepDeliveryTagField;
    }

    public void setAckStepDeliveryTagField(String val)
    {
        this.ackStepDeliveryTagField = val;
    }

    public String getRejectStepName()
    {
        return rejectStepName;
    }

    public void setRejectStepName(String val)
    {
        this.rejectStepName = val;
    }


    public String getRejectStepDeliveryTagField()
    {
        return rejectStepDeliveryTagField;
    }

    public void setRejectStepDeliveryTagField(String val)
    {
        this.rejectStepDeliveryTagField = val;
    }


    public Long getLimit()
    {
        if (limit == null) {
            limit = 10000L;
        }

        return limit;
    }

    public String getLimitString()
    {
        return String.valueOf(getLimit());
    }

    public void setLimit(Long limit)
    {
        this.limit = limit;
    }

    public void setLimit(String limit)
    {
        this.limit = null;

        if (Const.isEmpty(limit)) {
            return;
        }

        this.limit = Long.parseLong(limit);
    }

    public Long getWaitTimeout()
    {
        if (waitTimeout == null) {
            waitTimeout = 60000L;
        }

        return waitTimeout;
    }

    public String getWaitTimeoutString()
    {
        return String.valueOf(getWaitTimeout());
    }

    public void setWaitTimeout(Long waitTimeout)
    {
        this.waitTimeout = waitTimeout;
    }

    public void setWaitTimeout(String waitTimeout)
    {
        this.waitTimeout = null;

        if (Const.isEmpty(waitTimeout)) {
            return;
        }

        this.waitTimeout = Long.parseLong(waitTimeout);
    }

    public int getPrefetchCount()
    {
        return prefetchCount;
    }

    public String getPrefetchCountString()
    {
        return String.valueOf(getPrefetchCount());
    }

    public void setPrefetchCount(int prefetchCount)
    {
        this.prefetchCount = prefetchCount;
    }

    public void setPrefetchCount(String prefetchCount)
    {
        this.prefetchCount = 0;

        if (Const.isEmpty(prefetchCount)) {
            return;
        }

        this.prefetchCount = Integer.parseInt(prefetchCount);
    }

    public void addBinding(String target, String target_type, String routing)
    {
        this.bindings.add(new AMQPPluginMeta.Binding(target, target_type, routing));
    }

    public List<AMQPPluginMeta.Binding> getBindings()
    {
        return this.bindings;
    }

    public void clearBindings()
    {
        this.bindings.clear();
    }



  @Override
  public AMQPPluginMetaInjection getStepMetaInjectionInterface() {
    return new AMQPPluginMetaInjection( this );
  }

  public List<StepInjectionMetaEntry> extractStepMetadataEntries() throws KettleException {
    return getStepMetaInjectionInterface().extractStepMetadataEntries();
  }

}
