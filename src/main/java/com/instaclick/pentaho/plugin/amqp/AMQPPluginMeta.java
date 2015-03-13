package com.instaclick.pentaho.plugin.amqp;

import java.util.List;
import java.util.Map;

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
import org.w3c.dom.Node;

public class AMQPPluginMeta extends BaseStepMeta implements StepMetaInterface
{
    private static final String FIELD_TRANSACTIONAL = "transactional";
    private static final String FIELD_BODY_FIELD    = "body_field";
    private static final String FIELD_ROUTING       = "routing";
    private static final String FIELD_LIMIT         = "limit";
    private static final String FIELD_TARGET        = "target";
    private static final String FIELD_MODE          = "mode";
    private static final String FIELD_URI           = "uri";

    private static final String FIELD_USERNAME      = "username";
    private static final String FIELD_PASSWORD      = "password";
    private static final String FIELD_HOST          = "host";
    private static final String FIELD_PORT          = "port";
    private static final String FIELD_VHOST         = "vhost";
    private static final String FIELD_USESSL = "usessl";
    private static final String FIELD_BINDING = "binding";
    private static final String FIELD_BINDING_LINE = "line";
    private static final String FIELD_BINDING_LINE_EXCHANGE = "exchange_value";
    private static final String FIELD_BINDING_LINE_ROUTING = "routing_value";
    private static final String FIELD_BINDING_LINE_EXCHTYPE = "exchtype_value";
    private static final String FIELD_DECLARE = "declare";
    private static final String FIELD_DURABLE = "durable";
    private static final String FIELD_AUTODEL = "autodel";
    private static final String FIELD_EXCHTYPE = "exchtype";
    private static final String FIELD_EXCLUSIVE = "exclusive";

    private static final String DEFAULT_USERNAME    = "guest";
    private static final String DEFAULT_PASSWORD    = "guest";
    private static final String DEFAULT_HOST        = "localhost";
    private static final String DEFAULT_PORT        = "5672";
    private static final String DEFAULT_VHOST       = "/";
    private static final String DEFAULT_USESSL       = "N";
    private static final String DEFAULT_DECLARE       = "N";
    private static final String DEFAULT_DURABLE       = "Y";
    private static final String DEFAULT_AUTODEL       = "N";
    private static final String DEFAULT_EXCHTYPE       = AMQPPluginData.EXCHTYPE_DIRECT;
    private static final String DEFAULT_EXCLUSIVE       = "N";

    private static final String DEFAULT_URI = "amqp://guest:guest@localhost:5672";
    private static final String DEFAULT_BODY_FIELD = "message";

    private boolean transactional   = false;
    private String bodyField        = DEFAULT_BODY_FIELD;
    private String mode             = AMQPPluginData.MODE_CONSUMER;
    private String uri              = DEFAULT_URI;
    private String routing;
    private String target;
    private Long limit;
    private String username         = DEFAULT_USERNAME;
    private String password         = DEFAULT_PASSWORD;
    private String host             = DEFAULT_HOST;
    private String port             = DEFAULT_PORT;
    private String vhost            = DEFAULT_VHOST;
    private boolean usessl   = false;
    private boolean declare   = false;
    private boolean durable   = true;
    private boolean autodel   = false;
    private String exchtype   = DEFAULT_EXCHTYPE;
    private boolean exclusive   = false;


    private String bindingExchangeValue[];
    private String bindingExchtypeValue[];
    private String bindingRoutingValue[];

    public AMQPPluginMeta() {
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
    public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) throws KettleStepException
    {
        if (AMQPPluginData.MODE_CONSUMER.equals(mode)) {
            // a value meta object contains the meta data for a field
            final ValueMetaInterface b = new ValueMeta(getBodyField(), ValueMeta.TYPE_STRING);
            // the name of the step that adds this field
            b.setOrigin(name);
            // modify the row structure and add the field this step generates
            inputRowMeta.addValueMeta(b);

            if ( ! Const.isEmpty(routing)) {
                final ValueMetaInterface r = new ValueMeta(routing, ValueMeta.TYPE_STRING);
                r.setOrigin(name);
                inputRowMeta.addValueMeta(r);
            }
        }
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info)
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
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_LIMIT, getLimitString()));
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
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_EXCHTYPE, getExchtype()));

	bufer.append("    <"+FIELD_BINDING+">").append(Const.CR); 
		
	for (int i=0;i<bindingExchangeValue.length;i++)
	{
		bufer.append("      <"+FIELD_BINDING_LINE+">").append(Const.CR); 
		bufer.append("        ").append(XMLHandler.addTagValue(FIELD_BINDING_LINE_EXCHANGE, bindingExchangeValue[i])); 
		bufer.append("        ").append(XMLHandler.addTagValue(FIELD_BINDING_LINE_EXCHTYPE, bindingExchtypeValue[i])); 
		bufer.append("        ").append(XMLHandler.addTagValue(FIELD_BINDING_LINE_ROUTING, bindingRoutingValue[i])); 
		bufer.append("      </"+FIELD_BINDING_LINE+">").append(Const.CR); 
	}
	bufer.append("    </"+FIELD_BINDING+">").append(Const.CR); 

        return bufer.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleXMLException
    {
        try {
            setTransactional(XMLHandler.getTagValue(stepnode, FIELD_TRANSACTIONAL));
            setBodyField(XMLHandler.getTagValue(stepnode, FIELD_BODY_FIELD));
            setRouting(XMLHandler.getTagValue(stepnode, FIELD_ROUTING));
            setTarget(XMLHandler.getTagValue(stepnode, FIELD_TARGET));
            setLimit(XMLHandler.getTagValue(stepnode, FIELD_LIMIT));
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
            setExchtype(XMLHandler.getTagValue(stepnode, FIELD_EXCHTYPE));

	    Node binding = XMLHandler.getSubNode(stepnode, FIELD_BINDING); //$NON-NLS-1$
	    int count   = XMLHandler.countNodes(binding, FIELD_BINDING_LINE); //$NON-NLS-1$
	
	    allocateBinding(count);
					
	    for (int i=0;i<count;i++)
	    {
		Node lnode = XMLHandler.getSubNodeByNr(binding, FIELD_BINDING_LINE, i);
			
		bindingExchangeValue[i] = XMLHandler.getTagValue(lnode, FIELD_BINDING_LINE_EXCHANGE); 
		bindingExchtypeValue[i] = XMLHandler.getTagValue(lnode, FIELD_BINDING_LINE_EXCHTYPE); 
	        bindingRoutingValue[i] = XMLHandler.getTagValue(lnode, FIELD_BINDING_LINE_ROUTING);
            }


        } catch (Exception e) {
            throw new KettleXMLException("Unable to read step info from XML node", e);
        }
    }

    @Override
    public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleException
    {
        try {
            setTransactional(rep.getStepAttributeString(idStep, FIELD_TRANSACTIONAL));
            setBodyField(rep.getStepAttributeString(idStep, FIELD_BODY_FIELD));
            setRouting(rep.getStepAttributeString(idStep, FIELD_ROUTING));
            setTarget(rep.getStepAttributeString(idStep, FIELD_TARGET));
            setLimit(rep.getStepAttributeString(idStep, FIELD_LIMIT));
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
            setExchtype(rep.getStepAttributeString(idStep, FIELD_EXCHTYPE));


	    int nrbindingLines = rep.countNrStepAttributes(idStep, FIELD_BINDING_LINE_EXCHANGE); //$NON-NLS-1$
			
	    allocateBinding(nrbindingLines);
	
	    for (int i=0;i<nrbindingLines;i++)
	    {
		bindingExchangeValue[i] = rep.getStepAttributeString(idStep, i, FIELD_BINDING_LINE_EXCHANGE); //$NON-NLS-1$
		bindingExchtypeValue[i] = rep.getStepAttributeString(idStep, i, FIELD_BINDING_LINE_EXCHTYPE); //$NON-NLS-1$
		bindingRoutingValue[i] = rep.getStepAttributeString(idStep, i, FIELD_BINDING_LINE_ROUTING); //$NON-NLS-1$
	    }

        } catch (KettleDatabaseException dbe) {
            throw new KettleException("error reading step with id_step=" + idStep + " from the repository", dbe);
        } catch (KettleException e) {
            throw new KettleException("Unexpected error reading step with id_step=" + idStep + " from the repository", e);
        }
    }

    @Override
    public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep) throws KettleException
    {
        try {
            rep.saveStepAttribute(idTransformation, idStep, FIELD_TRANSACTIONAL, isTransactional());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_BODY_FIELD, getBodyField());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_LIMIT, getLimitString());
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
            rep.saveStepAttribute(idTransformation, idStep, FIELD_EXCHTYPE, getExchtype());

	    for (int i=0;i<bindingExchangeValue.length;i++)
	    {
		rep.saveStepAttribute(idTransformation, idStep, i, FIELD_BINDING_LINE_EXCHANGE, bindingExchangeValue[i]); //$NON-NLS-1$
		rep.saveStepAttribute(idTransformation, idStep, i, FIELD_BINDING_LINE_EXCHTYPE, bindingExchtypeValue[i]); //$NON-NLS-1$
		rep.saveStepAttribute(idTransformation, idStep, i, FIELD_BINDING_LINE_ROUTING,  bindingRoutingValue[i]); //$NON-NLS-1$
	    }


        } catch (KettleDatabaseException dbe) {
            throw new KettleException("Unable to save step information to the repository, id_step=" + idStep, dbe);
        }
    }

    @Override
    public void setDefault()
    {
        this.mode           = AMQPPluginData.MODE_CONSUMER;
        this.exchtype	= DEFAULT_EXCHTYPE;
        this.bodyField      = DEFAULT_BODY_FIELD;
        this.uri            = DEFAULT_URI;

        this.username            = DEFAULT_USERNAME;
        this.password            = DEFAULT_PASSWORD;
        this.host            = DEFAULT_HOST;
        this.port            = DEFAULT_PORT;
        this.vhost            = DEFAULT_VHOST;
        this.usessl	= false;
        this.declare	= false;
        this.durable	= false;
        this.autodel	= false;
        this.exclusive	= false;

        this.transactional  = false;

	int bindingCount=0;
	allocateBinding(bindingCount);
    }

    @Override
    public boolean supportsErrorHandling() 
    {
        return false;
    }

    public String getUri()
    {
        if (Const.isEmpty(uri)) {
	    if (Const.isEmpty(host)) {
            	uri = DEFAULT_URI; 
	    } else uri = "";
        }
	
        return uri;
    }

    public void setUri(String uri)
    {
        this.uri = uri;
    }


    public String getUsername()
    {
        if (Const.isEmpty(username)) {
            username = DEFAULT_USERNAME;
        }

        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public String getPassword()
    {
        if (Const.isEmpty(password)) {
            password = DEFAULT_PASSWORD;
        }

        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getHost()
    {
        if (Const.isEmpty(host)) {
	    if (Const.isEmpty(uri)) {
            	host = DEFAULT_HOST; 
	    } else host = "";
        }

        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }


    public String getPort()
    {
        if (Const.isEmpty(port)) {
            port = DEFAULT_PORT;
        }

        return port;
    }

    public void setPort(String port)
    {
        this.port = port;
    }


    public String getVhost()
    {
        if (Const.isEmpty(vhost)) {
            vhost = DEFAULT_VHOST;
        }

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

    public void setExclusive(String declare)
    {
        this.exclusive = Boolean.TRUE.toString().equals(exclusive) || "Y".equals(exclusive);
    }

    public void setExclusive(boolean exclusive)
    {
        this.exclusive = exclusive;
    }




    public String getBodyField()
    {
        if (Const.isEmpty(bodyField)) {
            bodyField = DEFAULT_BODY_FIELD;
        }

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
            target = "exchange_name";
        }

        if (Const.isEmpty(target)) {
            target = "exchange_or_queue_name";
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

    public Long getLimit()
    {
        return limit;
    }

    public String getLimitString()
    {
        if (limit == null) {
            return "";
        }

        return String.valueOf(limit);
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



    public void allocateBinding(int count)
    {
	bindingExchangeValue  = new String[count];
	bindingExchtypeValue  = new String[count];
	bindingRoutingValue = new String[count];
    }

    public String[] getBindingExchangeValue()
    {
        return bindingExchangeValue;
    }
    
    public void setBindingExchangeValue(String[] fieldName)
    {
        this.bindingExchangeValue = fieldName;
    }


    public String[] getBindingExchtypeValue()
    {
        return bindingExchtypeValue;
    }
    
    public void setBindingExchtypeValue(String[] fieldName)
    {
        this.bindingExchtypeValue = fieldName;
    }
 
    public String[] getBindingRoutingValue()
    {
        return bindingRoutingValue;
    }
    
    public void setBindingRoutingValue(String[] fieldValue)
    {
        this.bindingRoutingValue = fieldValue;
    }

}
