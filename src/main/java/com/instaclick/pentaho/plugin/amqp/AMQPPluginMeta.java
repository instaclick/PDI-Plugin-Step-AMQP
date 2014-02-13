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

    private static final String DEFAULT_URI = "amqp://guest:guest@localhost:5672";
    private static final String DEFAULT_BODY_FIELD = "message";

    private boolean transactional   = false;
    private String bodyField        = DEFAULT_BODY_FIELD;
    private String mode             = AMQPPluginData.MODE_CONSUMER;
    private String uri              = DEFAULT_URI;
    private String routing;
    private String target;
    private Long limit;


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

        } catch (KettleDatabaseException dbe) {
            throw new KettleException("Unable to save step information to the repository, id_step=" + idStep, dbe);
        }
    }

    @Override
    public void setDefault()
    {
        this.mode           = AMQPPluginData.MODE_CONSUMER;
        this.bodyField      = DEFAULT_BODY_FIELD;
        this.uri            = DEFAULT_URI;
        this.transactional  = false;
    }

    @Override
    public boolean supportsErrorHandling() 
    {
        return false;
    }

    public String getUri()
    {
        if (Const.isEmpty(uri)) {
            uri = DEFAULT_URI;
        }

        return uri;
    }

    public void setUri(String uri)
    {
        this.uri = uri;
    }

    public String getMode()
    {
        return mode;
    }

    public void setMode(String filter)
    {
        this.mode = filter;
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
}
