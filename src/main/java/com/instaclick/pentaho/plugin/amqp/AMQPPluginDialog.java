package com.instaclick.pentaho.plugin.amqp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.i18n.BaseMessages;

import org.pentaho.di.core.Const;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.core.widget.TextVar;
import static com.instaclick.pentaho.plugin.amqp.Messages.getString;

public class AMQPPluginDialog extends BaseStepDialog implements StepDialogInterface
{
    private AMQPPluginMeta input;

    private Label labelURI;
    private TextVar textURI;
    private FormData formURILabel;
    private FormData formURIText;

    private Label labelUsername;
    private TextVar textUsername;
    private FormData formUsernameLabel;
    private FormData formUsernameText;

    private Label  labelPassword;
    private TextVar textPassword;
    private FormData formPasswordLabel;
    private FormData formPasswordText;

    private Label labelHost;
    private TextVar textHost;
    private FormData formHostLabel;
    private FormData formHostText;

    private Label labelPort;
    private TextVar textPort;
    private FormData formPortLabel;
    private FormData formPortText;

    private Label labelVhost;
    private TextVar textVhost;
    private FormData formVhostLabel;
    private FormData formVhostText;

    private Label labelUseSsl;
    private Button checkUseSsl;
    private FormData formUseSslLabel;
    private FormData formUseSslText;

    private Label labelDeclare;
    private Button checkDeclare;
    private FormData formDeclareLabel;
    private FormData formDeclareText;

    private Label labelDurable;
    private Button checkDurable;
    private FormData formDurableLabel;
    private FormData formDurableText;

    private Label labelAutodel;
    private Button checkAutodel;
    private FormData formAutodelLabel;
    private FormData formAutodelText;

    private Label labelExclusive;
    private Button checkExclusive;
    private FormData formExclusiveLabel;
    private FormData formExclusiveText;

    private Label    labelMode;
    private CCombo   comboMode;
    private FormData formModeLabel;
    private FormData formModeCombo;

    private Label    labelExchtype;
    private CCombo   comboExchtype;
    private FormData formExchtypeLabel;
    private FormData formExchtypeCombo;

    private Label labelTarget;
    private TextVar textTarget;
    private FormData formTargetLabel;
    private FormData formTargetText;

    private Label labelRouting;
    private TextVar textRouting;
    private FormData formRoutingLabel;
    private FormData formRoutingText;

    private Label labelLimit;
    private Text textLimit;
    private FormData formLimitLabel;
    private FormData formLimitText;

    private Label labelBodyField;
    private TextVar textBodyField;
    private FormData formBodyLabel;
    private FormData formBodyText;

    private Label labelTransactional;
    private Button checkTransactional;
    private FormData formTransactionalLabel;
    private FormData formTransactionalText;


    private Label        wlFields;
    private TableView    wBinding;
    private FormData     fdlFields, fdFields;

    private static final List<String> modes = new ArrayList<String>(Arrays.asList(new String[] {
        AMQPPluginData.MODE_CONSUMER,
        AMQPPluginData.MODE_PRODUCER
    }));

    private static final List<String> exchtypes = new ArrayList<String>(Arrays.asList(new String[] {
        AMQPPluginData.EXCHTYPE_FANOUT,
        AMQPPluginData.EXCHTYPE_DIRECT,
        AMQPPluginData.EXCHTYPE_HEADERS,
        AMQPPluginData.EXCHTYPE_TOPIC
    }));


    private final ModifyListener modifyListener = new ModifyListener() {
        @Override
        public void modifyText(ModifyEvent e) {
            input.setChanged();
        }
    };

    private final SelectionAdapter selectionModifyListener = new SelectionAdapter() {
        @Override
        public void widgetSelected(SelectionEvent e) {
            input.setChanged();
        }
    };

    private final SelectionAdapter comboModeListener = new SelectionAdapter() {
        @Override
        public void widgetSelected(SelectionEvent e) {

            if (AMQPPluginData.MODE_PRODUCER.equals(comboMode.getText())) toProducerMode();
            if (AMQPPluginData.MODE_CONSUMER.equals(comboMode.getText())) toConsumerMode();
        }
    };

    private final SelectionAdapter comboExchtypeListener = new SelectionAdapter() {
        @Override
        public void widgetSelected(SelectionEvent e) {
        }
    };


    public AMQPPluginDialog(Shell parent, Object in, TransMeta transMeta, String sname)
    {
        super(parent, (BaseStepMeta) in, transMeta, sname);

        input = (AMQPPluginMeta) in;
    }

    private void toProducerMode() {
        textLimit.setVisible(false);
        labelLimit.setVisible(false);
	comboExchtype.setVisible(true);
	labelExchtype.setVisible(true);
        labelTarget.setText(getString("AmqpPlugin.Exchange.Label"));
    }

    private void toConsumerMode() {
        textLimit.setVisible(true);
        labelLimit.setVisible(true);
	comboExchtype.setVisible(false);
	labelExchtype.setVisible(false);	
        labelTarget.setText(getString("AmqpPlugin.Queue.Label"));
    }

    @Override
    public String open()
    {
        Shell parent    = getParent();
        Display display = parent.getDisplay();
        shell           = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

        props.setLook(shell);
        setShellImage(shell, input);

        changed = input.hasChanged();

        FormLayout formLayout   = new FormLayout();
        formLayout.marginWidth  = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(getString("AmqpPlugin.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Stepname line
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(getString("AmqpPlugin.StepName.Label"));
        props.setLook(wlStepname);

        fdlStepname         = new FormData();
        fdlStepname.left    = new FormAttachment(0, 0);
        fdlStepname.right   = new FormAttachment(middle, -margin);
        fdlStepname.top     = new FormAttachment(0, margin);

        wlStepname.setLayoutData(fdlStepname);
        wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        wStepname.setText(stepname);
        props.setLook(wStepname);
        wStepname.addModifyListener(modifyListener);

        fdStepname       = new FormData();
        fdStepname.left  = new FormAttachment(middle, 0);
        fdStepname.top   = new FormAttachment(0, margin);
        fdStepname.right = new FormAttachment(100, 0);

        wStepname.setLayoutData(fdStepname);

        // Mode
        labelMode = new Label(shell, SWT.RIGHT);
        labelMode.setText(getString("AmqpPlugin.Type.Label"));
        props.setLook(labelMode);

        formModeLabel       = new FormData();
        formModeLabel.left  = new FormAttachment(0, 0);
        formModeLabel.top   = new FormAttachment(wStepname, margin);
        formModeLabel.right = new FormAttachment(middle, 0);

        labelMode.setLayoutData(formModeLabel);

        comboMode = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);

        comboMode.setToolTipText(getString("AmqpPlugin.Type.Label"));
        comboMode.addSelectionListener(comboModeListener);
        comboMode.addSelectionListener(selectionModifyListener);
        comboMode.setItems(modes.toArray(new String[modes.size()]));
        props.setLook(comboMode);

        formModeCombo      = new FormData();
        formModeCombo.left = new FormAttachment(middle, margin);
        formModeCombo.top  = new FormAttachment(wStepname, margin);
        formModeCombo.right= new FormAttachment(100, 0);

        comboMode.setLayoutData(formModeCombo);

        // URI line
        labelURI = new Label(shell, SWT.RIGHT);
        labelURI.setText(getString("AmqpPlugin.URI.Label"));
        props.setLook(labelURI);

        formURILabel       = new FormData();
        formURILabel.left  = new FormAttachment(0, 0);
        formURILabel.right = new FormAttachment(middle, -margin);
        formURILabel.top   = new FormAttachment(comboMode , margin);

        labelURI.setLayoutData(formURILabel);

        textURI = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        props.setLook(textURI);
        textURI.addModifyListener(modifyListener);

        formURIText        = new FormData();
        formURIText.left   = new FormAttachment(middle, 0);
        formURIText.right  = new FormAttachment(100, 0);
        formURIText.top    = new FormAttachment(comboMode, margin);
	
        textURI.setLayoutData(formURIText);

        // Username line
        labelUsername = new Label(shell, SWT.RIGHT);
        labelUsername.setText(getString("AmqpPlugin.Username.Label"));
        props.setLook(labelUsername);

        formUsernameLabel       = new FormData();
        formUsernameLabel.left  = new FormAttachment(0, 0);
        formUsernameLabel.right = new FormAttachment(middle, -margin);
        formUsernameLabel.top   = new FormAttachment(textURI , margin);

        labelUsername.setLayoutData(formUsernameLabel);

        textUsername = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        props.setLook(textUsername);
        textUsername.addModifyListener(modifyListener);

        formUsernameText        = new FormData();
        formUsernameText.left   = new FormAttachment(middle, 0);
        formUsernameText.right  = new FormAttachment(100, 0);
        formUsernameText.top    = new FormAttachment(textURI, margin);

        textUsername.setLayoutData(formUsernameText);

        // Password line
        labelPassword = new Label(shell, SWT.RIGHT);
        labelPassword.setText(getString("AmqpPlugin.Password.Label"));
        props.setLook(labelPassword);

        formPasswordLabel       = new FormData();
        formPasswordLabel.left  = new FormAttachment(0, 0);
        formPasswordLabel.right = new FormAttachment(middle, -margin);
        formPasswordLabel.top   = new FormAttachment(textUsername , margin);
        labelPassword.setLayoutData(formPasswordLabel);

        textPassword = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(textPassword);
        textPassword.addModifyListener(modifyListener);

        formPasswordText        = new FormData();
        formPasswordText.left   = new FormAttachment(middle, 0);
        formPasswordText.right  = new FormAttachment(100, 0);
        formPasswordText.top    = new FormAttachment(textUsername, margin);

        textPassword.setLayoutData(formPasswordText);
        textPassword.setToolTipText(getString("AmqpPlugin.Password.Tooltip"));
	textPassword.setEchoChar('*');



        // Host line
        labelHost = new Label(shell, SWT.RIGHT);
        labelHost.setText(getString("AmqpPlugin.Host.Label"));
        props.setLook(labelHost);

        formHostLabel       = new FormData();
        formHostLabel.left  = new FormAttachment(0, 0);
        formHostLabel.right = new FormAttachment(middle, -margin);
        formHostLabel.top   = new FormAttachment(textPassword , margin);

        labelHost.setLayoutData(formHostLabel);

        textHost = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        props.setLook(textHost);
        textHost.addModifyListener(modifyListener);

        formHostText        = new FormData();
        formHostText.left   = new FormAttachment(middle, 0);
        formHostText.right  = new FormAttachment(100, 0);
        formHostText.top    = new FormAttachment(textPassword, margin);

        textHost.setLayoutData(formHostText);

        // Port line
        labelPort = new Label(shell, SWT.RIGHT);
        labelPort.setText(getString("AmqpPlugin.Port.Label"));
        props.setLook(labelPort);

        formPortLabel       = new FormData();
        formPortLabel.left  = new FormAttachment(0, 0);
        formPortLabel.right = new FormAttachment(middle, -margin);
        formPortLabel.top   = new FormAttachment(textHost , margin);

        labelPort.setLayoutData(formPortLabel);

        textPort = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        props.setLook(textPort);
        textPort.addModifyListener(modifyListener);

        formPortText        = new FormData();
        formPortText.left   = new FormAttachment(middle, 0);
        formPortText.right  = new FormAttachment(100, 0);
        formPortText.top    = new FormAttachment(textHost, margin);

        textPort.setLayoutData(formPortText);

        // Vhost line
        labelVhost = new Label(shell, SWT.RIGHT);
        labelVhost.setText(getString("AmqpPlugin.Vhost.Label"));
        props.setLook(labelVhost);

        formVhostLabel       = new FormData();
        formVhostLabel.left  = new FormAttachment(0, 0);
        formVhostLabel.right = new FormAttachment(middle, -margin);
        formVhostLabel.top   = new FormAttachment(textPort , margin);

        labelVhost.setLayoutData(formVhostLabel);

        textVhost = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        props.setLook(textVhost);
        textVhost.addModifyListener(modifyListener);

        formVhostText        = new FormData();
        formVhostText.left   = new FormAttachment(middle, 0);
        formVhostText.right  = new FormAttachment(100, 0);
        formVhostText.top    = new FormAttachment(textPort, margin);

        textVhost.setLayoutData(formVhostText);



        // UseSsl
        labelUseSsl = new Label(shell, SWT.RIGHT);
        labelUseSsl.setText(getString("AmqpPlugin.UseSsl.Label"));
        props.setLook(labelUseSsl);

        formUseSslLabel       = new FormData();
        formUseSslLabel.left  = new FormAttachment(0, 0);
        formUseSslLabel.right = new FormAttachment(middle, -margin);
        formUseSslLabel.top   = new FormAttachment(textVhost , margin);

        labelUseSsl.setLayoutData(formUseSslLabel);

        checkUseSsl = new Button(shell, SWT.CHECK);
        props.setLook(checkUseSsl);
        checkUseSsl.addSelectionListener(selectionModifyListener);

        formUseSslText        = new FormData();
        formUseSslText.left   = new FormAttachment(middle, 0);
        formUseSslText.right  = new FormAttachment(100, 0);
        formUseSslText.top    = new FormAttachment(textVhost, margin);

        checkUseSsl.setLayoutData(formUseSslText);


        // Transactional
        labelTransactional = new Label(shell, SWT.RIGHT);
        labelTransactional.setText(getString("AmqpPlugin.Transactional.Label"));
        props.setLook(labelTransactional);

        formTransactionalLabel       = new FormData();
        formTransactionalLabel.left  = new FormAttachment(0, 0);
        formTransactionalLabel.right = new FormAttachment(middle, -margin);
        formTransactionalLabel.top   = new FormAttachment(labelUseSsl , margin);

        labelTransactional.setLayoutData(formTransactionalLabel);

        checkTransactional = new Button(shell, SWT.CHECK);
        props.setLook(checkTransactional);
        checkTransactional.addSelectionListener(selectionModifyListener);

        formTransactionalText        = new FormData();
        formTransactionalText.left   = new FormAttachment(middle, 0);
        formTransactionalText.right  = new FormAttachment(100, 0);
        formTransactionalText.top    = new FormAttachment(labelUseSsl, margin);

        checkTransactional.setLayoutData(formTransactionalText);

         // Body line
        labelBodyField = new Label(shell, SWT.RIGHT);
        labelBodyField.setText(getString("AmqpPlugin.Body.Label"));
        props.setLook(labelBodyField);

        formBodyLabel       = new FormData();
        formBodyLabel.left  = new FormAttachment(0, 0);
        formBodyLabel.right = new FormAttachment(middle, -margin);
        formBodyLabel.top   = new FormAttachment(labelTransactional , margin);

        labelBodyField.setLayoutData(formBodyLabel);

        textBodyField = new TextVar(transMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textBodyField);
        textBodyField.addModifyListener(modifyListener);

        formBodyText        = new FormData();
        formBodyText.left   = new FormAttachment(middle, 0);
        formBodyText.right  = new FormAttachment(100, 0);
        formBodyText.top    = new FormAttachment(labelTransactional, margin);

        textBodyField.setLayoutData(formBodyText);

        // Target line
        labelTarget = new Label(shell, SWT.RIGHT);
        labelTarget.setText(getString("AmqpPlugin.Target.Label"));
        props.setLook(labelTarget);

        formTargetLabel       = new FormData();
        formTargetLabel.left  = new FormAttachment(0, 0);
        formTargetLabel.right = new FormAttachment(middle, -margin);
        formTargetLabel.top   = new FormAttachment(textBodyField , margin);

        labelTarget.setLayoutData(formTargetLabel);

        textTarget = new TextVar(transMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textTarget);
        textTarget.addModifyListener(modifyListener);

        formTargetText        = new FormData();
        formTargetText.left   = new FormAttachment(middle, 0);
        formTargetText.right  = new FormAttachment(100, 0);
        formTargetText.top    = new FormAttachment(textBodyField, margin);

        textTarget.setLayoutData(formTargetText);

        // Routing line
        labelRouting = new Label(shell, SWT.RIGHT);
        labelRouting.setText(getString("AmqpPlugin.Routing.Label"));
        props.setLook(labelRouting);

        formRoutingLabel       = new FormData();
        formRoutingLabel.left  = new FormAttachment(0, 0);
        formRoutingLabel.right = new FormAttachment(middle, -margin);
        formRoutingLabel.top   = new FormAttachment(textTarget , margin);

        labelRouting.setLayoutData(formRoutingLabel);

        textRouting = new TextVar(transMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textRouting);
        textRouting.addModifyListener(modifyListener);

        formRoutingText        = new FormData();
        formRoutingText.left   = new FormAttachment(middle, 0);
        formRoutingText.right  = new FormAttachment(100, 0);
        formRoutingText.top    = new FormAttachment(textTarget, margin);

        textRouting.setLayoutData(formRoutingText);

        // Limit line
        labelLimit = new Label(shell, SWT.RIGHT);
        labelLimit.setText(getString("AmqpPlugin.Limit.Label"));
        props.setLook(labelLimit);

        formLimitLabel       = new FormData();
        formLimitLabel.left  = new FormAttachment(0, 0);
        formLimitLabel.right = new FormAttachment(middle, -margin);
        formLimitLabel.top   = new FormAttachment(textRouting , margin);

        labelLimit.setLayoutData(formLimitLabel);

        textLimit = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textLimit);
        textLimit.addModifyListener(modifyListener);

        formLimitText        = new FormData();
        formLimitText.left   = new FormAttachment(middle, 0);
        formLimitText.right  = new FormAttachment(100, 0);
        formLimitText.top    = new FormAttachment(textRouting, margin);

        textLimit.setLayoutData(formLimitText);


        // DecalreOption
        labelDeclare = new Label(shell, SWT.RIGHT);
        labelDeclare.setText(getString("AmqpPlugin.Declare.Label"));
        props.setLook(labelDeclare);

        formDeclareLabel       = new FormData();
        formDeclareLabel.left  = new FormAttachment(0, 0);
        formDeclareLabel.right = new FormAttachment(middle, -margin);
        formDeclareLabel.top   = new FormAttachment(textLimit , margin);

        labelDeclare.setLayoutData(formDeclareLabel);

        checkDeclare = new Button(shell, SWT.CHECK);
        props.setLook(checkDeclare);
        checkDeclare.addSelectionListener(selectionModifyListener);

        formDeclareText        = new FormData();
        formDeclareText.left   = new FormAttachment(middle, 0);
        formDeclareText.right  = new FormAttachment(100, 0);
        formDeclareText.top    = new FormAttachment(textLimit, margin);

        checkDeclare.setLayoutData(formDeclareText);

        // Exchange Type
        labelExchtype = new Label(shell, SWT.RIGHT);
        labelExchtype.setText(getString("AmqpPlugin.Exchtype.Label"));
        props.setLook(labelExchtype);

        formExchtypeLabel       = new FormData();
        formExchtypeLabel.left  = new FormAttachment(0, 0);
        formExchtypeLabel.top   = new FormAttachment(checkDeclare, margin);
        formExchtypeLabel.right = new FormAttachment(middle, 0);

        labelExchtype.setLayoutData(formExchtypeLabel);

        comboExchtype = new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);

        comboExchtype.setToolTipText(getString("AmqpPlugin.Type.Label"));
        comboExchtype.addSelectionListener(comboExchtypeListener);
        comboExchtype.addSelectionListener(selectionModifyListener);
        comboExchtype.setItems(exchtypes.toArray(new String[exchtypes.size()]));
        props.setLook(comboExchtype);

        formExchtypeCombo      = new FormData();
        formExchtypeCombo.left = new FormAttachment(middle, margin);
        formExchtypeCombo.top  = new FormAttachment(checkDeclare, margin);
        formExchtypeCombo.right= new FormAttachment(100, 0);

        comboExchtype.setLayoutData(formExchtypeCombo);


        // DurableOption
        labelDurable = new Label(shell, SWT.RIGHT);
        labelDurable.setText(getString("AmqpPlugin.Durable.Label"));
        props.setLook(labelDeclare);

        formDurableLabel       = new FormData();
        formDurableLabel.left  = new FormAttachment(0, 0);
        formDurableLabel.right = new FormAttachment(middle, -margin);
        formDurableLabel.top   = new FormAttachment(comboExchtype , margin);

        labelDurable.setLayoutData(formDurableLabel);

        checkDurable = new Button(shell, SWT.CHECK);
        props.setLook(checkDurable);
        checkDurable.addSelectionListener(selectionModifyListener);

        formDurableText        = new FormData();
        formDurableText.left   = new FormAttachment(middle, 0);
        formDurableText.right  = new FormAttachment(100, 0);
        formDurableText.top    = new FormAttachment(comboExchtype, margin);

        checkDurable.setLayoutData(formDurableText);

        // Autodelete
        labelAutodel = new Label(shell, SWT.RIGHT);
        labelAutodel.setText(getString("AmqpPlugin.Autodel.Label"));
        props.setLook(labelAutodel);

        formAutodelLabel       = new FormData();
        formAutodelLabel.left  = new FormAttachment(0, 0);
        formAutodelLabel.right = new FormAttachment(middle , -margin);
        formAutodelLabel.top   = new FormAttachment(checkDurable , margin);

        labelAutodel.setLayoutData(formAutodelLabel);

        checkAutodel = new Button(shell, SWT.CHECK);
        props.setLook(checkAutodel);
        checkAutodel.addSelectionListener(selectionModifyListener);

        formAutodelText        = new FormData();
        formAutodelText.left   = new FormAttachment(middle , 0);
        formAutodelText.right  = new FormAttachment(100, 0);
        formAutodelText.top    = new FormAttachment(checkDurable, margin);

        checkAutodel.setLayoutData(formAutodelText);

        // Exclusive
        labelExclusive = new Label(shell, SWT.RIGHT);
        labelExclusive.setText(getString("AmqpPlugin.Exclusive.Label"));
        props.setLook(labelExclusive);

        formExclusiveLabel       = new FormData();
        formExclusiveLabel.left  = new FormAttachment(0, 0);
        formExclusiveLabel.right = new FormAttachment(middle , -margin);
        formExclusiveLabel.top   = new FormAttachment(checkAutodel , margin);

        labelExclusive.setLayoutData(formExclusiveLabel);

        checkExclusive = new Button(shell, SWT.CHECK);
        props.setLook(checkExclusive);
        checkExclusive.addSelectionListener(selectionModifyListener);

        formExclusiveText        = new FormData();
        formExclusiveText.left   = new FormAttachment(middle, 0);
        formExclusiveText.right  = new FormAttachment(100, 0);
        formExclusiveText.top    = new FormAttachment(checkAutodel, margin);

        checkExclusive.setLayoutData(formExclusiveText);




	// Consumer Bindings
	wlFields=new Label(shell, SWT.NONE);
	wlFields.setText(getString("AmqpPlugin.Binding.Label")); //$NON-NLS-1$
 	props.setLook(wlFields);
	fdlFields=new FormData();
	fdlFields.left = new FormAttachment(0, 0);
	fdlFields.top  = new FormAttachment(checkExclusive, margin);
	wlFields.setLayoutData(fdlFields);
	
	final int FieldsCols=3;
	final int FieldsRows=input.getBindingTargetValue().length;
		
	ColumnInfo[] colinf=new ColumnInfo[FieldsCols];
	colinf[0]=new ColumnInfo(getString("AmqpPlugin.Binding.Column.Target"), ColumnInfo.COLUMN_TYPE_TEXT, false); 
	colinf[1]=new ColumnInfo(getString("AmqpPlugin.Binding.Column.Exchtype"), ColumnInfo.COLUMN_TYPE_CCOMBO,exchtypes.toArray(new String[exchtypes.size()]), false); 
	colinf[2]=new ColumnInfo(getString("AmqpPlugin.Binding.Column.RoutingKey"), ColumnInfo.COLUMN_TYPE_TEXT, false); 
	colinf[0].setUsingVariables(true);
	colinf[1].setUsingVariables(true);
	colinf[2].setUsingVariables(true);
	wBinding=new TableView(transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, 
		  colinf, 
		  FieldsRows,  
		  modifyListener,
		  props
	  );
	fdFields=new FormData();
	fdFields.left  = new FormAttachment(0, 0);
	fdFields.top   = new FormAttachment(wlFields, margin);
	fdFields.right = new FormAttachment(100, 0);
	fdFields.bottom= new FormAttachment(100, -50);
	wBinding.setLayoutData(fdFields);



        // Some buttons
        wOK     = new Button(shell, SWT.PUSH);
        wCancel = new Button(shell, SWT.PUSH);

        wOK.setText(getString("System.Button.OK"));
        wCancel.setText(getString("System.Button.Cancel"));

        setButtonPositions(new Button[] { wOK, wCancel }, margin, null);

        // Add listeners
        lsCancel = new Listener() {
            @Override
            public void handleEvent(Event e) {
                cancel();
            }
        };

        lsOK = new Listener() {
            @Override
            public void handleEvent(Event e) {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);

        lsDef = new SelectionAdapter() {
            @Override
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        textBodyField.addSelectionListener(lsDef);
        wStepname.addSelectionListener(lsDef);
        textURI.addSelectionListener(lsDef);

        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener(new ShellAdapter() {
            @Override
            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        // Set the shell size, based upon previous time...
        setSize();

        getData();
        input.setChanged(changed);

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return stepname;
    }

    // Read data from input (TextFileInputInfo)
    public void getData()
    {
        wStepname.selectAll();

        checkTransactional.setSelection(false);

        textLimit.setEnabled(false);

        if (input.isTransactional()) {
            checkTransactional.setSelection(true);
        }

        checkUseSsl.setSelection(input.isUseSsl());
        checkDeclare.setSelection(input.isDeclare());
        checkDurable.setSelection(input.isDurable());

        int index = modes.indexOf(input.getMode());

        if (index == -1) {
            index = 0;
        }
        comboMode.select(index);

        index = exchtypes.indexOf(input.getExchtype());

        if (index == -1) {
            index = 0;
        }
        comboExchtype.select(index);

        if (AMQPPluginData.MODE_PRODUCER.equals(comboMode.getText())) toProducerMode();
        if (AMQPPluginData.MODE_CONSUMER.equals(comboMode.getText())) toConsumerMode();


        textURI.setText(input.getUri());
        textUsername.setText(input.getUsername());
        textPassword.setText(input.getPassword());
        textHost.setText(input.getHost());
        textPort.setText(input.getPort());
        textVhost.setText(input.getVhost());
        textTarget.setText(input.getTarget());
        textBodyField.setText(input.getBodyField());

        if (input.getRouting() != null) {
            textRouting.setText(input.getRouting());
        }

        if (input.getLimit() != null) {
            textLimit.setText(input.getLimitString());
        }

        if (AMQPPluginData.MODE_CONSUMER.equals(comboMode.getText())) {
            textLimit.setEnabled(true);
        }


	for (int i=0;i<input.getBindingTargetValue().length;i++)
	{
		TableItem item = wBinding.table.getItem(i);
		String exc = input.getBindingTargetValue()[i];
		String typ = input.getBindingExchtypeValue()[i];
		String rout = input.getBindingRoutingValue()[i];
		
		if (exc!=null) item.setText(1, exc);
		if (typ!=null) item.setText(2, typ);
		if (rout!=null) item.setText(3, rout);
	}

	wBinding.setRowNums();
	wBinding.optWidth(true);


        wStepname.selectAll();
    }

    private void cancel()
    {
        stepname = null;
        input.setChanged(changed);

        dispose();
    }

    private void ok()
    {
        stepname = wStepname.getText();

        if (Const.isEmpty(textURI.getText()) && Const.isEmpty(textHost.getText())) {
            textURI.setFocus();
            return;
        }

        if (Const.isEmpty(textTarget.getText())) {
            textTarget.setFocus();
            return;
        }

        if (Const.isEmpty(textBodyField.getText())) {
            textBodyField.setFocus();
            return;
        }

        input.setUri(textURI.getText());
        input.setMode(comboMode.getText());
        input.setExchtype(comboExchtype.getText());
        input.setTarget(textTarget.getText());
        input.setBodyField(textBodyField.getText());
        input.setTransactional(checkTransactional.getSelection());

        input.setUsername(textUsername.getText());
        input.setPassword(textPassword.getText());
        input.setHost(textHost.getText());
        input.setPort(textPort.getText());
        input.setVhost(textVhost.getText());
        input.setUseSsl(checkUseSsl.getSelection());
        input.setDurable(checkDurable.getSelection());
        input.setDeclare(checkDeclare.getSelection());

        if ( ! Const.isEmpty(textRouting.getText())) {
            input.setRouting(textRouting.getText());
        }

        if ( ! Const.isEmpty(textLimit.getText())) {
            input.setLimit(textLimit.getText());
        }


	//Save Binding Table
	int count = wBinding.nrNonEmpty();
	// in Cosumer mode if we declare queue , we have to bind it to Target
	if ( checkDeclare.getSelection() && count ==  0 && AMQPPluginData.MODE_CONSUMER.equals(input.getMode()) ) {
	   wBinding.setFocus();
           return;
	}
	input.allocateBinding(count);
		
	for (int i=0;i<count;i++)
	{
		TableItem item = wBinding.getNonEmpty(i);
		input.getBindingTargetValue()[i]  = Const.isEmpty(item.getText(1))?null:item.getText(1);
		input.getBindingExchtypeValue()[i]  = item.getText(2);
		input.getBindingRoutingValue()[i]  = item.getText(3);
	}

        dispose();
    }
}
