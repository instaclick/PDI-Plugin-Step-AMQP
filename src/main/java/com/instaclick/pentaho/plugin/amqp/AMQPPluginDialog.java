package com.instaclick.pentaho.plugin.amqp;

import com.instaclick.pentaho.plugin.amqp.AMQPPluginMeta.Binding;
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
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Composite;
import org.pentaho.di.core.Props;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.MessageBox;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;

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

    private CTabFolder wTabFolder;
    private FormData fdTabFolder;

    private CTabItem wConnectionTab, wDeclareTab, wConditionTab;

    private FormData fdConnectionComp, fdDeclareComp, fdConditionComp;


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

    private Label labelRequeue;
    private Button checkRequeue;
    private FormData formRequeueLabel;
    private FormData formRequeueText;

    private Label labelAutodel;
    private Button checkAutodel;
    private FormData formAutodelLabel;
    private FormData formAutodelText;

    private Label labelExclusive;
    private Button checkExclusive;
    private FormData formExclusiveLabel;
    private FormData formExclusiveText;

    private Label labelWaitingConsumer;
    private Button checkWaitingConsumer;
    private FormData formWaitingConsumerLabel;
    private FormData formWaitingConsumerText;


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


    private Label labelPrefetchCount;
    private Text textPrefetchCount;
    private FormData formPrefetchCountLabel;
    private FormData formPrefetchCountText;

    private Label labelWaitTimeout;
    private Text textWaitTimeout;
    private FormData formWaitTimeoutLabel;
    private FormData formWaitTimeoutText;


    private Label labelBodyField;
    private TextVar textBodyField;
    private FormData formBodyLabel;
    private FormData formBodyText;

    private Label labelTransactional;
    private Button checkTransactional;
    private FormData formTransactionalLabel;
    private FormData formTransactionalText;

    private Label        labelBinding;
    private TableView    tableBinding;
    private FormData     formBindingLabel;
    private FormData     formBindingText;

    private static final List<String> modes = new ArrayList<String>(Arrays.asList(new String[] {
        AMQPPluginData.MODE_CONSUMER,
        AMQPPluginData.MODE_PRODUCER
    }));

    private static final List<String> target_types = new ArrayList<String>(Arrays.asList(new String[] {
        AMQPPluginData.TARGET_TYPE_EXCHANGE,
        AMQPPluginData.TARGET_TYPE_QUEUE
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

    private final SelectionAdapter declareModifyListener = new SelectionAdapter() {
        @Override
        public void widgetSelected(SelectionEvent e) {}
    };

    private final SelectionAdapter comboModeListener = new SelectionAdapter() {
        @Override
        public void widgetSelected(SelectionEvent e) {
        setModeDependendFileds();
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

    private void setModeDependendFileds() {
       if (AMQPPluginData.MODE_PRODUCER.equals(comboMode.getText())) {
             enableProducerFields();
       }

       if (AMQPPluginData.MODE_CONSUMER.equals(comboMode.getText())) {
             enableConsumerFields();
       }
    }


    private void enableProducerFields()
    {
        textLimit.setVisible(false);
        labelLimit.setVisible(false);
        textPrefetchCount.setVisible(false);
        labelPrefetchCount.setVisible(false);
        textWaitTimeout.setVisible(false);
        labelWaitTimeout.setVisible(false);
        comboExchtype.setVisible(true);
        labelExchtype.setVisible(true);

        labelWaitingConsumer.setVisible(false);
        checkWaitingConsumer.setVisible(false);

        labelRequeue.setVisible(false);
        checkRequeue.setVisible(false);

        labelTarget.setText(getString("AmqpPlugin.Exchange.Label"));

        tableBinding.setColumnText(1,getString("AmqpPlugin.Binding.Column.Target.ProducerMode"));
        tableBinding.setColumnText(2,getString("AmqpPlugin.Binding.Column.TargetType.ProducerMode"));

    tableBinding.setColumnInfo(1,new ColumnInfo(getString("AmqpPlugin.Binding.Column.TargetType.ProducerMode"), ColumnInfo.COLUMN_TYPE_CCOMBO,target_types.toArray(new String[target_types.size()]), true));
    }

    private void enableConsumerFields()
    {
        textLimit.setVisible(true);
        labelLimit.setVisible(true);

        textPrefetchCount.setVisible(true);
        labelPrefetchCount.setVisible(true);
        textWaitTimeout.setVisible(true);
        labelWaitTimeout.setVisible(true);

        comboExchtype.setVisible(false);
        labelExchtype.setVisible(false);

        labelWaitingConsumer.setVisible(true);
        checkWaitingConsumer.setVisible(true);

        labelRequeue.setVisible(true);
        checkRequeue.setVisible(true);

        labelTarget.setText(getString("AmqpPlugin.Queue.Label"));

        tableBinding.setColumnText(1,getString("AmqpPlugin.Binding.Column.Target.ConsumerMode"));
        tableBinding.setColumnText(2,getString("AmqpPlugin.Binding.Column.TargetType.ConsumerMode"));
        //disable type for consumer
        tableBinding.setColumnInfo(1,new ColumnInfo(getString("AmqpPlugin.Binding.Column.TargetType.ConsumerMode"), ColumnInfo.COLUMN_TYPE_NONE, true));
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

        wTabFolder = new CTabFolder( shell, SWT.BORDER );
        props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
        wTabFolder.setSimple( false );

        ///////////////////////
        // CONNECTION TAB
        //

        // ////////////////////////
        // START OF Connection TAB///
        // /
        wConnectionTab = new CTabItem( wTabFolder, SWT.NONE );
        wConnectionTab.setText( getString("AmqpPlugin.ConnectionTab.TabTitle" ) );

        Composite wConnectionComp = new Composite( wTabFolder, SWT.NONE );
        props.setLook( wConnectionComp );

        FormLayout connectionLayout = new FormLayout();
        connectionLayout.marginWidth = 3;
        connectionLayout.marginHeight = 3;
        wConnectionComp.setLayout( connectionLayout );

        // Mode
        labelMode = new Label(wConnectionComp, SWT.RIGHT);
        labelMode.setText(getString("AmqpPlugin.Type.Label"));
        props.setLook(labelMode);

        formModeLabel       = new FormData();
        formModeLabel.left  = new FormAttachment(0, 0);
        formModeLabel.top   = new FormAttachment(0, margin);
        formModeLabel.right = new FormAttachment(middle, 0);

        labelMode.setLayoutData(formModeLabel);

        comboMode = new CCombo(wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);

        comboMode.setToolTipText(getString("AmqpPlugin.Type.Label"));
        comboMode.addSelectionListener(comboModeListener);
        comboMode.addSelectionListener(selectionModifyListener);
        comboMode.setItems(modes.toArray(new String[modes.size()]));
        props.setLook(comboMode);

        formModeCombo      = new FormData();
        formModeCombo.left = new FormAttachment(middle, margin);
        formModeCombo.top  = new FormAttachment(0, margin);
        formModeCombo.right= new FormAttachment(100, 0);

        comboMode.setLayoutData(formModeCombo);

        // URI line
        labelURI = new Label(wConnectionComp, SWT.RIGHT);
        labelURI.setText(getString("AmqpPlugin.URI.Label"));
        props.setLook(labelURI);

        formURILabel       = new FormData();
        formURILabel.left  = new FormAttachment(0, 0);
        formURILabel.right = new FormAttachment(middle, -margin);
        formURILabel.top   = new FormAttachment(comboMode , margin);

        labelURI.setLayoutData(formURILabel);

        textURI = new TextVar(transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        props.setLook(textURI);
        textURI.addModifyListener(modifyListener);

        formURIText        = new FormData();
        formURIText.left   = new FormAttachment(middle, 0);
        formURIText.right  = new FormAttachment(100, 0);
        formURIText.top    = new FormAttachment(comboMode, margin);

        textURI.setLayoutData(formURIText);

        // Username line
        labelUsername = new Label(wConnectionComp, SWT.RIGHT);
        labelUsername.setText(getString("AmqpPlugin.Username.Label"));
        props.setLook(labelUsername);

        formUsernameLabel       = new FormData();
        formUsernameLabel.left  = new FormAttachment(0, 0);
        formUsernameLabel.right = new FormAttachment(middle, -margin);
        formUsernameLabel.top   = new FormAttachment(textURI , margin);

        labelUsername.setLayoutData(formUsernameLabel);

        textUsername = new TextVar(transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        props.setLook(textUsername);
        textUsername.addModifyListener(modifyListener);

        formUsernameText        = new FormData();
        formUsernameText.left   = new FormAttachment(middle, 0);
        formUsernameText.right  = new FormAttachment(100, 0);
        formUsernameText.top    = new FormAttachment(textURI, margin);

        textUsername.setLayoutData(formUsernameText);

        // Password line
        labelPassword = new Label(wConnectionComp, SWT.RIGHT);
        labelPassword.setText(getString("AmqpPlugin.Password.Label"));
        props.setLook(labelPassword);

        formPasswordLabel       = new FormData();
        formPasswordLabel.left  = new FormAttachment(0, 0);
        formPasswordLabel.right = new FormAttachment(middle, -margin);
        formPasswordLabel.top   = new FormAttachment(textUsername , margin);
        labelPassword.setLayoutData(formPasswordLabel);

        textPassword = new TextVar(transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(textPassword);
        textPassword.addModifyListener(modifyListener);

        formPasswordText        = new FormData();
        formPasswordText.left   = new FormAttachment(middle, 0);
        formPasswordText.right  = new FormAttachment(100, 0);
        formPasswordText.top    = new FormAttachment(textUsername, margin);

        textPassword.setLayoutData(formPasswordText);
        textPassword.setToolTipText(getString("AmqpPlugin.Password.Tooltip"));

        // UseSsl
        labelUseSsl = new Label(wConnectionComp, SWT.RIGHT);
        labelUseSsl.setText(getString("AmqpPlugin.UseSsl.Label"));
        props.setLook(labelUseSsl);

        formUseSslLabel       = new FormData();
        formUseSslLabel.left  = new FormAttachment(0, 0);
        formUseSslLabel.right = new FormAttachment(middle, -margin);
        formUseSslLabel.top   = new FormAttachment(textPassword , margin);

        labelUseSsl.setLayoutData(formUseSslLabel);

        checkUseSsl = new Button(wConnectionComp, SWT.CHECK);
        props.setLook(checkUseSsl);
        checkUseSsl.addSelectionListener(selectionModifyListener);

        formUseSslText        = new FormData();
        formUseSslText.left   = new FormAttachment(middle, 0);
        formUseSslText.right  = new FormAttachment(100, 0);
        formUseSslText.top    = new FormAttachment(textPassword, margin);

        checkUseSsl.setLayoutData(formUseSslText);

        // Host line
        labelHost = new Label(wConnectionComp, SWT.RIGHT);
        labelHost.setText(getString("AmqpPlugin.Host.Label"));
        props.setLook(labelHost);

        formHostLabel       = new FormData();
        formHostLabel.left  = new FormAttachment(0, 0);
        formHostLabel.right = new FormAttachment(middle, -margin);
        formHostLabel.top   = new FormAttachment(labelUseSsl , margin);

        labelHost.setLayoutData(formHostLabel);

        textHost = new TextVar(transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        props.setLook(textHost);
        textHost.addModifyListener(modifyListener);

        formHostText        = new FormData();
        formHostText.left   = new FormAttachment(middle, 0);
        formHostText.right  = new FormAttachment(100, 0);
        formHostText.top    = new FormAttachment(labelUseSsl, margin);

        textHost.setLayoutData(formHostText);

        // Port line
        labelPort = new Label(wConnectionComp, SWT.RIGHT);
        labelPort.setText(getString("AmqpPlugin.Port.Label"));
        props.setLook(labelPort);

        formPortLabel       = new FormData();
        formPortLabel.left  = new FormAttachment(0, 0);
        formPortLabel.right = new FormAttachment(middle, -margin);
        formPortLabel.top   = new FormAttachment(textHost , margin);

        labelPort.setLayoutData(formPortLabel);

        textPort = new TextVar(transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        props.setLook(textPort);
        textPort.addModifyListener(modifyListener);

        formPortText        = new FormData();
        formPortText.left   = new FormAttachment(middle, 0);
        formPortText.right  = new FormAttachment(100, 0);
        formPortText.top    = new FormAttachment(textHost, margin);

        textPort.setLayoutData(formPortText);

        // Vhost line
        labelVhost = new Label(wConnectionComp, SWT.RIGHT);
        labelVhost.setText(getString("AmqpPlugin.Vhost.Label"));
        props.setLook(labelVhost);

        formVhostLabel       = new FormData();
        formVhostLabel.left  = new FormAttachment(0, 0);
        formVhostLabel.right = new FormAttachment(middle, -margin);
        formVhostLabel.top   = new FormAttachment(textPort , margin);

        labelVhost.setLayoutData(formVhostLabel);

        textVhost = new TextVar(transMeta, wConnectionComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);

        props.setLook(textVhost);
        textVhost.addModifyListener(modifyListener);

        formVhostText        = new FormData();
        formVhostText.left   = new FormAttachment(middle, 0);
        formVhostText.right  = new FormAttachment(100, 0);
        formVhostText.top    = new FormAttachment(textPort, margin);

        textVhost.setLayoutData(formVhostText);

         // Body line
        labelBodyField = new Label(wConnectionComp, SWT.RIGHT);
        labelBodyField.setText(getString("AmqpPlugin.Body.Label"));
        props.setLook(labelBodyField);

        formBodyLabel       = new FormData();
        formBodyLabel.left  = new FormAttachment(0, 0);
        formBodyLabel.right = new FormAttachment(middle, -margin);
        formBodyLabel.top   = new FormAttachment(textVhost , margin);

        labelBodyField.setLayoutData(formBodyLabel);

        textBodyField = new TextVar(transMeta, wConnectionComp, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textBodyField);
        textBodyField.addModifyListener(modifyListener);

        formBodyText        = new FormData();
        formBodyText.left   = new FormAttachment(middle, 0);
        formBodyText.right  = new FormAttachment(100, 0);
        formBodyText.top    = new FormAttachment(textVhost, margin);

        textBodyField.setLayoutData(formBodyText);

        // Target line
        labelTarget = new Label(wConnectionComp, SWT.RIGHT);
        labelTarget.setText(getString("AmqpPlugin.Target.Label"));
        props.setLook(labelTarget);

        formTargetLabel       = new FormData();
        formTargetLabel.left  = new FormAttachment(0, 0);
        formTargetLabel.right = new FormAttachment(middle, -margin);
        formTargetLabel.top   = new FormAttachment(textBodyField , margin);

        labelTarget.setLayoutData(formTargetLabel);

        textTarget = new TextVar(transMeta, wConnectionComp, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textTarget);
        textTarget.addModifyListener(modifyListener);

        formTargetText        = new FormData();
        formTargetText.left   = new FormAttachment(middle, 0);
        formTargetText.right  = new FormAttachment(100, 0);
        formTargetText.top    = new FormAttachment(textBodyField, margin);

        textTarget.setLayoutData(formTargetText);

        // Routing line
        labelRouting = new Label(wConnectionComp, SWT.RIGHT);
        labelRouting.setText(getString("AmqpPlugin.Routing.Label"));
        props.setLook(labelRouting);

        formRoutingLabel       = new FormData();
        formRoutingLabel.left  = new FormAttachment(0, 0);
        formRoutingLabel.right = new FormAttachment(middle, -margin);
        formRoutingLabel.top   = new FormAttachment(textTarget , margin);

        labelRouting.setLayoutData(formRoutingLabel);

        textRouting = new TextVar(transMeta, wConnectionComp, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textRouting);
        textRouting.addModifyListener(modifyListener);

        formRoutingText        = new FormData();
        formRoutingText.left   = new FormAttachment(middle, 0);
        formRoutingText.right  = new FormAttachment(100, 0);
        formRoutingText.top    = new FormAttachment(textTarget, margin);

        textRouting.setLayoutData(formRoutingText);


        fdConnectionComp = new FormData();
        fdConnectionComp.left = new FormAttachment( 0, 0 );
        fdConnectionComp.top = new FormAttachment( 0, 0 );
        fdConnectionComp.right = new FormAttachment( 100, 0 );
        fdConnectionComp.bottom = new FormAttachment( 100, 0 );
        wConnectionComp.setLayoutData( fdConnectionComp );

        wConnectionComp.layout();
        wConnectionTab.setControl( wConnectionComp );

        // ///////////////////////////////////////////////////////////
        // / END OF Connection TAB
        // ///////////////////////////////////////////////////////////


        // ////////////////////////
        // START OF Declare TAB///
        // /
        wDeclareTab = new CTabItem( wTabFolder, SWT.NONE );
        wDeclareTab.setText( getString("AmqpPlugin.DeclareTab.TabTitle" ) );

        Composite wDeclareComp = new Composite( wTabFolder, SWT.NONE );
        props.setLook( wDeclareComp );

        FormLayout declareLayout = new FormLayout();
        declareLayout.marginWidth = 3;
        declareLayout.marginHeight = 3;
        wDeclareComp.setLayout( declareLayout );


        // DecalreOption
        labelDeclare = new Label(wDeclareComp, SWT.RIGHT);
        labelDeclare.setText(getString("AmqpPlugin.Declare.Label"));
        props.setLook(labelDeclare);

        formDeclareLabel       = new FormData();
        formDeclareLabel.left  = new FormAttachment(0, 0);
        formDeclareLabel.right = new FormAttachment(middle, -margin);
        formDeclareLabel.top   = new FormAttachment(0 , margin);

        labelDeclare.setLayoutData(formDeclareLabel);

        checkDeclare = new Button(wDeclareComp, SWT.CHECK);
        props.setLook(checkDeclare);
        checkDeclare.addSelectionListener(selectionModifyListener);
        checkDeclare.addSelectionListener(declareModifyListener);

        formDeclareText        = new FormData();
        formDeclareText.left   = new FormAttachment(middle, 0);
        formDeclareText.right  = new FormAttachment(100, 0);
        formDeclareText.top    = new FormAttachment(0, margin);

        checkDeclare.setLayoutData(formDeclareText);

        // Exchange Type
        labelExchtype = new Label(wDeclareComp, SWT.RIGHT);
        labelExchtype.setText(getString("AmqpPlugin.Exchtype.Label"));
        props.setLook(labelExchtype);

        formExchtypeLabel       = new FormData();
        formExchtypeLabel.left  = new FormAttachment(0, 0);
        formExchtypeLabel.top   = new FormAttachment(checkDeclare, margin);
        formExchtypeLabel.right = new FormAttachment(middle, 0);

        labelExchtype.setLayoutData(formExchtypeLabel);

        comboExchtype = new CCombo(wDeclareComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);

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
        labelDurable = new Label(wDeclareComp, SWT.RIGHT);
        labelDurable.setText(getString("AmqpPlugin.Durable.Label"));
        props.setLook(labelDeclare);

        formDurableLabel       = new FormData();
        formDurableLabel.left  = new FormAttachment(0, 0);
        formDurableLabel.right = new FormAttachment(middle, -margin);
        formDurableLabel.top   = new FormAttachment(comboExchtype , margin);

        labelDurable.setLayoutData(formDurableLabel);

        checkDurable = new Button(wDeclareComp, SWT.CHECK);
        props.setLook(checkDurable);
        checkDurable.addSelectionListener(selectionModifyListener);

        formDurableText        = new FormData();
        formDurableText.left   = new FormAttachment(middle, 0);
        formDurableText.right  = new FormAttachment(100, 0);
        formDurableText.top    = new FormAttachment(comboExchtype, margin);

        checkDurable.setLayoutData(formDurableText);

        // Autodelete
        labelAutodel = new Label(wDeclareComp, SWT.RIGHT);
        labelAutodel.setText(getString("AmqpPlugin.Autodel.Label"));
        props.setLook(labelAutodel);

        formAutodelLabel       = new FormData();
        formAutodelLabel.left  = new FormAttachment(0, 0);
        formAutodelLabel.right = new FormAttachment(middle , -margin);
        formAutodelLabel.top   = new FormAttachment(labelDurable , margin);

        labelAutodel.setLayoutData(formAutodelLabel);

        checkAutodel = new Button(wDeclareComp, SWT.CHECK);
        props.setLook(checkAutodel);
        checkAutodel.addSelectionListener(selectionModifyListener);

        formAutodelText        = new FormData();
        formAutodelText.left   = new FormAttachment(middle , 0);
        formAutodelText.right  = new FormAttachment(100, 0);
        formAutodelText.top    = new FormAttachment(labelDurable, margin);

        checkAutodel.setLayoutData(formAutodelText);

        // Exclusive
        labelExclusive = new Label(wDeclareComp, SWT.RIGHT);
        labelExclusive.setText(getString("AmqpPlugin.Exclusive.Label"));
        props.setLook(labelExclusive);

        formExclusiveLabel       = new FormData();
        formExclusiveLabel.left  = new FormAttachment(0, 0);
        formExclusiveLabel.right = new FormAttachment(middle , -margin);
        formExclusiveLabel.top   = new FormAttachment(labelAutodel , margin);

        labelExclusive.setLayoutData(formExclusiveLabel);

        checkExclusive = new Button(wDeclareComp, SWT.CHECK);
        props.setLook(checkExclusive);
        checkExclusive.addSelectionListener(selectionModifyListener);

        formExclusiveText        = new FormData();
        formExclusiveText.left   = new FormAttachment(middle, margin);
        formExclusiveText.right  = new FormAttachment(100, 0);
        formExclusiveText.top    = new FormAttachment(labelAutodel, margin);

        checkExclusive.setLayoutData(formExclusiveText);

        // Bindings
        labelBinding = new Label(wDeclareComp, SWT.NONE);

        labelBinding.setText(getString("AmqpPlugin.Binding.Label"));
        props.setLook(labelBinding);

        formBindingLabel      = new FormData();
        formBindingLabel.left = new FormAttachment(0, 0);
        formBindingLabel.top  = new FormAttachment(checkExclusive, margin);
        labelBinding.setLayoutData(formBindingLabel);

        ColumnInfo[] colinf  = new ColumnInfo[]{
            new ColumnInfo(getString("AmqpPlugin.Binding.Column.Target"), ColumnInfo.COLUMN_TYPE_TEXT, false),
            new ColumnInfo(getString("AmqpPlugin.Binding.Column.TargetType.ProducerMode"), ColumnInfo.COLUMN_TYPE_CCOMBO,target_types.toArray(new String[target_types.size()]), true),
            new ColumnInfo(getString("AmqpPlugin.Binding.Column.RoutingKey"), ColumnInfo.COLUMN_TYPE_TEXT, false)
        };

        //colinf[0].setUsingVariables(true);
        //colinf[1].setUsingVariables(true);
        //colinf[2].setUsingVariables(true);

        tableBinding = new TableView(transMeta, wDeclareComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            colinf,
            input.getBindings().size(),
            modifyListener,
            props
        );

        formBindingText        = new FormData();
        formBindingText.left   = new FormAttachment(0, 0);
        formBindingText.top    = new FormAttachment(labelBinding, margin);
        formBindingText.right  = new FormAttachment(100, 0);
        formBindingText.bottom = new FormAttachment(100, -50);

        tableBinding.setLayoutData(formBindingText);


        fdDeclareComp = new FormData();
        fdDeclareComp.left = new FormAttachment( 0, 0 );
        fdDeclareComp.top = new FormAttachment( 0, 0 );
        fdDeclareComp.right = new FormAttachment( 100, 0 );
        fdDeclareComp.bottom = new FormAttachment( 100, 0 );
        wDeclareComp.setLayoutData( fdDeclareComp );

        wDeclareComp.layout();
        wDeclareTab.setControl( wDeclareComp );

        // ///////////////////////////////////////////////////////////
        // / END OF Declare TAB
        // ///////////////////////////////////////////////////////////


        // ////////////////////////
        // START OF Condition TAB///
        wConditionTab = new CTabItem( wTabFolder, SWT.NONE );
        wConditionTab.setText( getString("AmqpPlugin.ConditionTab.TabTitle" ) );

        Composite wConditionComp = new Composite( wTabFolder, SWT.NONE );
        props.setLook( wConditionComp );

        FormLayout conditionLayout = new FormLayout();
        conditionLayout.marginWidth = 3;
        conditionLayout.marginHeight = 3;
        wConditionComp.setLayout( conditionLayout );


        // Transactional
        labelTransactional = new Label(wConditionComp, SWT.RIGHT);
        labelTransactional.setText(getString("AmqpPlugin.Transactional.Label"));
        props.setLook(labelTransactional);

        formTransactionalLabel       = new FormData();
        formTransactionalLabel.left  = new FormAttachment(0, 0);
        formTransactionalLabel.right = new FormAttachment(middle, -margin);
        formTransactionalLabel.top   = new FormAttachment(0 , margin);

        labelTransactional.setLayoutData(formTransactionalLabel);

        checkTransactional = new Button(wConditionComp, SWT.CHECK);
        props.setLook(checkTransactional);
        checkTransactional.addSelectionListener(selectionModifyListener);

        formTransactionalText        = new FormData();
        formTransactionalText.left   = new FormAttachment(middle, 0);
        formTransactionalText.right  = new FormAttachment(100, 0);
        formTransactionalText.top    = new FormAttachment(0, margin);

        checkTransactional.setLayoutData(formTransactionalText);

        // Limit line
        labelLimit = new Label(wConditionComp, SWT.RIGHT);
        labelLimit.setText(getString("AmqpPlugin.Limit.Label"));
        props.setLook(labelLimit);

        formLimitLabel       = new FormData();
        formLimitLabel.left  = new FormAttachment(0, 0);
        formLimitLabel.right = new FormAttachment(middle, -margin);
        formLimitLabel.top   = new FormAttachment(labelTransactional , margin);

        labelLimit.setLayoutData(formLimitLabel);

        textLimit = new Text(wConditionComp, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textLimit);
        textLimit.addModifyListener(modifyListener);

        formLimitText        = new FormData();
        formLimitText.left   = new FormAttachment(middle, 0);
        formLimitText.right  = new FormAttachment(100, 0);
        formLimitText.top    = new FormAttachment(labelTransactional, margin);

        textLimit.setLayoutData(formLimitText);

        // PrefetchCount
        labelPrefetchCount = new Label(wConditionComp, SWT.RIGHT);
        labelPrefetchCount.setText(getString("AmqpPlugin.PrefetchCount.Label"));
        labelPrefetchCount.setForeground(new Color(shell.getDisplay(), 255, 0, 0));
        props.setLook(labelPrefetchCount);

        formPrefetchCountLabel       = new FormData();
        formPrefetchCountLabel.left  = new FormAttachment(0, 0);
        formPrefetchCountLabel.right = new FormAttachment(middle, -margin);
        formPrefetchCountLabel.top   = new FormAttachment(textLimit , margin);

        labelPrefetchCount.setLayoutData(formPrefetchCountLabel);

        textPrefetchCount = new Text(wConditionComp, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textPrefetchCount);
        textPrefetchCount.addModifyListener(modifyListener);

        formPrefetchCountText        = new FormData();
        formPrefetchCountText.left   = new FormAttachment(middle, 0);
        formPrefetchCountText.right  = new FormAttachment(100, 0);
        formPrefetchCountText.top    = new FormAttachment(textLimit, margin);

        textPrefetchCount.setLayoutData(formPrefetchCountText);


        // WaitingConsumer
        labelWaitingConsumer = new Label(wConditionComp, SWT.RIGHT);
        labelWaitingConsumer.setText(getString("AmqpPlugin.WaitingConsumer.Label"));
        labelWaitingConsumer.setForeground(new Color(shell.getDisplay(), 255, 0, 0));
        props.setLook(labelWaitingConsumer);

        formWaitingConsumerLabel       = new FormData();
        formWaitingConsumerLabel.left  = new FormAttachment(0, 0);
        formWaitingConsumerLabel.right = new FormAttachment(middle , -margin);
        formWaitingConsumerLabel.top   = new FormAttachment(textPrefetchCount , margin);

        labelWaitingConsumer.setLayoutData(formWaitingConsumerLabel);

        checkWaitingConsumer = new Button(wConditionComp, SWT.CHECK);
        props.setLook(checkWaitingConsumer);
        checkWaitingConsumer.addSelectionListener(selectionModifyListener);

        formWaitingConsumerText        = new FormData();
        formWaitingConsumerText.left   = new FormAttachment(middle, margin);
        formWaitingConsumerText.right  = new FormAttachment(100, 0);
        formWaitingConsumerText.top    = new FormAttachment(textPrefetchCount, margin);

        checkWaitingConsumer.setLayoutData(formWaitingConsumerText);


        // WaitTimeout
        labelWaitTimeout = new Label(wConditionComp, SWT.RIGHT);
        labelWaitTimeout.setText(getString("AmqpPlugin.WaitTimeout.Label"));
        labelWaitTimeout.setForeground(new Color(shell.getDisplay(), 255, 0, 0));
        props.setLook(labelWaitTimeout);

        formWaitTimeoutLabel       = new FormData();
        formWaitTimeoutLabel.left  = new FormAttachment(0, 0);
        formWaitTimeoutLabel.right = new FormAttachment(middle, -margin);
        formWaitTimeoutLabel.top   = new FormAttachment(checkWaitingConsumer , margin);

        labelWaitTimeout.setLayoutData(formWaitTimeoutLabel);

        textWaitTimeout = new Text(wConditionComp, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textWaitTimeout);
        textWaitTimeout.addModifyListener(modifyListener);

        formWaitTimeoutText        = new FormData();
        formWaitTimeoutText.left   = new FormAttachment(middle, 0);
        formWaitTimeoutText.right  = new FormAttachment(100, 0);
        formWaitTimeoutText.top    = new FormAttachment(checkWaitingConsumer, margin);

        textWaitTimeout.setLayoutData(formWaitTimeoutText);

        // Requeue consumed data
        labelRequeue = new Label(wConditionComp, SWT.RIGHT);
        labelRequeue.setText(getString("AmqpPlugin.Requeue.Label"));
        labelRequeue.setForeground(new Color(shell.getDisplay(), 255, 0, 0));
        props.setLook(labelRequeue);

        formRequeueLabel       = new FormData();
        formRequeueLabel.left  = new FormAttachment(0, 0);
        formRequeueLabel.right = new FormAttachment(middle , -margin);
        formRequeueLabel.top   = new FormAttachment(textWaitTimeout , margin);

        labelRequeue.setLayoutData(formRequeueLabel);

        checkRequeue = new Button(wConditionComp, SWT.CHECK);
        props.setLook(checkRequeue);
        checkRequeue.addSelectionListener(selectionModifyListener);

        formRequeueText        = new FormData();
        formRequeueText.left   = new FormAttachment(middle, margin);
        formRequeueText.right  = new FormAttachment(100, 0);
        formRequeueText.top    = new FormAttachment(textWaitTimeout, margin);

        checkRequeue.setLayoutData(formRequeueText);

        fdConditionComp = new FormData();
        fdConditionComp.left = new FormAttachment( 0, 0 );
        fdConditionComp.top = new FormAttachment( 0, 0 );
        fdConditionComp.right = new FormAttachment( 100, 0 );
        fdConditionComp.bottom = new FormAttachment( 100, 0 );
        wConditionComp.setLayoutData( fdConditionComp );

        wConditionComp.layout();
        wConditionTab.setControl( wConditionComp );

        // ///////////////////////////////////////////////////////////
        // / END OF Condition TAB
        // ///////////////////////////////////////////////////////////


        /// place TabFolder element
        fdTabFolder = new FormData();
        fdTabFolder.left = new FormAttachment( 0, 0 );
        fdTabFolder.top = new FormAttachment( wStepname, margin );
        fdTabFolder.right = new FormAttachment( 100, 0 );
        fdTabFolder.bottom = new FormAttachment( 100, -50 );
        wTabFolder.setLayoutData( fdTabFolder );

        // Some buttons
        wOK     = new Button(shell, SWT.PUSH);
        wCancel = new Button(shell, SWT.PUSH);

        wOK.setText(getString("System.Button.OK"));
        wCancel.setText(getString("System.Button.Cancel"));

        setButtonPositions(new Button[] { wOK, wCancel }, margin, wTabFolder);

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

        lsDef = new SelectionAdapter() {
            @Override
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);

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

        wTabFolder.setSelection( 0 );
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

        checkUseSsl.setSelection(input.isUseSsl());
        checkDeclare.setSelection(input.isDeclare());
        checkDurable.setSelection(input.isDurable());
        checkTransactional.setSelection(input.isTransactional());
        checkAutodel.setSelection(input.isAutodel());
        checkExclusive.setSelection(input.isExclusive());
        checkWaitingConsumer.setSelection(input.isWaitingConsumer());

        checkRequeue.setSelection(input.isRequeue());


        int indexMode     = modes.indexOf(input.getMode());
        int indexExchtype = exchtypes.indexOf(input.getExchtype());

        if (indexMode == -1) {
            indexMode = 0;
        }

        if (indexExchtype == -1) {
            indexExchtype = 0;
        }

        comboMode.select(indexMode);
        comboExchtype.select(indexExchtype);

        if (AMQPPluginData.MODE_PRODUCER.equals(comboMode.getText())) {
            enableProducerFields();
        }

        if (AMQPPluginData.MODE_CONSUMER.equals(comboMode.getText())) {
            enableConsumerFields();
        }

        setFieldText(textURI, input.getUri());
        setFieldText(textHost, input.getHost());
        setFieldText(textPort, input.getPort());
        setFieldText(textVhost, input.getVhost());
        setFieldText(textTarget, input.getTarget());
        setFieldText(textRouting, input.getRouting());
        setFieldText(textLimit, input.getLimitString());
        setFieldText(textUsername, input.getUsername());
        setFieldText(textPassword, input.getPassword());
        setFieldText(textBodyField, input.getBodyField());

        setFieldText(textPrefetchCount, input.getPrefetchCountString());
        setFieldText(textWaitTimeout, input.getWaitTimeoutString());


        for (int i = 0; i < input.getBindings().size(); i++) {
            final Binding binding = input.getBindings().get(i);
            final TableItem item  = tableBinding.table.getItem(i);
            final String target   = binding.getTarget();
            final String target_type  = binding.getTargetType();
            final String routing  = binding.getRouting();

            if ( ! Const.isEmpty(target)) {
                item.setText(1, target);
            }

            if ( ! Const.isEmpty(target_type)) {
                item.setText(2, target_type);
            }

            if ( ! Const.isEmpty(routing)) {
                item.setText(3, routing);
            }
        }

        tableBinding.setRowNums();
        tableBinding.optWidth(true);
        setModeDependendFileds();

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

        input.setUri(getFieldText(textURI));
        input.setTarget(getFieldText(textTarget));
        input.setBodyField(getFieldText(textBodyField));
        input.setUsername(getFieldText(textUsername));
        input.setPassword(getFieldText(textPassword));
        input.setHost(getFieldText(textHost));
        input.setPort(getFieldText(textPort));
        input.setVhost(getFieldText(textVhost));
        input.setRouting(getFieldText(textRouting));
        input.setLimit(getFieldText(textLimit));
        input.setPrefetchCount(getFieldText(textPrefetchCount));
        input.setWaitTimeout(getFieldText(textWaitTimeout));

        input.setUseSsl(checkUseSsl.getSelection());
        input.setDurable(checkDurable.getSelection());
        input.setDeclare(checkDeclare.getSelection());
        input.setTransactional(checkTransactional.getSelection());
        input.setExclusive(checkExclusive.getSelection());
        input.setWaitingConsumer(checkWaitingConsumer.getSelection());
        input.setRequeue(checkRequeue.getSelection());
        input.setAutodel(checkAutodel.getSelection());

        input.setMode(getFieldText(comboMode));
        input.setExchtype(getFieldText(comboExchtype));
        input.clearBindings();

        if (Const.isEmpty(input.getUri()) && Const.isEmpty(input.getHost())) {
            textURI.setFocus();
            textHost.setFocus();
            return;
        }

        if (Const.isEmpty(input.getTarget())) {
            textTarget.setFocus();
            return;
        }

        if (Const.isEmpty(input.getBodyField())) {
            textBodyField.setFocus();
            return;
        }

        //Save Binding Table
        int count = tableBinding.nrNonEmpty();



        if ( input.isConsumer() && input.isTransactional() && input.getPrefetchCount() > 0 ) {
            MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
            mb.setMessage( getString("AmqpPlugin.Error.PrefetchCountAndTransactionalNotSupported" ) );
            mb.setText( getString(  "AmqpPlugin.Error" ) );
            mb.open();
            textPrefetchCount.setFocus();
            return;
        }

        // nothing to save
        if (count <= 0) {
            dispose();
            return;
        }

        for (int i = 0; i< count; i++) {
            TableItem item = tableBinding.getNonEmpty(i);

            if (item == null) {
                continue;
            }

            String target  = item.getText(1);
            String target_type = item.getText(2);
            String routing = item.getText(3);

            if (Const.isEmpty(target)) {
                continue;
            }

            input.addBinding(target, target_type, routing);
        }

        dispose();
    }

    private void setFieldText(TextVar field, String value)
    {
        if (value == null) {
            field.setText("");

            return;
        }

        field.setText(value);
    }

    private void setFieldText(Text field, String value)
    {
        if (value == null) {
            field.setText("");

            return;
        }

        field.setText(value);
    }

    private String getFieldText(TextVar field)
    {
        if (Const.isEmpty(field.getText())) {
            return null;
        }

        return field.getText();
    }

    private String getFieldText(Text field)
    {
        if (Const.isEmpty(field.getText())) {
            return null;
        }

        return field.getText();
    }

    private String getFieldText(CCombo field)
    {
        if (Const.isEmpty(field.getText())) {
            return null;
        }

        return field.getText();
    }
}
