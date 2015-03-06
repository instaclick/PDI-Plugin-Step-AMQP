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

    private Label    labelMode;
    private CCombo   comboMode;
    private FormData formModeLabel;
    private FormData formModeCombo;

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

    private static final List<String> modes = new ArrayList<String>(Arrays.asList(new String[] {
        AMQPPluginData.MODE_CONSUMER,
        AMQPPluginData.MODE_PRODUCER
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
            textLimit.setEnabled(false);

            if (AMQPPluginData.MODE_CONSUMER.equals(comboMode.getText())) {
                textLimit.setEnabled(true);
            }
        }
    };

    public AMQPPluginDialog(Shell parent, Object in, TransMeta transMeta, String sname)
    {
        super(parent, (BaseStepMeta) in, transMeta, sname);

        input = (AMQPPluginMeta) in;
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

        textURI = new TextVar(transMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER);

        props.setLook(textURI);
        textURI.addModifyListener(modifyListener);

        formURIText        = new FormData();
        formURIText.left   = new FormAttachment(middle, 0);
        formURIText.right  = new FormAttachment(100, 0);
        formURIText.top    = new FormAttachment(comboMode, margin);

        textURI.setLayoutData(formURIText);

        // Transactional
        labelTransactional = new Label(shell, SWT.RIGHT);
        labelTransactional.setText(getString("AmqpPlugin.Transactional.Label"));
        props.setLook(labelTransactional);

        formTransactionalLabel       = new FormData();
        formTransactionalLabel.left  = new FormAttachment(0, 0);
        formTransactionalLabel.right = new FormAttachment(middle, -margin);
        formTransactionalLabel.top   = new FormAttachment(textURI , margin);

        labelTransactional.setLayoutData(formTransactionalLabel);

        checkTransactional = new Button(shell, SWT.CHECK);
        props.setLook(checkTransactional);
        checkTransactional.addSelectionListener(selectionModifyListener);

        formTransactionalText        = new FormData();
        formTransactionalText.left   = new FormAttachment(middle, 0);
        formTransactionalText.right  = new FormAttachment(100, 0);
        formTransactionalText.top    = new FormAttachment(textURI, margin);

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

        int index = modes.indexOf(input.getMode());

        if (index == -1) {
            index = 0;
        }

        comboMode.select(index);
        textURI.setText(input.getUri());
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

        if (Const.isEmpty(textURI.getText())) {
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
        input.setTarget(textTarget.getText());
        input.setBodyField(textBodyField.getText());
        input.setTransactional(checkTransactional.getSelection());

        if ( ! Const.isEmpty(textRouting.getText())) {
            input.setRouting(textRouting.getText());
        }

        if ( ! Const.isEmpty(textLimit.getText())) {
            input.setLimit(textLimit.getText());
        }

        dispose();
    }
}
