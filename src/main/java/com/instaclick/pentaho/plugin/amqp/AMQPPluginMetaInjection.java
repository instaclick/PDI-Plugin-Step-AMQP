
package com.instaclick.pentaho.plugin.amqp;

import com.instaclick.pentaho.plugin.amqp.AMQPPluginMeta.Binding;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.StepInjectionMetaEntry;
import org.pentaho.di.trans.step.StepInjectionUtil;
import org.pentaho.di.trans.step.StepMetaInjectionEntryInterface;
import org.pentaho.di.trans.step.StepMetaInjectionInterface;
import org.pentaho.di.core.Const;
import static com.instaclick.pentaho.plugin.amqp.Messages.getString;
/**
 * This takes care of the external metadata injection into the AMQPPluginMeta class
 *
 * @author Shvedchenko, Denys
 */
public class AMQPPluginMetaInjection implements StepMetaInjectionInterface {


  public enum Entry implements StepMetaInjectionEntryInterface {

    AMQP_MODE( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Type.Label") ),
    AMQP_URI( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.URI.Label") ),
    AMQP_USESSL( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.UseSsl.Label") ),

    BODY( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Body.Label") ),
    TARGET( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Target.Label") ),
    ROUTING( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Routing.Label") ),
    LIMIT( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Limit.Label") ),
    TRANSACTIONAL( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Transactional.Label") ),
    AMQP_USERNAME( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Username.Label") ),
    AMQP_PASSWORD( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Password.Label") ),
    AMQP_HOST( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Host.Label") ),
    AMQP_PORT( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Port.Label") ),
    AMQP_VHOST( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Vhost.Label") ),
    DURABLE( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Durable.Label") ),
    AUTODEL( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Autodel.Label") ),
    REQUEUE( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Requeue.Label") ),
    EXCHTYPE( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Exchtype.Label") ),
    EXCLUSIVE( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Exclusive.Label") ),
    WAITINGCONSUMER( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.WaitingConsumer.Label") ),
    WAITTIMEOUT( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.WaitTimeout.Label") ),
    PREFETCHCOUNT( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.PrefetchCount.Label") ),
    DECLARE( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Declare.Label") ),


    BINDINGS( ValueMetaInterface.TYPE_NONE, getString("AmqpPlugin.Binding.Label") ),
    BINDING_ROW( ValueMetaInterface.TYPE_NONE, "One binding row" ),
    BINDING_TARGET( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Binding.Column.Target.ProducerMode") ),
    BINDING_TARGET_TYPE( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Binding.Column.TargetType.ProducerMode") ),
    BINDING_ROUTING_KEY( ValueMetaInterface.TYPE_STRING, getString("AmqpPlugin.Binding.Column.RoutingKey") );

    private int valueType;
    private String description;

    private Entry( int valueType, String description ) {
      this.valueType = valueType;
      this.description = description;
    }

    /**
     * @return the valueType
     */
    public int getValueType() {
      return valueType;
    }

    /**
     * @return the description
     */
    public String getDescription() {
      return description;
    }

    public static Entry findEntry( String key ) {
      return Entry.valueOf( key );
    }
  }

  private AMQPPluginMeta meta;

  public AMQPPluginMetaInjection( AMQPPluginMeta meta ) {
    this.meta = meta;
  }

  @Override
  public List<StepInjectionMetaEntry> getStepInjectionMetadataEntries() throws KettleException {
    List<StepInjectionMetaEntry> all = new ArrayList<StepInjectionMetaEntry>();

    Entry[] topEntries =
      new Entry[] {
        Entry.AMQP_MODE
	, Entry.AMQP_URI
	, Entry.AMQP_USESSL 
	, Entry.AMQP_USERNAME
	, Entry.AMQP_PASSWORD
	, Entry.AMQP_HOST
	, Entry.AMQP_PORT
	, Entry.AMQP_VHOST
	, Entry.BODY
	, Entry.TARGET
	, Entry.ROUTING
	, Entry.LIMIT
	, Entry.TRANSACTIONAL
	, Entry.DURABLE
	, Entry.AUTODEL
	, Entry.REQUEUE
	, Entry.EXCHTYPE
	, Entry.EXCLUSIVE
	, Entry.WAITINGCONSUMER
	, Entry.WAITTIMEOUT
	, Entry.PREFETCHCOUNT
	, Entry.DECLARE
    };
    for ( Entry topEntry : topEntries ) {
      all.add( new StepInjectionMetaEntry( topEntry.name(), topEntry.getValueType(), topEntry.getDescription() ) );
    }

    // bindings
    //
    StepInjectionMetaEntry bindingsEntry =
      new StepInjectionMetaEntry(
        Entry.BINDINGS.name(), ValueMetaInterface.TYPE_NONE, Entry.BINDINGS.description );
    all.add( bindingsEntry );
    StepInjectionMetaEntry bindingEntry =
      new StepInjectionMetaEntry(
        Entry.BINDING_ROW.name(), ValueMetaInterface.TYPE_NONE, Entry.BINDING_ROW.description );
    bindingsEntry.getDetails().add( bindingEntry );

    Entry[] bindingsEntries = new Entry[] { Entry.BINDING_TARGET, Entry.BINDING_TARGET_TYPE, Entry.BINDING_ROUTING_KEY,};
    for ( Entry entry : bindingsEntries ) {
      StepInjectionMetaEntry metaEntry =
        new StepInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
      bindingEntry.getDetails().add( metaEntry );
    }
    return all;
  }

  @Override
  public void injectStepMetadataEntries( List<StepInjectionMetaEntry> all ) throws KettleException {

    List<String> bindingTargets = new ArrayList<String>();
    List<String> bindingTargetTypes = new ArrayList<String>();
    List<String> bindingRoutingKeys = new ArrayList<String>();

    // Parse the fields, inject into the meta class..
    //
    for ( StepInjectionMetaEntry lookFields : all ) {
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry == null ) {
        continue;
      }

      String lookValue = (String) lookFields.getValue();
      switch ( fieldsEntry ) {
        case BINDINGS:
          for ( StepInjectionMetaEntry lookField : lookFields.getDetails() ) {
            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
            if ( fieldEntry == Entry.BINDING_ROW ) {

              String bindingTarget = null;
              String bindingTargetType = null;
	      String bindingRoutingKey = null;

              List<StepInjectionMetaEntry> entries = lookField.getDetails();
              for ( StepInjectionMetaEntry entry : entries ) {
                Entry metaEntry = Entry.findEntry( entry.getKey() );
                if ( metaEntry != null ) {
                  String value = (String) entry.getValue();
                  switch ( metaEntry ) {
                    case BINDING_TARGET:
                      bindingTarget = value;
                      break;
                    case BINDING_TARGET_TYPE:
                      bindingTargetType = value;
                      break;
                    case BINDING_ROUTING_KEY:
                      bindingRoutingKey = value;
                      break;
                    default:
                      break;
                  }
                }
              }
              bindingTargets.add( bindingTarget );
              bindingTargetTypes.add( bindingTargetType );
              bindingRoutingKeys.add( bindingRoutingKey );
            }
          }
          break;
        case AMQP_MODE:
          meta.setMode( lookValue );
          break;
        case AMQP_URI:
          meta.setUri( lookValue );
          break;
        case AMQP_USESSL:
          meta.setUseSsl( lookValue );
          break;

        case BODY:
          meta.setBodyField( lookValue );
          break;
        case TARGET:
          meta.setTarget( lookValue );
          break;
        case ROUTING:
          meta.setRouting( lookValue );
          break;
        case LIMIT:
          meta.setLimit( lookValue );
          break;
        case TRANSACTIONAL:
          meta.setTransactional( lookValue );
          break;
        case AMQP_USERNAME:
          meta.setUsername( lookValue );
          break;
        case AMQP_PASSWORD:
          meta.setPassword( lookValue );
          break;
        case AMQP_HOST:
          meta.setHost( lookValue );
          break;
        case AMQP_PORT:
          meta.setPort( lookValue );
          break;
        case AMQP_VHOST:
          meta.setVhost( lookValue );
          break;
        case DURABLE:
          meta.setDurable( lookValue );
          break;
        case AUTODEL:
          meta.setAutodel( lookValue );
          break;
        case REQUEUE:
          meta.setRequeue( lookValue );
          break;
        case EXCHTYPE:
          meta.setExchtype( lookValue );
          break;
        case EXCLUSIVE:
          meta.setExclusive( lookValue );
          break;
        case WAITINGCONSUMER:
          meta.setWaitingConsumer( lookValue );
          break;
        case WAITTIMEOUT:
          meta.setWaitTimeout( lookValue );
          break;
        case PREFETCHCOUNT:
          meta.setPrefetchCount( lookValue );
          break;
        case DECLARE:
          meta.setDeclare( lookValue );
          break;


        default:
          break;
      }
    }

    // Pass the grid to the step metadata
    //
    int count = bindingTargets.size();

    for (int i = 0; i< count; i++) {
       String target  = bindingTargets.get(i);
       String target_type = bindingTargetTypes.get(i);
       String routing = bindingRoutingKeys.get(i);

       if (Const.isEmpty(target)) {
            continue;
       }

      meta.addBinding(target, target_type, routing);
    }




  }

  public List<StepInjectionMetaEntry> extractStepMetadataEntries() throws KettleException {
    List<StepInjectionMetaEntry> list = new ArrayList<StepInjectionMetaEntry>();

    list.add( StepInjectionUtil.getEntry( Entry.AMQP_MODE, meta.getMode() ) );
    list.add( StepInjectionUtil.getEntry( Entry.AMQP_USESSL, meta.isUseSsl() ) );
    list.add( StepInjectionUtil.getEntry( Entry.AMQP_URI, meta.getUri() ) );

    list.add( StepInjectionUtil.getEntry( Entry.BODY, meta.getBodyField() ) );
    list.add( StepInjectionUtil.getEntry( Entry.TARGET, meta.getTarget() ) );
    list.add( StepInjectionUtil.getEntry( Entry.ROUTING, meta.getRouting() ) );
    list.add( StepInjectionUtil.getEntry( Entry.LIMIT, meta.getLimitString() ) );
    list.add( StepInjectionUtil.getEntry( Entry.TRANSACTIONAL, meta.isTransactional() ) );
    list.add( StepInjectionUtil.getEntry( Entry.AMQP_USERNAME, meta.getUsername() ) );
    list.add( StepInjectionUtil.getEntry( Entry.AMQP_PASSWORD, meta.getPassword() ) );
    list.add( StepInjectionUtil.getEntry( Entry.AMQP_HOST, meta.getHost() ) );
    list.add( StepInjectionUtil.getEntry( Entry.AMQP_PORT, meta.getPort() ) );
    list.add( StepInjectionUtil.getEntry( Entry.AMQP_VHOST, meta.getVhost() ) );
    list.add( StepInjectionUtil.getEntry( Entry.DURABLE, meta.isDurable() ) );
    list.add( StepInjectionUtil.getEntry( Entry.AUTODEL, meta.isAutodel() ) );
    list.add( StepInjectionUtil.getEntry( Entry.REQUEUE, meta.isRequeue() ) );
    list.add( StepInjectionUtil.getEntry( Entry.EXCHTYPE, meta.getExchtype() ) );
    list.add( StepInjectionUtil.getEntry( Entry.EXCLUSIVE, meta.isExclusive() ) );
    list.add( StepInjectionUtil.getEntry( Entry.WAITINGCONSUMER, meta.isWaitingConsumer() ) );
    list.add( StepInjectionUtil.getEntry( Entry.WAITTIMEOUT, meta.getWaitTimeoutString() ) );
    list.add( StepInjectionUtil.getEntry( Entry.PREFETCHCOUNT, meta.getPrefetchCountString() ) );
    list.add( StepInjectionUtil.getEntry( Entry.DECLARE, meta.isDeclare() ) );


    StepInjectionMetaEntry fieldsEntry = StepInjectionUtil.getEntry( Entry.BINDINGS );
    list.add( fieldsEntry );
    for ( int i = 0; i < meta.getBindings().size(); i++ ) {
      StepInjectionMetaEntry fieldEntry = StepInjectionUtil.getEntry( Entry.BINDING_ROW );
      List<StepInjectionMetaEntry> details = fieldEntry.getDetails();
      final Binding binding = meta.getBindings().get(i);
      details.add( StepInjectionUtil.getEntry( Entry.BINDING_TARGET, binding.getTarget() ) );
      details.add( StepInjectionUtil.getEntry( Entry.BINDING_TARGET_TYPE, binding.getTargetType() ) );
      details.add( StepInjectionUtil.getEntry( Entry.BINDING_ROUTING_KEY, binding.getRouting() ) );
      fieldsEntry.getDetails().add( fieldEntry );
    }

    return list;
  }

  public AMQPPluginMeta getMeta() {
    return meta;
  }
}