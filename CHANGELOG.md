2.3.0
* reafctoring
* fix lost setting port value



2.2.1
* add Confirmation Tab
* add Confirmation row listeners. Listen input of ACK and REJECT steps for deliverytags and Ack, Reject them accordingly
  As no hop (stream) can be created (will made data loop) from those Listeners to Consumer step, you have to configure names of those steps on Consumer along with DeliveryTagFields.
  Using variables for DeliveryTagField you can specify it once
* name of deliveryTag fields from consumer and on ACK and REJECT steps can be different
* non Ack or Rejected messages will be treated as not read
* fixed error when body field and routing key specified with variables can form incorrect stream ( no expansion were performed )

2.1.3
* add check for channel isOpen and connection is Open on close events
* add check for channel isOpen in flush, prevent double flush for consumer.

2.1.2
* add support for MetaData Injector Plugin

2.1.1
* added to Binding table target type, to allow bind exchange to exchange
* fixed store : Autodelete, Exclsuive
* added WaitForData Consumer mode
* added prefetchCount for channel.basicQos method
* added waitTimeout for Consumer mode. it is not delivery(timeout). it is limit for Consumer step to run if WaitForData active
* added support for KettleEncrypted Password for password dedicated field

2.1.0
* added declare for queue/exchange
* added declare option , auto-delete, exclusive, durable, exchange type
* added Binding table , target, routing key
* connection speicfied either by URI or by each parameters 
