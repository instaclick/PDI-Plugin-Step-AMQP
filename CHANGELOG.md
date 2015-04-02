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
