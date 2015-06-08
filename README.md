IC AMQP Plugin
==============

[![Build Status](https://travis-ci.org/instaclick/PDI-Plugin-Step-AMQP.svg?branch=2.0.x)](https://travis-ci.org/instaclick/PDI-Plugin-Step-AMQP)

## Compatible with PDI/Kettle 5.x 

Building
--------
The IC AMQP Plugin is built with maven for dependency management.
All you'll need to get started is maven.

    $ git clone https://github.com/instaclick/PDI-Plugin-Step-AMQP.git
    $ cd PDI-Plugin-Step-AMQP
    $ mvn package


This will produce a pentaho plugin in ``target/ic-amqp-plugin-pdi-<version>.zip``
This archive can then be extracted into your Pentaho Data Integration plugin directory.


Download Packages
-----------------
https://github.com/instaclick/pdi-marketplace-packages


PDI Step Configuration
-----------------------

| Property              | Description                                                                   |
| ----------------------|:-----------------------------------------------------------------------------:|
| Type                  | If the step is a consumer or a producer                                       |
| URI                   | AMQP connection URI (amqp://userName:password@hostName:portNumber/virtualHost)|
| Username              | Username , can be specified as variables                                      |
| Password              | Password , can be specified as variables, can be Encrypted                    |
| Host                  | Host, can be specified as variables                                           |
| Port                  | Port , can be specified with variables                                        |
| Vhost                 | VirtualHost , can be specified with variables                                 |
| UseSsl                | to use ssl or not                                                             |
| Body                  | Field that will be used as message body                                       |
| Delivery Tag          | Carry amqpDeliveryTag, used for confirmation                                  |
| Exchange/Queue name   | The exchange name for producers or queue name for consumers                   |
| Routing key           | Field that store the routing key                                              |
| Limit                 | Max number of message reads when using as consumer                            |
| Declare               | work with pre configured Exchange, Queue and Binding ,or manage them by plugin|
| Durable               | Durability for message                                                        |
| Autodelete            | Autodelete Exchange or Queue after produce,consume                            |
| Exclusive             | for queue , exclsuive usage                                                   |
| Binding               | Target, Routing, Target type ( queue or exchange in PRODUCER mode ) specified |
| Wait for Messages     | Consumer waiting for messages mode                                            |
| Wait Timeout          | Consumer waiting for messages mode, waiting tiemout, 0 for no timeout         |
| PrefetchCount         | Consumer mode, basicQos parameter                                             |
| Requeue               | For consumer. after receiving messages, Nack them with requeue flag           |

STEP Confirmation steps ralted
--------------------------------
| Property                            | Description                                                                                                                        |
| ------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------:|
| Ack Msg Step name                   | Name of step which input row will be monitored and each deliveryTag value will be used to Confirm messages received                |
| Ack Msg Step DeliveryTag Field      | Name of field carry delivery tag value in input stream of AckMsg Step                                                              |
| Rejected Msg Step Name              | Name of step which input row will be monitored and each deliveryTag value will be used to Reject ( dead-letter ) messages received |
| Rejected Msg Step DeliveryTag Field | Name of field carry delivery tag value in input stream of Rejected Msg Step                                                        |


Limitations
-----------
* No declaration for target, These must be "declared" before they can be used.
* PrefetchCount is incomaptible with Transcations, as kettle
