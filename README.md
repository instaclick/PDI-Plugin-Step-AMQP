IC AMQP Plugin
==============

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
| Body                  | Field that will be used as message body                                       |
| Exchange/Queue name   | The exchange name for producers or queue name for consumers                   |
| Routing key           | Field that store the routing key                                              |
| Limit                 | Max number of message reads when using as consumer                            |



Limitations
-----------

* This plugin does not create any queue/exchange nor bindings. 
* Queue, Exchange and Binding should be created before using the plugin
