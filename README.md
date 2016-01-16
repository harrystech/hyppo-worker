# Hyppo Workers
This codebase represents the components of hyppo that negotiate and execute work to be executed in support of the [hyppo source api](https://github.com/harrystech/hyppo-source-api). The codebase is laid out as follows

## executor/
This codebase is a minimal environment that launches to actuall run code written against the API. The runtime system in hyppo allows integrations to leverage any libraries and or JVM languages of their choosing by not introducing any libraries or other JVM languages that might conflict with it into the JVM hosting the integration code.

Additionally, in an attempt to make modularity and testing easier- the executor only knows about local "java.io.File" and socket instances. It makes executors easy to isolate from the work coordination mechanism and to feed actions either from a test harness or CLI application (any takers on implementing one?).

## worker-api/
This section includes a few utility objects, but mostly just contains the "message" objects that workers use to coordinate work with the coordinator through RabbitMQ. The inputs and outputs that the coordinator and worker use are all described here, but contain no explicit functionality themselves.

## worker/
This is where the code that bridges the RabbitMQ work broker and the executor JVM lives. It prepares the necessary inputs that the executor needs, feeds the executor commands, uploads the result of executor actions, and responds to the coordinator with the results. This is deffinitely the "meat" of the hyppo work system.

