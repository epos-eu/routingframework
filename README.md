# EPOS Routing Framework

This framework enables EPOS components, written in Java, to connect to the ICS-C message bus and allow components to communicate with each other. The framework's main aim is to reduce the amount of code a component needs to write for message communication to a minimum.

It supports a specific architectural style of communication which may be best described as _Chained Remote Procedure Calling_. The Remote Procedure Call (RPC) part refers to the fact that a request will originate from some component (e.g. the WebAPI component as a result of a HTTP request) and then be passed to another for processing. The initiating component then waits for a response to come back. (At present the process initiating the request will be blocked until the response is received.)

The "chained" part of _chained Remote Procedure calling_ refers to the fact that there can be multiple components involved in the generation of the response  passed back to the initiating component.

Another aspect of the architecture is that for a given request there is no parallel component processing, so no forking/joining capability is supported: A message will flow from one component to the next in a serial manner. In Message-Orientated middleware (MOM) terminolgy this implies message communication adheres more to a message queueing model rather than a publish-subscribe model.

The implementation is specific to the technology underlying the ICS-C message bus, which consists of the **RabbitMQ** message broker using the **AMQP (Advanced Message Queuing Protocol)** protocol.

See [usage and architecture documentation](./documentation/ARCHITECTURE.md)

