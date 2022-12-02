# Axon Server - Standard Edition

[Axon Server](https://developer.axoniq.io/axon-server/overview) is a zero-configuration message router and event store.
The message router has a clear separation of
different message types: events, queries and commands. The event store is optimized to handle huge volumes of events
without performance degradation.

Axon Server uses HTTP/2 for its connections, specifically Google's gRPC protocol, which adds a binary Protobuf-based
RMI layer on top of HTTP/2. This is a very efficient protocol supporting two-way communication.
All connections with Axon Server is client initiated. This makes it easy to set up, as it is only the clients that need
to know the location of Axon Server. Axon Server does not need to know the location of the clients.

Axon Server is initially built to support
distributed [Axon Framework](https://developer.axoniq.io/axon-framework/overview) microservices. Starting from Axon
Framework version 4
Axon Server is the default implementation for the CommandBus, EventBus/EventStore, and QueryBus interfaces. Though this
is
the initial approach, usage of Axon Server is not limited to Axon Framework applications.

## Getting started

Numerous resources can help you on your journey in using Axon Server.
A good starting point is [AxonIQ Developer Portal](https://developer.axoniq.io/), which provides links to resources
like blogs, videos, and descriptions.

Furthermore, below are several other helpful resources:

- We have our very own [academy](https://academy.axoniq.io/)! The introductory courses are free, followed by more
  in-depth (paid) courses.
- When ready, you can quickly and easily start your very own Axon Framework based application
  at https://start.axoniq.io/. Note that this solution is only feasible if you want to stick to the Spring ecosphere.
- The [reference guide](https://docs.axoniq.io/) explains all details for using Axon Server
- If the guide doesn't help, our [forum](https://discuss.axoniq.io/) provides a place to ask questions you have during
  development.

## Receiving help

Are you having trouble using any of our products? Know that we want to help you out the best we can! There are a couple
of things to consider when you're traversing anything Axon:

- Checking the [reference guide](https://docs.axoniq.io/) should be your first stop.
- When the reference guide does not cover your predicament, we would greatly appreciate it if you could file
  an [issue](https://github.com/AxonIQ/reference-guide/issues) for it.
- Our [forum](https://discuss.axoniq.io/) provides a space to communicate with the Axon community to help you out.
  AxonIQ developers will help you out on a best-effort basis. And if you know how to help someone else, we greatly
  appreciate your contributions!
- We also monitor Stack Overflow for any question tagged with [axon](https://stackoverflow.com/questions/tagged/axon).
  Similarly to the forum, AxonIQ developers help out on a best-effort basis.

## About this repository

This repository contains the sources to build Axon Server Standard Edition.
Files in this repository are subject to the AxonIQ Open Source License, unless explicitly stated otherwise.

