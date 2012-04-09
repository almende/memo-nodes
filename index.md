---
layout: default
title: Introduction
---

# Introduction

MemoNodes is a general purpose graph-based data storage implemented in Java.
It provides a clear, simple and conceptually elegant API, implemented as a REST-full and/or JSON-RPC interface, implementing a technology-independent database.
There are two implementations packaged with the core graph database: 
- an in-memory data store and 
- one data store implementation based on Google App Engine. 

This illustrates the possibility to build simple, efficient, and scalable data stores managed using Platform as a Service (PaaS).
MemoNodes supports extensions with additional model abstraction layers for Model Driven Engineering. 
This can be extended to implement any data storage for web-based applications which need to share data. 

The [CHAP agent platform](#chap) uses MemoNodes as its underlying data store for its knowledge base. 
Even though CHAP uses the existing Google App Engine as implementation, MemoNodes is designed to be platform-independent and can fit other implementations as well. 

The core features of MemoNodes are enumerated below:

- [Java library implementation using Java Beans-like objects](#javabeans)
- [Support exporting data to JSON format for further processing by other CHAP components](#protocol)
- [Generic, general purpose, data storage-independent platform, based on a graph representation](#multipurpose)
- [It is an open-source, modular, open-standards supporting and easy to extend solution](#opensource)
- [Easy to install, to set up and to use](#modular)
- [Lightweight, has a very small memory usage](#multiplatform)

[<img src="img/MemoNodes.png" 
  style="margin-top: 30px;" 
  title="Click for a larger view">](img/MemoNodes.png)

## Java library implementation using Java Beans-like objects {#javabeans}

- **Abstraction layer based on Java Beans for implementing Application-specific Business Entities**  
  MemoNodes can be used as an abstraction to link different data structures corresponding 
  to the Business Entities of a specific domain. One can abstract away the actual 
  properties and attributes of a Business Entity, and use a uniform CRUD-like interface 
  using REST-principles for invoking domain-specific functions. 
  
  This amounts to the fact that MemoNodes implements business logic independently of the data structures.

## Support exporting data to JSON format for further processing by other applications or services/components{#protocol}

- **Support for JSON format and JSON-RPC protocol - Great interoperability with web-based applications**  
  MemoNodes supports the lightweight JSON data interchange format, very popular human-readable format for web applications.
  It also supports JSON-RPC, which is a specific RPC-based communication model implementation using JSON. 

## Generic, general purpose, data storage-independent platform, based on a graph representation{#multipurpose}

- **Wrapping existing services.**  
  Web services can be exposed for applications and software agents by creating a 
  simple "wrapper class" as container for several web services. For example a Question
  service uses the existing generic web services for handling questions and interactions at application level, and makes this information
  service available for other extensions of communication acts, e.g. *ClosedQuestion*, *MultiChoiceQuestion*, *FAQQuestion*, etc.


## It is an open-source, modular, open-standards supporting and easy to extend solution{#opensource}

   MemoNodes is open-source. You can download, compile and run its Java source from the *MemoNodes URL*(#memonodesurl).

## Easy to install, to set up and to use{#modular}

- **Easily set up an agent platform**  
  MemoNodes aims to enable quick prototyping and design of domain specific data stores.

## Lightweight, has a very small memory usage{#multiplatform}

  MemoNodes is a parsimonious implementation of an associative memory, which uses system resources economically. 
  It has therefore good performance and is scalable, and can be used to set up large data stores, where the performance is more stable than in a relational database.

## Multipurpose {#multipurpose}

New data structures and instantiations can be built on any type of domain.
MemoNodes offers the base to built your own domain. 


## Modular set up {#modular}

The conceptual structure of MemoNodes has been set up in a modular way. 
There is one core package, MemoNodes Core, which contains the basic functionallity for handling memory objects, 
data persistency, and base classes to define and store structured data instances. 

On top of this core, other library extensions are built, containing implementations of MemoNodes for different types of storage mechanisms.

### MemoNodes Core

MemoNodes Core is the core library of CHAP. 
It contains the basic functionallity for setting up the domain representation suitable for a scalable multi-agent execution and simulation platform.

### MemoNodes GAE

MemoNodes GAE is a library built on top of MemoNodes core, which offers support for domain-specific applications.

## Open source {#opensource}

MemoNodes is an open platform, and it is encouraged to help extending the
platform by providing new libraries with agents, or create implementations in
other programming languages, or improve existing code. 
One of the key issues for the platform to be successful is that it needs to be
accessible, open, and easy to implement for any developer in any development
environment. 

Offering the software as open source is a logic result of the aims for 
openness and collaborative development.

## Functionality {#functionality}
MemoNodes displays several design features specific for a temporal associative memory with grouping, filtering and querying features. 

It allows dynamic data structures to be formed and transformed using links and dynamic relations similar to what is happening with the neurons of a brain, that encode concepts. 

Among the features of MemoNodes, we count:

- The “cells” of MemoNodes memory are completely immutable Nodes: any change in value and/or Arcs will lead to a new Node creation, but with the same node ID (the old value will become discarded, but still available in the node history).

- Arcs point to Node IDs, all nodes representing the same value (at different times) share the same Node ID.

- Nodes can be added into multiple memory clusters, i.e. groups of nodes sharing similar attributes or grouping criteria

- Nodes are clustered around creation time, which makes retrieval relatively fast in most cases (that is, when the most recent values are retrieved)

- The history of one node can be retrieved by invoking method Node.history()

- The entire graph database (MemoNodes memory) can be reset, i.e. cleared of all nodes (Tabula rasa)

- Memories can be shared by exchanging a Node ID corresponding to a node from which the entire memory sub-graph can be visited.

- The query Language of this graph database is a graph structure pattern matching algorithm which can itself be stored in the graph database, as a subgraph

- The structure of the memory graph is the essence of the associative memory: you can find nodes based on some structural properties they have; this can also be used for temporal patterns

- MemoNodes is a self-modifying storage model: its structure changes over time, allowing updates of its values

You can use MemoNodes with very little training on the use of its API, as illustrated below.

## Example Usage
Let's exemplify the content of MemoNodes.

MemoNodes stores application data in a graph representation, the most generic of data structures, capable of representing any kind of data in an accessible, simple to comprehend and conceptually elegant manner. The graph contains nodes and directed arcs connecting them, to encode data attributes of elements from an arbitrary domain. 

MemoNodes can express state properties, such as:

- Values of a set of Properties at a given time. (Entity E has property P with value V at time T)

- Existence of Relations at a given time.  (Entity E has relation R with a given entity or group of entities G?)

Such properties can be encoded in a graph-based representation as shown below.

**Memory**:
   *Start* (*ROOT*)
    /  \                
>--1    2               
| / \  / \              
|/   3    4
3    |   / \
|    5  5   5
6    |  |   |
      / \   8  7   8
      5 7
      |
      8

These properties can be queried against the graph-based representation as shown below.

**Query Pattern**:
              _                            
             \ /                           
 PreAmble --> any                           
 Pattern  --> 5 -- 8                        
 
**Pattern Matching Query Result**: Pattern matches three 5's from the graph

## References

[1] *MemoNodes*{#memonodes} GitHub project - URL: https://github.com/almende/MemoNodes

[2] CHAP GitHub project - URL: https://github.com/almende/chap
