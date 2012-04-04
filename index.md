---
layout: default
title: Introduction
---

# Introduction

MemoNodes is a general purpose graph-based data storage which is technology-independent database.

This page introduces the core features of MemoNodes:

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

## Support exporting data to JSON format for further processing by other CHAP components{#protocol}

## Generic, general purpose, data storage-independent platform, based on a graph representation{#multipurpose}

## It is an open-source, modular, open-standards supporting and easy to extend solution{#opensource}

## Easy to install, to set up and to use{#modular}

## Lightweight, has a very small memory usage{#multiplatform}

## Multipurpose {#multipurpose}

The MemoNodes agent platform can be used for all kind of applications. 
New types of agents can be built to act on any type of domain.
MemoNodes offers the base to built your own agent platform. 

A number of possible application scenarios for Eve:


- **Easily set up an agent platform**  
  The basic purpose is of course to set up a (scalable) agent platform, containing 
  your own agents and functionality. MemoNodes aims to make it easy to set up the
  platform.

- **Large scale simulations**  
  MemoNodes can be used to set up a large simulation environment,
  making use of the scalability and options for parallel and asynchronous
  processing.

- **Wrapping existing services.**  
  Web services can be made available for software agents by creating a 
  simple "wrapper agent" for the web services. For example a GoogleDirectionsAgent
  which just uses the existing web services of Google and makes this information
  service available for all MemoNodes agents.

- **Linking software systems together**  
  Existing software systems can be linked to the world
  of software agents by creating "wrapper agents" for the systems.
  This way it is possible to link completely separeted software systems together
  via agents, even when the software systems are developed in different 
  development environments or are deployed on different locations. 

- **Abstraction layer**  
  MemoNodes can be used as an abstraction to link different services acting on the
  same domain together. For example one can abstract from different calendaring
  systems by creating agents having the same interface but linked to a different
  calendaring system (Gmail, Exchange, iCal, ...).
  
- And possibly more...



## Modular set up {#modular}

The sourcecode of MemoNodes has been set up in a modular way. There is one core
package, MemoNodes Core, which contains the basic functionallity for webservices, 
persistency, and base classes used to develop new types of agents. 
On top of this core, various libraries libraries are built, containing agents 
acting on different application domains such as calendaring, planning, 
communication, negotiation, reporting, and information systems.


### MemoNodes Core

MemoNodes Core is the core library of Eve. 
It contains the basic functionallity for webservices, to set up a scalable
agent platform.
Furthermore it contains solutions for persistency, and contains base classes 
used to develop new types of agents. 

### MemoNodes Planning

MemoNodes Planning is a library built on top of Eve, which offers agents acting on the 
domain of calendaring and planning.
These agents can take over all kind of (small) administrative tasks from the 
user, such as dynamically planning of meetings in a calendar, planning travel time. 
Examples are:


- The calendar of a user is managed by a
*CalendarAgent*. It does not matter to what type of
calendar the CalendarAgent itself is linked to: Gmail, Exchange, iCal, ...

- Appointments in the calendar are managed
dynamically by *MeetingAgents*. They negotiate with
each other on a time slot in the calendar of their
participant(s), and automatically move appointments
when needed.

- *TravelAgents* automatically plan travel time when
consecutive appointments with differing locations are
found in the calendar.


## Open source {#opensource}

MemoNodes is an open platform, and it is encouraged to help extending the
platform by providing new libraries with agents, or create implementations in
other programming languages, or improve existing code. 
One of the key issues for the platform to be successful is that it needs to be
accessible, open, and easy to implement for any developer in any development
environment. 

Offering the software as open source is a logic result of the aims for 
openness and collaborative development.

