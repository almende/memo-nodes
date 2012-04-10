---
layout: default
title: Communication protocol
---

# Communication Protocol


Communication with MemoNodes can be done via regular HTTP POST requests, 
using the JSON-RPC protocol.
This is a simple and readable protocol, using JSON to format requests and responses. 
[JSON](http://www.json.org/) (JavaScript Object Notation) 
is a lightweight, flexible data-interchange format, easy to read for humans to read and write, 
and for machines to parse and generate. 

## JSON-RPC

JSON-RPC is implemented in two ways:

- **Asynchronous**  
  Requester performs an HTTP POST request to a MemoNodes instance.
  When MemoNodes has completed the action, it calls back the requester at the specified callback URL, containing the response.

Note that only JSON-RPC 2.0 is supported.
In JSON-RPC 2.0, method parameters are defined as an object with *named* parameters,
unlike JSON-RPC 1.0 where method parameters are defined as an array with *unnamed*
parameters, which is much more ambiguous.

## Asynchronous communication

Requester (which can be an agent or a human) performs a request to MemoNodes. 

<table class="example" summary="Asynchronous request 1/2">
<tr>
<th class="example">Url</th><td class="example"><pre class="example">http://myserver.com/memo/Y</pre></td>
</tr>
<tr>
<th class="example">Request</th><td class="example"><pre class="example">{ 
  "id": 1,
  "method": "add_domain",
  "params": {
    "domain": "Questionnaire", 
    "namespace": "http://questionnaire.com/domain/ns:questionnaire"
  },
  "callback": {
    "url": "http://myserver.com/agentX",
    "method": "addCallback"
  }
}</pre></td>
</tr>
<tr>
<th class="example">Response</th><td class="example"><pre class="example">{
  "id": 1,
  "result": null,
  "error": null
}</pre></td>
</tr>
</table>

As soon as MemoNodes has executed the task from the queue, it returns the result
via a new request, adressing the callback url and method of agent X:

<table class="example" summary="Asynchronous request 2/2">
<tr>
<th class="example">Url</th><td class="example"><pre class="example">http://myserver.com/agent/X</pre></td>
</tr>
<tr>
<th class="example">Request</th><td class="example"><pre class="example">{
  "id": 1,
  "method": "addCallback",
  "params": {
    "result: "domain(Questionnaire, http://questionnaire.com/domain/ns:questionnaire)",
    "error": null
  }
}</pre></td>
</tr>
<tr>
<th class="example">Response</th><td class="example"><pre class="example">{
  "id": 1,
  "result": null,
  "error": null
}</pre></td>
</tr>
</table>

## Documentation

Documentation on the JSON-RPC protocol can be found via the following links:

- [http://www.json.org](http://www.json.org)
- [http://json-rpc.org](http://json-rpc.org)
- [http://jsonrpc.org](http://jsonrpc.org)
- [http://en.wikipedia.org/wiki/Json](http://en.wikipedia.org/wiki/Json)
- [http://en.wikipedia.org/wiki/JSON_RPC](http://en.wikipedia.org/wiki/JSON_RPC)
