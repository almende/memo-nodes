---
layout: default
title: Examples
---

# Examples MemoNodes usage

Let's exemplify the content of MemoNodes for a specific application domain.

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

