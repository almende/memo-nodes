package com.chap.memo.memoNodes;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import com.eaio.uuid.UUID;

/*
 * This file acts like a simple compile-time switchboard between storage models. 
 * Two models are available at this time: In memory only (localNodeList) and datastore backed.
 */
final class NodeList {
	static NodeListIntf nodeList = new LocalNodeList();
	//static NodeListIntf nodeList = new datastoreNodeList();
	
	// Get newest version of Node
	public static Node find(UUID id) {
		return nodeList.find(id);
	}
	public static Node findBefore(UUID id,Date timestamp) {
		return nodeList.findBefore(id,timestamp);
	}
	// Time-ordered list of node instances, newest first
	public static ArrayList<Node> findAll(UUID id){
		return nodeList.findAll(id);
	}
	// Add new instance to storage
	public static void store(Node node) {
		nodeList.store(node);
	}	
}

interface NodeListIntf{
	public Node find(UUID id);
	public Node findBefore(UUID id,Date timestamp);
	public ArrayList<Node> findAll(UUID id);
	public void store(Node node);
}

final class LocalNodeList implements NodeListIntf {
	static HashMap<UUID,ArrayList<Node>> knownNodes = new HashMap<UUID,ArrayList<Node>>(1000000);
	public Node find(UUID id){	
		ArrayList<Node> res = knownNodes.get(id);
		if (res != null && !res.isEmpty()) {
			Node result = null;
			Iterator<Node> iter = res.iterator();
			while (iter.hasNext()){
				Node next = iter.next();
				if (result == null || next.getTimestamp().after(result.getTimestamp())){
					result=next;
				}
			}
			return result;
		}
		return null;
	}
	public Node findBefore(UUID id,Date timestamp){	
		ArrayList<Node> res = knownNodes.get(id);
		if (res != null && !res.isEmpty()) {
			Node result = null;
			Iterator<Node> iter = res.iterator();
			while (iter.hasNext()){
				Node next = iter.next();
				if (next.getTimestamp().before(timestamp)){
					if (result == null || next.getTimestamp().after(result.getTimestamp())){
						result=next;
					}
				}
			}
			return result;
		}
		return null;
	}
	public void store(Node node){
		ArrayList<Node> cur = knownNodes.get(node.getId());
		if (cur != null) {
			int size = cur.size();
			boolean found = false;
			for (int i=0; i<size;i++){
				Date comp=cur.get(i).getTimestamp();
				if (node.getTimestamp().equals(comp) || comp.before(node.getTimestamp())){
					cur.add(i, node);
					found = true;
					break;
				}
			}
			if (!found){
				cur.add(node);
			}
		}else {
			cur = new ArrayList<Node>(3);
			cur.add(node);
		}
		knownNodes.put(node.getId(), cur);
	}
	public ArrayList<Node> findAll(UUID id){
		return knownNodes.get(id);
	}
}
final class datastoreNodeList implements NodeListIntf {
	public Node find(UUID id){
		return MemoShardStore.findNode(id);
	}
	public Node findBefore(UUID id,Date timestamp){
		return MemoShardStore.findBefore(id,timestamp);
	}
	public ArrayList<Node> findAll(UUID id){
		return MemoShardStore.findAll(id);
	}
	public void store(Node node){
		MemoShardStore.addNode(node);
	}
}