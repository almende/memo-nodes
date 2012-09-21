package com.chap.memo.memoNodes.servlet;

import java.util.ArrayList;

import com.eaio.uuid.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

final public class JSONTuple {
	static final ObjectMapper om = new ObjectMapper();

	ArrayNode nodes = om.createArrayNode();
	ArrayNode links = om.createArrayNode();
	ArrayList<UUID> seenNodes= new ArrayList<UUID>();
	public ArrayNode getNodes() {
		return nodes;
	}
	public void setNodes(ArrayNode nodes) {
		this.nodes = nodes;
	}
	public ArrayNode getLinks() {
		return links;
	}
	public void setLinks(ArrayNode links) {
		this.links = links;
	}
	public ArrayList<UUID> getSeenNodes() {
		return seenNodes;
	}
	public void setSeenNodes(ArrayList<UUID> seenNodes) {
		this.seenNodes = seenNodes;
	}
}
