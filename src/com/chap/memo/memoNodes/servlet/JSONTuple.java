package com.chap.memo.memoNodes.servlet;

import java.util.ArrayList;

import com.chap.memo.memoNodes.MemoNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

final public class JSONTuple {
	static final ObjectMapper om = new ObjectMapper();

	ArrayNode nodes = om.createArrayNode();
	ArrayNode links = om.createArrayNode();
	ArrayList<MemoNode> seenNodes= new ArrayList<MemoNode>();
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
	public ArrayList<MemoNode> getSeenNodes() {
		return seenNodes;
	}
	public void setSeenNodes(ArrayList<MemoNode> seenNodes) {
		this.seenNodes = seenNodes;
	}
}
