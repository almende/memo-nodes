package com.chap.memo.memoNodes.servlet;

import java.util.ArrayList;

import com.chap.memo.memoNodes.MemoNode;

import net.sf.json.JSONArray;

final public class JSONTuple {
	JSONArray nodes = new JSONArray();
	JSONArray links = new JSONArray();
	ArrayList<MemoNode> seenNodes= new ArrayList<MemoNode>();
	public JSONArray getNodes() {
		return nodes;
	}
	public void setNodes(JSONArray nodes) {
		this.nodes = nodes;
	}
	public JSONArray getLinks() {
		return links;
	}
	public void setLinks(JSONArray links) {
		this.links = links;
	}
	public ArrayList<MemoNode> getSeenNodes() {
		return seenNodes;
	}
	public void setSeenNodes(ArrayList<MemoNode> seenNodes) {
		this.seenNodes = seenNodes;
	}
}
