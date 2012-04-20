package com.chap.memo.memoNodes.NewImpl;

import net.sf.json.JSONArray;

final public class JSONTuple {
	JSONArray nodes = new JSONArray();
	JSONArray links = new JSONArray();
	
	public JSONTuple merge(JSONTuple other){
		nodes.addAll(other.nodes);
		links.addAll(other.links);
		return this;
	}
}
