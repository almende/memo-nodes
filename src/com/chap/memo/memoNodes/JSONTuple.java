package com.chap.memo.memoNodes;

import net.sf.json.JSONArray;

public class JSONTuple {
	JSONArray nodes = new JSONArray();
	JSONArray links = new JSONArray();
	
	public JSONTuple merge(JSONTuple other){
		nodes.addAll(other.nodes);
		links.addAll(other.links);
		return this;
	}
}
