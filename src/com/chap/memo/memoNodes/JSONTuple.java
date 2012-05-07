package com.chap.memo.memoNodes;

import net.sf.json.JSONArray;

final public class JSONTuple {
	JSONArray nodes = new JSONArray();
	JSONArray links = new JSONArray();
	
	JSONTuple merge(JSONTuple other){
		nodes.addAll(other.nodes);
		links.addAll(other.links);
		return this;
	}
}
