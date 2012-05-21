package com.chap.memo.memoNodes;

import java.util.ArrayList;
import net.sf.json.JSONArray;

final public class JSONTuple {
	JSONArray nodes = new JSONArray();
	JSONArray links = new JSONArray();
	ArrayList<MemoNode> seenNodes= new ArrayList<MemoNode>();
}
