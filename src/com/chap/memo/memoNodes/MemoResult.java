package com.chap.memo.memoNodes;

import java.util.ArrayList;

class MemoResult {
	ArrayList<Node> match = null;
	Node result = null;
	public MemoResult(Node node,ArrayList<Node> match){
		this.match = match;
		this.result = node;
	}
}