package com.chap.memo.memoNodes;

import java.util.ArrayList;

public class MemoResult {
	ArrayList<MemoNode> match = null;
	MemoNode result = null;

	public MemoResult(MemoNode node, ArrayList<MemoNode> match) {
		this.match = match;
		this.result = node;
	}

	public ArrayList<MemoNode> getMatch() {
		return match;
	}

	public MemoNode getResult() {
		return result;
	}

	public void setMatch(ArrayList<MemoNode> match) {
		this.match = match;
	}

	public void setResult(MemoNode result) {
		this.result = result;
	}

}