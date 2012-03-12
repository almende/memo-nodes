package com.chap.memo.memoNodes;

import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Pattern;

import com.eaio.uuid.UUID;

public interface MemoNode {
	public Node getRealNode();

	public MemoNode update(String value);

	public String valueAt(Date timestamp);

	public ArrayList<Node> history();

	public UUID getId();

	public String getValue();

	public Date getTimestamp();

	public Arc addParent(MemoNode parent, boolean doOther);

	public Arc addChild(MemoNode child, boolean doOther);

	public Arc addParent(MemoNode node);

	public Arc addChild(MemoNode node);

	public MemoNode delChild(MemoNode child);

	public MemoNode delChild(MemoNode child, boolean doOther);

	public MemoNode delParent(MemoNode parent);

	public MemoNode delParent(MemoNode parent, boolean doOther);

	public MemoNode bulkAddParents(ArrayList<MemoNode> parents);

	public MemoNode bulkAddChildren(ArrayList<MemoNode> children);

	public MemoNode bulkDelParents(ArrayList<MemoNode> parents);

	public MemoNode bulkDelChildren(ArrayList<MemoNode> children);

	public ArrayList<MemoNode> getChildren();

	public ArrayList<MemoNode> getParents();

	public ArrayList<MemoNode> getChildrenByValue(String value, int topx);

	public ArrayList<MemoNode> getChildrenByRegEx(Pattern regex, int topx);

	public ArrayList<MemoNode> getChildrenByRange(int lower, int upper, int topx);

	public String getPropertyValue(String propName);

	public MemoNode setPropertyValue(String propName, String propValue);

	public ArrayList<MemoNode> search(ArrayList<MemoNode> preambles,
			ArrayList<MemoNode> patterns, int topx);

	public ArrayList<MemoNode> search(MemoNode preamble, MemoNode pattern,
			int topx);

	public ArrayList<MemoNode> search(MemoNode algorithm, int topx);
	
	public String toJSON(String result, int depth);
}
