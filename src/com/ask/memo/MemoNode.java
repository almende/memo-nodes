package com.ask.memo;

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
	public Arc addParent(MemoNode parent,boolean doOther);
	public Arc addChild(MemoNode child,boolean doOther);
	public Arc addParent(MemoNode node);
	public Arc addChild(MemoNode node);
	public Node bulkAddParents(ArrayList<MemoNode> parents);
	public Node bulkAddChildren(ArrayList<MemoNode> children);
	public ArrayList<Node> getChildren();
	public ArrayList<Node> getParents();
	public ArrayList<Node> getChildrenByValue(String value,int topx);
	public ArrayList<Node> getChildrenByRegEx(Pattern regex,int topx);
	public ArrayList<Node> getChildrenByRange(int lower, int upper, int topx);
	public String getPropertyValue(String propName);
	public ArrayList<MemoResult> search(MemoNode algorithm,int topx);
}
