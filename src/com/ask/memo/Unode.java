package com.ask.memo;

import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Pattern;

import com.eaio.uuid.UUID;

public class Unode implements MemoNode {
	private Node myNode = null;
	
	public Unode(Node myNode){
		if (myNode == null){
			throw new NullPointerException();
		}
		this.myNode=myNode;
	}
	public Node getRealNode(){
		return myNode;
	}
	@Override
	public String toString(){
		return myNode.toString();
	}
	
	public MemoNode update(String value){
		myNode = myNode.update(value);
		return myNode;
	}
	
	/* Get the value at a given time */
	public String valueAt(Date timestamp){
		return myNode.valueAt(timestamp);
	}
	
	/* time-ordered list of Nodes, newest first */
	public ArrayList<Node> history(){
		return myNode.history();
	}
	
	public UUID getId() {
		return myNode.getId();
	}
	
	public String getValue() {
		return myNode.getValue();
	}

	public Date getTimestamp() {
		return myNode.getTimestamp();
	}
	@Override
	public Arc addParent(MemoNode parent, boolean doOther) {
		Arc arc = myNode.addParent(parent.getRealNode(), doOther);
		myNode = arc.child;
		if (myNode == null)System.out.println("Error, actuatedNode going to null, addParent - bool");
		return arc;
	}
	@Override
	public Arc addChild(MemoNode child, boolean doOther) {
		Arc arc = myNode.addChild(child.getRealNode(), doOther);
		myNode = arc.parent;
		if (myNode == null)System.out.println("Error, actuatedNode going to null, addChild - bool");
		return arc;
	}

	public Arc addParent(MemoNode node){
		Arc arc = myNode.addParent(node);
		myNode = arc.child;
		if (myNode == null)System.out.println("Error, actuatedNode going to null, addParent");
		return arc;
	}
	public Arc addChild(MemoNode node){
		Arc arc = myNode.addChild(node);
		myNode = arc.parent;
		if (myNode == null)System.out.println("Error, actuatedNode going to null, addChild");
		return arc;
	}
	
	public Node bulkAddParents(ArrayList<MemoNode> parents){
		myNode = myNode.bulkAddParents(parents);
		return myNode;
	}
	public Node bulkAddChildren(ArrayList<MemoNode> children){
		myNode = myNode.bulkAddChildren(children);
		return myNode;		
	}
	
	public ArrayList<Node> getChildren(){
		return myNode.getChildren();
	}
	public ArrayList<Node> getParents(){
		return myNode.getParents();
	}
	public ArrayList<Node> getChildrenByValue(String value,int topx){
		return myNode.getChildrenByValue(value, topx);
	}
	public ArrayList<Node> getChildrenByRegEx(Pattern regex,int topx){
		return myNode.getChildrenByRegEx(regex, topx);
	}
	public ArrayList<Node> getChildrenByRange(int lower, int upper, int topx){
		return myNode.getChildrenByRange(lower, upper, topx);
	}
	public String getPropertyValue(String propName){
		return myNode.getPropertyValue(propName);
	}
	public ArrayList<MemoResult> search(MemoNode algorithm,int topx){
		return myNode.search(algorithm, topx);
	}

}
