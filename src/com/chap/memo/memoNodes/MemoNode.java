package com.chap.memo.memoNodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

import com.chap.memo.memoNodes.bus.MemoProxyBus;
import com.chap.memo.memoNodes.bus.MemoReadBus;
import com.chap.memo.memoNodes.bus.MemoWriteBus;
import com.chap.memo.memoNodes.model.ArcList;
import com.chap.memo.memoNodes.model.NodeValue;
import com.chap.memo.memoNodes.servlet.JSONTuple;
import com.eaio.uuid.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * MemoNode is a graph database, designed to run effectively on the Google App Engine datastore. 
 * It features arbitrary length byte values, full history and a self-referencing pattern-matching
 * search facility. 
 * 
 * Copyright: Ludo Stellingwerff, Almende B.V.
 * License:   Apache License Version 2.0
 * @see <a href="http://chap.almende.com/">Part of CHAP</a>
 * 
 * @author Ludo Stellingwerff
 * @author Almende B.V.
 * @version 1.0
 * 
 */
public class MemoNode implements Comparable<MemoNode> {
	private static UUID ROOT = new UUID("00000000-0000-002a-0000-000000000000");
	
//	private static int existingNodes = 0;
	static final ObjectMapper om = new ObjectMapper();
	
	private MemoReadBus readBus = MemoReadBus.getBus();
	private MemoWriteBus writeBus = MemoWriteBus.getBus();
	private MemoProxyBus proxyBus = MemoProxyBus.getBus();
	private long lastUpdate= System.currentTimeMillis();
	
	private UUID uuid;
	private NodeValue value = null;
	private final ArcList parents;
	private final ArcList children;
	public boolean isProxy=false;
	
	/**
	 * Makes sure all graph changes are written to the datastore. It is advisable to run this
	 * at least at the end of each Servlet call.
	 */
	public static void flushDB(){
		MemoWriteBus.getBus().flush();
	}
	/**
	 * Dangerous! Empties the entire graph database, irreversible! You will loose data...
	 */
	public static void emptyDB(){
		MemoWriteBus.emptyDB();
	}
	/**
	 * Get the node that can serve as a tree root, providing at least one anchor for the database.
	 * Use sparsely as this node will otherwise get a lot of children, better to use one or more 
	 * intermediate nodes.
	 * 
	 * Root node has UUID: 00000000-0000-002a-0000-000000000000 
	 */
	public static MemoNode getRootNode() {
		MemoReadBus readBus = MemoReadBus.getBus();
		MemoNode result = readBus.find(ROOT);
		if (result == null) {
			result = new MemoNode(ROOT, "root");
		}
		return result;
	}
	
	@Override
	public int compareTo(MemoNode o) {
		if (this.getId().equals(o.getId())) return 0;
		return (int) ((this.getTimestamp() - o.getTimestamp())%1);
	}
	
	@Override
	public int hashCode(){
		return this.getId().hashCode();
	}

	@Override
	public boolean equals(Object o){
		if (o instanceof MemoNode) {
			return this.getId().equals(((MemoNode)o).getId());
		} else {
			return false;
		}
	}
	
	/*	protected void finalize(){
		if (existingNodes > 100000) System.out.println("Quite many nodes found!"+existingNodes);
		existingNodes--;
	}*/
	/**
	 * Find or create node with specified UUID. This is the recommended way to obtain 
	 * existing nodes of which you know the UUID. If node can't be found, this node will have an
	 * empty value;
	 * 
	 * @see <a href="http://johannburkard.de/software/uuid/">http://johannburkard.de/software/uuid/</a>
	 */
	public MemoNode(UUID uuid){
//		existingNodes++;
		this.uuid=uuid;
		this.isProxy=proxyBus.isProxy(uuid);
		this.parents=new ArcList(uuid,0,isProxy);
		this.children=new ArcList(uuid,1,isProxy);
	}
	/**
	 * Create new node with specified value.
	 * 
	 */
	public MemoNode(byte[] value){
//		existingNodes++;
		this.uuid=new UUID();
		this.value=writeBus.store(this.uuid, value);
		this.parents=new ArcList(this.uuid,0,false);
		this.children=new ArcList(this.uuid,1,false);
	}
	/**
	 * Create new node with specified string value.
	 * 
	 */	
	public MemoNode(String value){
//		existingNodes++;
		this.uuid=new UUID();
		this.value=writeBus.store(this.uuid, value.getBytes());
		this.parents=new ArcList(this.uuid,0,false);
		this.children=new ArcList(this.uuid,1,false);
	}
	/**
	 * Find or create node with specified UUID and value. If node existed it will be updated to the
	 * provided value.
	 * 
	 * @see <a href="http://johannburkard.de/software/uuid/">http://johannburkard.de/software/uuid/</a>
	 */
	public MemoNode(UUID uuid,byte[] value){
//		existingNodes++;
		this.uuid=uuid;
		this.isProxy=proxyBus.isProxy(uuid);
		this.parents=new ArcList(this.uuid,0,isProxy);
		this.children=new ArcList(this.uuid,1,isProxy);
		if (this.isProxy){
			this.value=proxyBus.store(uuid, value);
		} else {
			this.value=writeBus.store(uuid, value);
		}
	}
	/**
	 * Find or create node with specified UUID and value. If node existed it will be updated to the
	 * provided string value.
	 * 
	 * @see <a href="http://johannburkard.de/software/uuid/">http://johannburkard.de/software/uuid/</a>
	 */	
	public MemoNode(UUID uuid,String value){
//		existingNodes++;
		this.uuid=uuid;
		this.isProxy=proxyBus.isProxy(uuid);
		this.parents=new ArcList(this.uuid,0,isProxy);
		this.children=new ArcList(this.uuid,1,isProxy);
		if (this.isProxy){
			this.value=proxyBus.store(uuid, value.getBytes());
		} else {
			this.value=writeBus.store(uuid, value.getBytes());		
		}
	}
	public MemoNode(NodeValue value){
//		existingNodes++;
		if (value!=null){
			this.uuid=value.getId();
			this.value=value;
		} else {
			System.out.println("Null value given, generating new UUID!");
			this.uuid=new UUID();
			this.value=null;
		}
		this.parents=new ArcList(this.uuid,0,false);
		this.children=new ArcList(this.uuid,1,false);
	}
	/**
	 * Update node's value to specified value.
	 * 
	 * @return this node
	 */
	public MemoNode update(byte[] value){
		if (value == null) value =  new byte[0];
		if (this.isProxy){
			this.value=proxyBus.store(uuid, value);
		} else {
			this.value=writeBus.store(this.getId(), value);
		}
		return this;
	}
	/**
	 * Update node's value to specified string value.
	 * 
	 * @return this node
	 */
	public MemoNode update(String value){
		if (this.isProxy){
			this.value=proxyBus.store(uuid, value.getBytes());
		} else {
			this.value=writeBus.store(this.getId(), value.getBytes());
		}
		return this;
	}
	/**
	 * Add a new parent arc between a node with specified parent UUID and this node, effectively
	 * making this node a child of the provided node.
	 * 
	 * @see <a href="http://johannburkard.de/software/uuid/">http://johannburkard.de/software/uuid/</a>
	 */
	public void addParent(UUID parent){
		parents.addNode(parent);
	}
	/**
	 * Add a new child arc between a node with specified child UUID and this node, effectively making
	 * the provided node a child of this node.
	 * 
	 * @see <a href="http://johannburkard.de/software/uuid/">http://johannburkard.de/software/uuid/</a>
	 */
	public void addChild(UUID child){
		children.addNode(child);
	}
	/**
	 * Remove the parent arc between the node with specified parent UUID and this node, effectively
	 * making this node no longer a child of the provided node.
	 * 
	 * @see <a href="http://johannburkard.de/software/uuid/">http://johannburkard.de/software/uuid/</a>
	 */
	public void delParent(UUID parent){
		parents.delNode(parent);
	}
	/**
	 * Remove the child arc between this node and the node with the provided child UUID, effectively making
	 * this node no longer a parent of the provided node.
	 * 
	 * @see <a href="http://johannburkard.de/software/uuid/">http://johannburkard.de/software/uuid/</a>
	 */
	public void delChild(UUID child){
		children.delNode(child);
	}
	/**
	 * Add a new parent arc between the specified parent node and this node, effectively
	 * making this node a child of the provided node.
	 * 
	 * @return parent
	 */
	public MemoNode addParent(MemoNode parent){
		addParent(parent.getId());
		return parent;
	}
	/**
	 * Add a new child arc between the specified child node and this node, effectively making
	 * the provided node a child of this node.
	 * 
	 * @return child
	 */
	public MemoNode addChild(MemoNode child){
		addChild(child.getId());
		return child;
	}
	/**
	 * Add a new parent arc between the specified parent node and this node, effectively
	 * making this node a child of the provided node.
	 * 
	 * @return this node
	 */
	public MemoNode setParent(MemoNode parent){
		addParent(parent.getId());
		return this;
	}
	/**
	 * Add a new child arc between the specified child node and this node, effectively making
	 * the provided node a child of this node.
	 * 
	 * @return this node
	 */
	public MemoNode setChild(MemoNode child){
		addChild(child.getId());
		return this;
	}
	/**
	 * Remove the parent arc between the specified parent node and this node, effectively
	 * making this node no longer a child of the provided node.
	 * 
	 * @return this node
	 */
	public MemoNode delParent(MemoNode parent){
		delParent(parent.getId());
		return this;
	}
	/**
	 * Remove the child arc between this node and the provided child node, effectively making
	 * this node no longer a parent of the provided node.
	 * 
	 * @return this node
	 */
	public MemoNode delChild(MemoNode child){
		delChild(child.getId());
		return this;
	}
	/**
	 * Get current value of node. If node has been deleted, can't be found or has been 
	 * updated to a null value, this call returns a zero-size byte[].
	 * 
	 * @return byte[], zero-size if node not found/empty/null
	 */
	public byte[] getValue(){
		if (this.isProxy){
			//TODO: check for valueChanged, or only update if value==null?
			this.value = proxyBus.getValue(this.uuid);
		} else {
			if (this.value == null || readBus.valueChanged(lastUpdate)){
				this.value=readBus.getValue(this.uuid);
				lastUpdate=System.currentTimeMillis();
			}
		}
		return this.value == null?null:this.value.getValue(); 
	}
	/**
	 * Get current value of node as a string. The returned string is limited to 250 bytes, for
	 * larger strings use "new String(getValue())" instead.
	 * If node has been deleted, can't be found or has been updated to a null value,
	 * this call returns an empty string.
	 * 
	 * @return String, limited to 250 bytes.
	 */
	public String getStringValue(){
		byte[] res = getValue();
		if (res != null) {
			return new String(res,0,Math.min(250, res.length));
		}
		return "";
	}
	public byte[] valueAt(long timestamp){
		NodeValue oldValue;
		if (this.isProxy){
			oldValue=proxyBus.getValue(getId(), timestamp);
		} else {
			oldValue=readBus.getValue(getId(), timestamp);	
		}
		return oldValue.getValue();
	}
	public ArrayList<MemoNode> history(){
		ArrayList<MemoNode> result = readBus.findAll(getId());
		if (!result.get(result.size()-1).equals(this)){
			result.add(this);	
		}
		return result;
	}
	/**
	 * Returns the UUID of this node. 
	 * 
	 * @see <a href="http://johannburkard.de/software/uuid/">http://johannburkard.de/software/uuid/</a>
	 */
	public UUID getId(){
		return this.uuid;
	}
	/**
	 * Returns the timestamp of the latest update (or creation) to this node as the amount of microseconds since midnight 1 Jan 1970. 
	 * 
	 * @return long, amount of microseconds since 1-1-1970 00:00:00.00;
	 */
	public long getTimestamp(){
		return Math.max(this.value.getTimestamp_long(),Math.max(this.children.getTimestamp_long(),this.parents.getTimestamp_long()));
	}
	/**
	 * Returns the list of direct parent nodes. 
	 * 
	 * @return ArrayList<MemoNode> parents
	 */
	public ArrayList<MemoNode> getParents(){
		return this.parents.getNodes();
	}
	/**
	 * Returns the list of direct child nodes. 
	 * 
	 * @return ArrayList<MemoNode> children
	 */
	public ArrayList<MemoNode> getChildren(){
		return this.children.getNodes();
	}
	/**
	 * Returns all children whose string value equal the given string.
	 * 
	 * @param value the string value to compare with
	 * @param topx return a maximum of topx children (Set to 0 for all matching children)
	 * @return ArrayList<MemoNode> children
	 */
	public ArrayList<MemoNode> getChildrenByStringValue(String value, int topx){
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(
				this.children.getLength());
		for (MemoNode child : getChildren()) {
			if (child.getStringValue().equals(value)) {
				result.add(child);
				if (topx > 0 && result.size() >= topx)
					return result;
			}
		}
		return result;
	}
	/**
	 * Get a single child whose string value equals the given string. If multiple 
	 * children are found, return the (arbitrary) first one.
	 * 
	 * @param value the string value to compare with
	 * @return MemoNode child
	 */
	public MemoNode getChildByStringValue(String value){
		ArrayList<MemoNode> children = getChildrenByStringValue(value,1);
		if (children != null && children.size()>0){
			return children.get(0);
		}
		return null;
	}
	/**
	 * Returns all children whose string value match the given regular expression.
	 * 
	 * @see  java.util.regex.Pattern
	 * @param regex the regular expression to match against
	 * @param topx return a maximum of topx children (Set to 0 for all matching children)
	 * @return ArrayList<MemoNode> children
	 */
	public ArrayList<MemoNode> getChildrenByRegEx(Pattern regex, int topx){
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(
				this.children.getLength());
		for (MemoNode child : getChildren()) {
			if (regex.matcher(child.getStringValue()).matches()) {
				result.add(child);
				if (topx > 0 && result.size() >= topx)
					return result;
			}
		}
		return result;
	}
	/**
	 * Returns all children whose integer value falls in the give range.
	 * (Currently interprets the string value as an integer, will probably still change in the future?)
	 * 
	 * @param lower the lower bound of the range, value is included in the range.
	 * @param upper the upper bound of the range, value is included in the range.
	 * @param topx return a maximum of topx children (Set to 0 for all matching children)
	 * @return ArrayList<MemoNode> children
	 */
	public ArrayList<MemoNode> getChildrenByRange(int lower, int upper, int topx){
		//TODO: store integers differently? Not as String...
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(
				this.children.getLength());
		for (MemoNode child : getChildren()) {
			try {
				int value = Integer.parseInt(child.getStringValue());
				if (value >= lower && value <= upper) {
					result.add(child);
					if (topx > 0 && result.size() >= topx)
						return result;
				}
			} catch (NumberFormatException e) {
			}
		}
		return result;
	}
	/**
	 * Remove this node.(=setting its value to null and removing all arcs) This method will delete
	 * entire subgraphs through setting the provided flag to true. Removing large subgraphs can be
	 * an expensive operation.
	 * 
	 */
	public void delete(){
		MemoNode current = this;
		ArrayList<UUID> todo = new ArrayList<UUID>(20);
		while (current != null){
			UUID[] children = current.children.getNodesIds();
			UUID[] parents = current.parents.getNodesIds();
			current.update((byte[])null);
			if (children.length>0){
				todo.ensureCapacity(todo.size()+children.length);
				todo.addAll(Arrays.asList(children));
				current.children.clear();
			}
			if (parents.length>0){
				current.parents.clear();
			}
			if (todo.size()>0){
				current = new MemoNode(todo.remove(0));
			} else {
				break;
			}
		}
	}
	/**
	 * Convenience method to store a property pattern for this node. This method
	 * will store the given propValue as a child of an intermediate propName node, which 
	 * will be stored as a child to the current node. (current->propName->propValue)
	 * Existing propNames for this node will lead to an update of the value.
	 * 
	 * @see #getPropertyValue(String propName)
	 * @param propName the name of this property, must be unique for this node.
	 * @param propValue the (new) value of this property.
	 * 
	 */
	public MemoNode setPropertyValue(String propName, String propValue){
		if (propName == null) return this;
		if (propValue == null) propValue="";
		
		ArrayList<MemoNode> properties = getChildrenByStringValue(propName, 1);
		switch (properties.size()) {
		case 0:
			MemoNode value = new MemoNode(propValue.getBytes());
			MemoNode property = new MemoNode(propName.getBytes());
			property.addChild(value.getId());
			this.addChild(property.getId());
			break;
		case 1:
			ArrayList<MemoNode> values = properties.get(0).getChildren();
			if (values.size() == 1) {
				values.get(0).update(propValue.getBytes());
				break;
			}
			//explicit no-break
		default:
			System.out
					.println("Error, incorrect properties found, skipping setPropertyValue("
							+ propName + "," + propValue + ")!");
		}
		return this;
	}
	/**
	 * Convenience method to get a property pattern for this node. 
	 * 
	 * @see #setPropertyValue(String propName,String propValue)
	 * @param propName the name of the property to retrieve
	 * 
	 */
	public String getPropertyValue(String propName){
		ArrayList<MemoNode> properties = getChildrenByStringValue(propName, 1);
		if (properties.size() == 1) {
			ArrayList<MemoNode> values = properties.get(0).getChildren();
			if (values.size() != 1)
				System.out.println("Warning, property with multiple values");
			if (values.size() >= 1)
				return values.get(0).getStringValue();
		}
		return "";
	}

	private class StepState{
		
		private boolean matched = false;
		public StepState(boolean matched,String reason,MemoQuery query,MemoNode toCompare){
			//System.out.println(toCompare.getValue()+"/"+query.value+" -> returning: "+matched+" reason:"+reason);
			this.setMatched(matched);
		}
		public boolean isMatched() {
			return matched;
		}
		public void setMatched(boolean matched) {
			this.matched = matched;
		}
	}
	
	private StepState doStep(boolean preamble, MemoQuery query, MemoNode toCompare,
						     ArrayList<MemoNode> results, HashSet<MemoNode> seenNodes, 
						     ArrayList<MemoNode> patterns, int topX, HashMap<String,String> arguments){
		
		MemoNode step = query.node;
		//System.out.println("checking node:" + toCompare.getStringValue() + "/" + query.value + "("+preamble+")");

		if (!query.match(toCompare)) return new StepState(false,"Node doesn't match.",query,toCompare);
		if (seenNodes.contains(toCompare)) return new StepState(true,"Loop/Multipath detected",query,toCompare);
		if (preamble) {
			for (MemoNode pattern : patterns){
				StepState res = doStep(false,MemoQuery.parseQuery(pattern.getChildren().get(0), arguments),
									   toCompare,null,new HashSet<MemoNode>(),null,0,arguments);
				if (res.matched){
					results.add(toCompare);
					return new StepState(true,"Node matches pattern! Added to result, no need to search deeper.",query,toCompare);
				}
			}
		}
		seenNodes.add(toCompare);
		
		ArrayList<MemoNode> nextPats = step.getChildren();
		int toMatchNo = nextPats.size();
		if (toMatchNo == 0) return new StepState(true,"End of pattern",query,toCompare);
		
		ArrayList<MemoNode> children = toCompare.getChildren();		
		if (!preamble && children.size() < toMatchNo) return new StepState(false,"Too little children for pattern",query,toCompare);
		
		ArrayList<MemoQuery> queries = new ArrayList<MemoQuery>(toMatchNo);
		HashSet<MemoQuery> foundQueries = new HashSet<MemoQuery>(toMatchNo);
		for (MemoNode nextPat : nextPats) {
			queries.add(MemoQuery.parseQuery(nextPat, arguments));
		}
		MemoQuery[] queryArray = { new MemoQuery() };
		queryArray = queries.toArray(queryArray);
		Arrays.sort(queryArray);
		
		for (MemoNode child : children) {
			for (MemoQuery iQuery : queryArray) {
				if (foundQueries.contains(iQuery)) continue;
				StepState res = doStep(preamble,iQuery,child,results,seenNodes,patterns,topX, arguments);
				
				if (topX > 0 && results.size() >= topX) return new StepState(true,"TopX results reached, returning!",query,toCompare);
				if (preamble || !res.isMatched()) continue;
				//Return on fully matched pattern
				foundQueries.add(iQuery);
				if (foundQueries.size() == queryArray.length) return new StepState(true,"Pattern fully matched",query,toCompare);
			}
		}
		if (preamble) return new StepState(true,"preamble return.",query,toCompare);
		return new StepState(false,"Pattern didn't match entirely.",query,toCompare);
	}
	/**
	 * Search for nodes according to the give Preamble(s) and Pattern(s). 
	 * (Full documentation is still on the todo list:) )
	 * 
	 * @see "MemoTestServlet.java for an example of searching."
	 */
	public ArrayList<MemoNode> search(ArrayList<MemoNode> preambles,
			ArrayList<MemoNode> patterns, int topx, HashMap<String,String> arguments) {

		ArrayList<MemoNode> result = new ArrayList<MemoNode>(topx>0?Math.min(200,topx):200);
		HashSet<MemoNode> seenNodes = new HashSet<MemoNode>(200);
		
		if (patterns.size() <= 0) {
			System.out.println("Warning, empty algorithm used (no patterns).");
			return result;
		}
		if (preambles.size() <= 0) {
			System.out.println("Warning, empty algorithm used (no preambles).");
			return result;
		}
		
		for (MemoNode preamble : preambles) {
			doStep(true,MemoQuery.parseQuery(preamble.getChildren().get(0),arguments),(MemoNode) this,result,seenNodes,patterns,topx,arguments);
		}
		return result;
	}

	/**
	 * Search for nodes according to the give Preamble(s) and Pattern(s). 
	 * (Full documentation is still on the todo list:) )
	 * 
	 * @see "MemoTestServlet.java for an example of searching."
	 */
	public ArrayList<MemoNode> search(MemoNode algorithm, int topx, HashMap<String,String> arguments) {
		ArrayList<MemoNode> preambles = algorithm.getChildrenByStringValue(
				"PreAmble", -1);
		ArrayList<MemoNode> patterns = algorithm.getChildrenByStringValue("Pattern",
				-1);
		return this.search(preambles, patterns, topx, arguments);
	}

	/**
	 * Search for nodes according to the give Preamble(s) and Pattern(s). 
	 * (Full documentation is still on the todo list:) )
	 * 
	 * @see "MemoTestServlet.java for an example of searching."
	 */
	public ArrayList<MemoNode> search(MemoNode preamble, MemoNode pattern,
			int topx, HashMap<String,String> arguments) {
		ArrayList<MemoNode> preambles = new ArrayList<MemoNode>(1);
		preambles.add(preamble);
		ArrayList<MemoNode> patterns = new ArrayList<MemoNode>(1);
		patterns.add(pattern);
		return this.search(preambles, patterns, topx, arguments);
	}
	/**
	 * Return the subgraph below this node as a JSON object suitable for the chap network
	 * viewer.
	 * 
	 * @see <a href="http://almende.github.com/chap-links-library/network.html">CHAP network viewer</a>
	 * @param depth maximum depth of the subgraph to retrieve. (set to 0 for unlimited =dangerous)
	 */
	public String toJSONString(int depth){
		//TODO: prevent loops
		JSONTuple tuple = new JSONTuple();
		this.toJSON(depth,tuple);
		ObjectNode node = om.createObjectNode();
		node.put("nodes", tuple.getNodes());
		node.put("links", tuple.getLinks());
		return node.toString();
	}

	private void toJSON(int depth,JSONTuple result) {
		if (result.getSeenNodes().contains(this)) return;
		result.getSeenNodes().add(this);
		ObjectNode node = om.createObjectNode();
		node.put("id", this.getId().toString());
		node.put("title", this.getStringValue());
		
		ArrayList<MemoNode> children = this.getChildren();
		if (depth-- > 0) {
			for (MemoNode child : children) {
				child.toJSON(depth,result);
				result.getLinks().add(
						om.createObjectNode().
						put("from", this.getId().toString()).
						put("to",child.getId().toString()));
			}
		} 
		if (depth < 0 && children.size()>0){
			node.put("group", "more");
		}
		result.getNodes().add(node);
	}
	
	public byte[] valueToMsg() throws IOException{
		return this.value.toMsg();
	}
	public byte[] parentsToMsg() throws IOException{
		return this.parents.toMsg();	
	}
	public byte[] childrenToMsg() throws IOException{
		return this.children.toMsg();	
	}
}
class MemoQuery implements Comparable<MemoQuery> {
	public enum Type {
		Equal, Regex, Range, Any
	};

	static HashMap<MemoNode, MemoQuery> queryCache = new HashMap<MemoNode, MemoQuery>();

	MemoNode node = null;
	Type type = Type.Equal;
	String value = "";
	boolean hasArg=false;
	java.util.regex.Pattern regex = null;
	int lower = 0;
	int upper = 0;

	@Override
	public int compareTo(MemoQuery arg0) {
		return this.type.compareTo(arg0.type);
	}

	public static MemoQuery parseQuery(MemoNode step,HashMap<String,String> arguments) {
		if (queryCache.containsKey(step)) {
			MemoQuery cached = queryCache.get(step);
			if (!cached.hasArg){
				return cached;
			};
		}
		MemoQuery result = new MemoQuery();
		result.node = step;
		String query = step.getStringValue();
		if (query.equals("any")) {
			result.type = MemoQuery.Type.Any;
		} else if (query.startsWith("equal;")) {
			result.type = MemoQuery.Type.Equal;
			result.value = query.substring(6);
			if (result.value.startsWith("arg(")){
				result.value=arguments.get(result.value.substring(4,result.value.length()-1));
				//System.out.println("Setting arg value to:"+result.value);
				result.hasArg=true;
			}
		} else if (query.startsWith("regex;")) {
			result.type = MemoQuery.Type.Regex;
			result.regex = Pattern.compile(query.substring(6));
		} else if (query.startsWith("range:")) {
			result.type = MemoQuery.Type.Range;
			String[] parts = query.substring(6).split(";");
			result.lower = Integer.parseInt(parts[0]);
			result.upper = Integer.parseInt(parts[0]);
		} else {
			result.type = MemoQuery.Type.Equal;
			result.value = query;
			if (result.value.startsWith("arg(")){
				result.value=arguments.get(result.value.substring(4,result.value.length()-1));
				result.hasArg=true;
			}
		}
		if (!result.hasArg) queryCache.put(step, result);
		return result;
	}

	public boolean match(MemoNode node) {
		switch (this.type) {
		case Equal:
			//System.out.println("Checking "+node.getId()+":"+node.getStringValue()+" against "+this.value);
			return node.getStringValue().equals(this.value);
		case Regex:
			return this.regex.matcher(node.getStringValue()).matches();
		case Range:
			try {
				int value = Integer.parseInt(node.getStringValue());
				return value >= this.lower && value <= this.upper;
			} catch (Exception e) {
				return false;
			}
		case Any:
			return true;
		default:
			System.out.println("MemoQuery: Unknown enum value???");
			return false;
		}
	}
}

