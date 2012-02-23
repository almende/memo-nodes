package com.chap.memo.memoNodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import com.eaio.uuid.UUID;

/* Node is completely immutable, meaning any change creates a new node
 * This leads to guaranteed consistency of the stored version, it will never change for the 
 * specific node. There can be newer versions of the Node somewhere, it is good to check for 
 * newer versions once in a while (e.g. on any findNode action...)
 *
 * This immutability also holds for changes to the arcs. By allowing multiple instances with the same nodeId
 * the arcs don't need to change is the value is updated, only a (relatively) fast way to find 
 * the newest instance of the node is required. The Node class has a cache of these arcs, which 
 * can be updated by an "unlink"/"relink" cycle.
 * 
 * To make Nodes more usable in real-life cases, a special decorator class has been created:
 * Unode (stands for Updating Node)
 * Both Unode and Node implement the MemoNode interface.
 * 
 * Basically: all functions that accept a Node, also accept a MemoNode. When this MemoNode is of the type
 * Unode, it will automatically update the Node references, which will prevent the need to continuously
 * track the "current" Node. This doesn't help for changes to the node at through the storage, an occasional
 * "find" is still required to keep current.
 */
public final class Node implements Serializable, MemoNode {
	private static final long serialVersionUID = -214365448207420311L;
	public static UUID ROOT = new UUID("00000000-0000-002a-0000-000000000000");

	// private final long[] uuid;
	private final UUID id; // UUID therefore 128bits (2x long)
	private final String value; // int for length, plus X bytes
	private final Date timestamp; // long

	private final UUID[] children; // int for no.children, per child: 2x long
	private final UUID[] parents; // int for no.parents, per parent: 2x long

	public Node getRealNode() {
		return this;
	}

	private Node(UUID id, String value, UUID[] children, UUID[] parents) {
		this.id = id;
		this.value = value;
		this.timestamp = new Date();
		this.children = children;
		this.parents = parents;
		NodeList.store(this);
	}

	private Node(String value, UUID[] children, UUID[] parents) {
		this(new UUID(), value, children, parents);
	}

	private Node(UUID id, String value) {
		this(id, value, new UUID[0], new UUID[0]);
	}

	private Node(String value) {
		this(new UUID(), value);
	}

	private Node(Node prev, UUID[] children, UUID[] parents) {
		this(prev.id, prev.value, children, parents);
	}

	private Node(Node prev, UUID id, String value) {
		this(id, value, prev.children, prev.parents);
	}

	private Node(Node prev, String value) {
		this(prev.id, value, prev.children, prev.parents);
	}

	/*
	 * We can override the equal and hashCode if we like to be able to say any
	 * instance of the node is equal to others. Not sure yet if we want that,
	 * depend on the implementation of getValue.
	 */
	/*
	 * @Override public boolean equals(Object object){ if
	 * (Node.class.isInstance(object)){ Node node = (Node) object; return
	 * this.id.equals(node.id); } return false; }
	 * 
	 * @Override public int hashCode(){ return new Integer(this.id).intValue();
	 * }
	 */
	@Override
	public String toString() {
		return this.getId() + ":" + this.timestamp + ":c="
				+ this.children.length + ",p=" + this.parents.length + ":"
				+ this.getValue();
	}

	/*
	 * create function, static version only (factory, due to immutable nature of
	 * Node)
	 */
	public static Node store(String value) {
		return new Node(value);
	}

	public static Node store(UUID id, String value) {
		Node prev = find(id);
		if (prev != null) {
			return new Node(prev, value);
		}
		return new Node(id, value);
	}

	public static Arc storeAsChild(String value, Node parent) {
		UUID[] parents = { parent.id };
		Node node = new Node(value, new UUID[0], parents);
		return new Arc(parent.addChild(node, false).parent, node);
	}

	public static Arc storeAsParent(String value, Node child) {
		UUID[] children = { child.id };
		Node node = new Node(value, children, new UUID[0]);
		return new Arc(node, child.addParent(node, false).child);
	}

	public static Arc storeAsChild(UUID id, String value, Node parent) {
		UUID[] parents = { parent.id };
		Node node = new Node(id, value, new UUID[0], parents);
		return new Arc(parent.addChild(node, false).parent, node);
	}

	public static Arc storeAsParent(UUID id, String value, Node child) {
		UUID[] children = { child.id };
		Node node = new Node(id, value, children, new UUID[0]);
		return new Arc(node, child.addParent(node, false).child);
	}

	public static Arc storeAsChild(String value, MemoNode parent) {
		UUID[] parents = { parent.getId() };
		Node node = new Node(value, new UUID[0], parents);
		return new Arc(parent.addChild(node, false).parent, node);
	}

	public static Arc storeAsParent(String value, MemoNode child) {
		UUID[] children = { child.getId() };
		Node node = new Node(value, children, new UUID[0]);
		return new Arc(node, child.addParent(node, false).child);
	}

	public static Arc storeAsChild(UUID id, String value, MemoNode parent) {
		UUID[] parents = { parent.getId() };
		Node node = new Node(id, value, new UUID[0], parents);
		return new Arc(parent.addChild(node, false).parent, node);
	}

	public static Arc storeAsParent(UUID id, String value, MemoNode child) {
		UUID[] children = { child.getId() };
		Node node = new Node(id, value, children, new UUID[0]);
		return new Arc(node, child.addParent(node, false).child);
	}

	public Node bulkAddChildren(ArrayList<MemoNode> children) {
		ArrayList<UUID> childids = new ArrayList<UUID>(children.size());
		for (MemoNode node : children) {
			node.addParent(this, false);
			childids.add(node.getId());
		}
		return new Node(this, childids.toArray(new UUID[0]), this.parents);
	}

	public Node bulkAddParents(ArrayList<MemoNode> parents) {
		ArrayList<UUID> parentids = new ArrayList<UUID>(parents.size());
		for (MemoNode node : parents) {
			node.addChild(this, false);
			parentids.add(node.getId());
		}
		return new Node(this, this.children, parentids.toArray(new UUID[0]));
	}

	public Node bulkDelChildren(ArrayList<MemoNode> children) {
		List<UUID> result = Arrays.asList(this.children);
		for (MemoNode node : children) {
			if (result.remove(node.getId()))
				node.delParent(this, false);
		}
		return new Node(this, result.toArray(new UUID[0]), this.parents);
	}

	public Node bulkDelParents(ArrayList<MemoNode> parents) {
		List<UUID> result = Arrays.asList(this.parents);
		for (MemoNode node : parents) {
			if (result.remove(node.getId()))
				node.delChild(this, false);
		}
		return new Node(this, this.children, result.toArray(new UUID[0]));
	}

	/* Update functions, both a static and non-static variant */
	public static Node update(Node prev, String value) {
		return new Node(prev, prev.id, value);
	}

	public Node update(String value) {
		return new Node(this, this.id, value);
	}

	/* Find gets the newest version of this Node */
	public static Node find(UUID id) {
		return NodeList.find(id);
	}

	/* Get the value at a given time */
	public String valueAt(Date timestamp) {
		Node node = NodeList.findBefore(this.id, timestamp);
		return node.getValue();
	}

	/* time-ordered list of Nodes, newest first */
	public ArrayList<Node> history() {
		return NodeList.findAll(this.id);// expensive
	}

	public UUID getId() {
		return id;
	}

	public String getValue() {
		return value;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public Arc addChild(MemoNode child, boolean doOther) {
		return addChild(child.getRealNode(), doOther);
	}

	private Arc addChild(Node child, boolean doOther) {
		// System.out.println("Add child:"+this.value+":"+child.value+"/"+doOther);
		UUID[] children = this.children.clone(); // copy which may be re-ordered
													// for search.
		Arrays.sort(children);
		if (Arrays.binarySearch(children, child.id) >= 0) {
			// System.out.println("Double try to add child");
			return new Arc(this, child);
		}
		UUID[] newchildren = new UUID[this.children.length + 1];
		newchildren[0] = child.id;
		System.arraycopy(this.children, 0, newchildren, 1, this.children.length);

		Arc other = new Arc();
		if (doOther)
			other = child.addParent(this, false);
		return new Arc(new Node(this, newchildren, this.parents), other.child);
	}

	public Arc addParent(MemoNode parent, boolean doOther) {
		return addParent(parent.getRealNode(), doOther);
	}

	private Arc addParent(Node parent, boolean doOther) {
		// System.out.println("Add Parent:"+this.value+":"+parent.value+"/"+doOther);
		UUID[] parents = this.parents.clone();
		Arrays.sort(parents);
		if (Arrays.binarySearch(parents, parent.id) >= 0) {
			// System.out.println("Double try to add parent");
			return new Arc(parent, this);
		}
		UUID[] newparents = new UUID[this.parents.length + 1];
		newparents[0] = parent.id;
		System.arraycopy(this.parents, 0, newparents, 1, this.parents.length);

		Arc other = new Arc();
		if (doOther)
			other = parent.addChild(this, false);
		// System.out.println(this.parents.length);
		return new Arc(other.parent, new Node(this, this.children, newparents));
	}

	public Arc addParent(MemoNode node) {
		return this.addParent(node.getRealNode(), true);
	}

	public Arc addChild(MemoNode node) {
		return this.addChild(node.getRealNode(), true);
	}

	public Node delChild(MemoNode child) {
		return this.delChild(child.getRealNode(), true);
	}

	public Node delChild(MemoNode child, boolean doOther) {
		ArrayList<UUID> result = new ArrayList<UUID>(this.children.length);
		for (UUID uuid : this.children) {
			if (!uuid.equals(child.getId()))
				result.add(uuid);
		}
		UUID[] newchildren = result.toArray(new UUID[0]);
		if (doOther)
			child.delParent(this, false);
		return new Node(this, newchildren, this.parents);
	}

	public Node delParent(MemoNode parent) {
		return this.delParent(parent.getRealNode(), true);
	}

	public Node delParent(MemoNode parent, boolean doOther) {
		ArrayList<UUID> result = new ArrayList<UUID>(this.parents.length);
		for (UUID uuid : this.parents) {
			if (!uuid.equals(parent.getId()))
				result.add(uuid);
		}
		UUID[] newparents = result.toArray(new UUID[0]);
		if (doOther)
			parent.delChild(this, false);
		return new Node(this, this.children, newparents);
	}

	public ArrayList<MemoNode> getChildren() {
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(
				this.children.length);
		List<UUID> children = Arrays.asList(this.children);
		for (UUID id : children) {
			result.add(Node.find(id));
		}
		return result;
	}

	public ArrayList<MemoNode> getParents() {
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(
				this.parents.length);
		List<UUID> parents = Arrays.asList(this.parents);
		for (UUID id : parents) {
			result.add(Node.find(id));
		}
		return result;
	}

	public ArrayList<MemoNode> getChildrenByValue(String value, int topx) {
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(
				this.children.length);
		for (MemoNode child : getChildren()) {
			if (child.getValue().equals(value)) {
				result.add(child);
				if (topx > 0 && result.size() >= topx)
					return result;
			}
		}
		return result;
	}

	public ArrayList<MemoNode> getChildrenByRegEx(Pattern regex, int topx) {
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(
				this.children.length);
		for (MemoNode child : getChildren()) {
			if (regex.matcher(child.getValue()).matches()) {
				result.add(child);
				if (topx > 0 && result.size() >= topx)
					return result;
			}
		}
		return result;
	}

	public ArrayList<MemoNode> getChildrenByRange(int lower, int upper, int topx) {
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(
				this.children.length);
		for (MemoNode child : getChildren()) {
			try {
				int value = Integer.parseInt(child.getValue());
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

	public String getPropertyValue(String propName) {
		ArrayList<MemoNode> properties = getChildrenByValue(propName, 1);
		if (properties.size() == 1) {
			ArrayList<MemoNode> values = properties.get(0).getChildren();
			if (values.size() != 1)
				System.out.println("Warning, property with multiple values");
			if (values.size() >= 1)
				return values.get(0).getValue();
		}
		return "";
	}

	public MemoNode setPropertyValue(String propName, String propValue) {
		ArrayList<MemoNode> properties = getChildrenByValue(propName, 1);
		switch (properties.size()) {
		case 0:
			MemoNode value = Node.store(propValue);
			MemoNode property = Node.storeAsParent(propName, value).parent;
			return this.addChild(property).parent;
		case 1:
			ArrayList<MemoNode> values = properties.get(0).getChildren();
			if (values.size() == 1) {
				values.get(0).update(propValue);
				return this;
			}
			// explicit no-break
		default:
			System.out
					.println("Error, incorrect properties found, skipping setPropertyValue("
							+ propName + "," + propValue + ")!");
			return this;
		}
	}

	private ArrayList<MemoNode> doPatternStep(MemoNode step, MemoNode toCompare) {
		ArrayList<MemoNode> result = new ArrayList<MemoNode>();
		ArrayList<MemoNode> nextPats = step.getChildren();
		int toMatchNo = nextPats.size();
		if (toMatchNo == 0) {
			// End of pattern, always correct.
			result.add(toCompare);
			return result;
		}
		ArrayList<MemoNode> children = toCompare.getChildren();
		if (children.size() < toMatchNo) {
			// Will never match, early out
			return result;
		}

		ArrayList<MemoQuery> queries = new ArrayList<MemoQuery>(toMatchNo);
		for (MemoNode nextPat : nextPats) {
			queries.add(MemoQuery.parseQuery(nextPat));
		}
		MemoQuery[] queryArray = { new MemoQuery() };
		queryArray = queries.toArray(queryArray);
		Arrays.sort(queryArray);

		// int count=0;
		for (MemoNode child : children) {
			// count++;
			boolean found = false;
			// Compare each child, at least one pattern path needs to match
			for (MemoQuery query : queryArray) {
				if (query.match(child)) {
					ArrayList<MemoNode> subRes = doPatternStep(query.node,
							child);
					if (subRes.size() > 0) {
						result.addAll(subRes);
						toMatchNo--;
						found = true;
					}
				}
				if (found)
					break;
			}
			if (toMatchNo <= 0) {
				// Allready found, early out.
				break;
			}
		}
		if (toMatchNo > 0) {
			return new ArrayList<MemoNode>(); // Not all patterns were matched!
		}
		if (result.size() > 0) {
			result.add(0, toCompare);
		}
		return result;
	}

	private ArrayList<MemoResult> doPreAmbleStep(MemoNode step,
			MemoNode toCompare, ArrayList<MemoNode> patterns,
			ArrayList<MemoNode> path, ArrayList<MemoResult> allResults, int topx) {
		ArrayList<MemoResult> result = new ArrayList<MemoResult>();
		if (path.contains(toCompare)) {
			// Loop detected!
			return result;
		}
		for (MemoResult earlier : allResults) {
			if (earlier.match.contains(toCompare)) {
				// multipath route detected
				return result;
			}
		}
		// First check if current Node matches any of the patterns, if it does,
		// don't go deeper, just return the match.
		for (MemoNode pattern : patterns) {
			ArrayList<MemoNode> ret = doPatternStep(pattern.getRealNode(),
					toCompare.getRealNode());
			if (ret.size() > 0) {
				MemoNode patternRoot = ret.get(1);
				ret.addAll(0, path);
				result.add(new MemoResult(patternRoot, ret));
				allResults.addAll(result);
				return result;
			}
		}
		path.add(toCompare);
		ArrayList<MemoNode> nextPats = step.getChildren();
		int toMatchNo = nextPats.size();
		if (toMatchNo == 0) {
			// End of pattern, always correct.
		}
		ArrayList<MemoNode> children = toCompare.getChildren();
		ArrayList<MemoQuery> queries = new ArrayList<MemoQuery>(toMatchNo);
		for (MemoNode nextPat : nextPats) {
			queries.add(MemoQuery.parseQuery(nextPat));
		}
		MemoQuery[] queryArray = { new MemoQuery() };
		queryArray = queries.toArray(queryArray);
		Arrays.sort(queryArray);

		for (MemoNode child : children) {
			boolean found = false;
			// Compare each child, at least one pattern path needs to match
			for (MemoQuery query : queryArray) {
				if (query.match(child)) {
					ArrayList<MemoResult> subRes = doPreAmbleStep(query.node,
							child, patterns, path, allResults, topx);
					if (subRes.size() > 0) {
						result.addAll(subRes);
						found = true;
					}
				}
				if (found)
					break;
			}
			if (topx > 0 && allResults.size() >= topx)
				break;
		}
		path.remove(toCompare);
		return result;
	}

	@SuppressWarnings("unchecked")
	public ArrayList<MemoResult> search(ArrayList<MemoNode> preambles,
			ArrayList<MemoNode> patterns, int topx) {
		ArrayList<MemoResult> result = null;

		if (topx <= 0) {
			result = new ArrayList<MemoResult>(Math.min(this.children.length,
					200));
		} else {
			result = new ArrayList<MemoResult>(Math.min(this.children.length,
					topx));
		}
		if (patterns.size() <= 0) {
			System.out.println("Warning, empty algorithm used.");
			return result;
		}
		for (MemoNode preamble : preambles) {
			result.addAll(doPreAmbleStep(preamble, (MemoNode) this, patterns,
					new ArrayList<MemoNode>(),
					(ArrayList<MemoResult>) result.clone(), topx));
		}
		return result;
	}

	public ArrayList<MemoResult> search(MemoNode algorithm, int topx) {
		ArrayList<MemoNode> preambles = algorithm.getChildrenByValue(
				"PreAmble", -1);
		ArrayList<MemoNode> patterns = algorithm.getChildrenByValue("Pattern",
				-1);
		return this.search(preambles, patterns, topx);
	}

	public ArrayList<MemoResult> search(MemoNode preamble, MemoNode pattern,
			int topx) {
		ArrayList<MemoNode> preambles = new ArrayList<MemoNode>(1);
		preambles.add(preamble.getRealNode());
		ArrayList<MemoNode> patterns = new ArrayList<MemoNode>(1);
		patterns.add(pattern.getRealNode());
		return this.search(preambles, patterns, topx);
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
	java.util.regex.Pattern regex = null;
	int lower = 0;
	int upper = 0;

	@Override
	public int compareTo(MemoQuery arg0) {
		return this.type.compareTo(arg0.type);
	}

	public static MemoQuery parseQuery(MemoNode step) {
		if (queryCache.containsKey(step)) {
			return queryCache.get(step);
		}
		MemoQuery result = new MemoQuery();
		result.node = step;
		String query = step.getValue();
		if (query.equals("any")) {
			result.type = MemoQuery.Type.Any;
		} else if (query.startsWith("equal;")) {
			result.type = MemoQuery.Type.Equal;
			result.value = query.substring(6);
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
		}
		queryCache.put(step, result);
		return result;
	}

	public boolean match(MemoNode node) {
		switch (this.type) {
		case Equal:
			return node.getValue().equals(this.value);
		case Regex:
			return this.regex.matcher(node.getValue()).matches();
		case Range:
			try {
				int value = Integer.parseInt(node.getValue());
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
