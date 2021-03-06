package com.chap.memo.memoNodes.servlet;

import static com.google.appengine.api.taskqueue.TaskOptions.Builder.withUrl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.chap.memo.memoNodes.MemoNode;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;

@SuppressWarnings("serial")
public class MemoTestServlet extends HttpServlet {
	boolean debug = false;
	static Queue queue = QueueFactory.getDefaultQueue();
	long total=0;
	
	private void log(HttpServletResponse resp, boolean ok, String shortMsg) {
		log(resp, ok, shortMsg, null);
	}

	private void log(HttpServletResponse resp, boolean ok, String shortMsg,
			String msg) {
		try {
			resp.getWriter().println(
					(ok ? "---" : "!E!") + "  -   " + shortMsg
							+ (!debug || msg == null ? "" : msg));
			resp.flushBuffer();
		} catch (IOException e) {
		}
		if (ok){
			System.out.println(shortMsg + (msg == null ? "" : msg));
		} else {
			System.err.println(shortMsg + (msg == null ? "" : msg));
		}
	}

	private boolean test(String compare, String result) {
		return compare.equals(result);
	}
	public void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		doGet(req, resp);
	}
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {

		try {
			debug = false;
			resp.setContentType("text/plain");
			
			System.out.println("Starting!");
			
			String cleanDBParm = req.getParameter("cleanDB");
			if (cleanDBParm != null) {
				MemoNode.emptyDB();
				log(resp, true, "\nDatabase cleared!");

				if (cleanDBParm.equals("only"))
					return;
			}
			String debugStr = req.getParameter("debug");
			if (debugStr != null && !debugStr.equals("null")) {
				debug = true;
			}
			if (debug)
				System.out.println("Outputting debug info:" + debugStr);

			int nofNodes = 10000;
			String sNofNodes = req.getParameter("nofNodes");
			if (sNofNodes != null) {
				try {
					nofNodes = Integer.parseInt(sNofNodes);
				} catch (Exception e) {
					System.out.println("couldn't parse nofNodes=" + sNofNodes);
				}
			}
			int nofArcs = 10000;
			String snofArcs = req.getParameter("nofArcs");
			if (snofArcs != null) {
				try {
					nofArcs = Integer.parseInt(snofArcs);
				} catch (Exception e) {
					System.out.println("couldn't parse nofArcs=" + snofArcs);
				}
			}
			
			String nofParallel = req.getParameter("parallel");
			if (nofParallel == null) {
				nofParallel = "1";
			}
			if (Integer.parseInt(nofParallel)>1){
				log(resp, true, "Going parallel:"+nofParallel,"");
				
				TaskOptions to = withUrl("/test")
						.param("parallel","1")
						.param("nofNodes", Integer.toString(nofNodes))
						.param("nofArcs", Integer.toString(nofArcs))
						.countdownMillis(1);
				
				int np = Integer.parseInt(nofParallel);
				while (np-- > 0){
					queue.add(to);
				}
			}
			log(resp, true, "\nStarting single node tests:");
			
			MemoNode rootNode = MemoNode.getRootNode();
			log(resp,true,"Root node found:"+rootNode.getId().toString());
			
			// Add single node
			String title = "First node";
			MemoNode first = new MemoNode(title);
			log(resp, test(first.getStringValue(), title), "Write/Read cycle",
					":" + first.getStringValue());

			first = new MemoNode(first.getId());
			log(resp, test(first.getStringValue(), title), "Read from readBus",
					": " + first.getId() + "/" + first.getStringValue());

			MemoNode.flushDB();
			log(resp, test(first.getStringValue(), title), "Read after flush",
					": " + first.getId() + "/" + first.getStringValue());

			first = new MemoNode(first.getId());
			log(resp, test(first.getStringValue(), title),
					"Read from readBus after flush", ": " + first.getId() + "/"
							+ first.getStringValue());

			log(resp, true, "\nUpdating node value:");
			title = "Updated node value";
			first.update(title);
			log(resp, test(first.getStringValue(), title), "Write/Read cycle",
					": " + first.getId() + "/" + first.getStringValue());

			first = new MemoNode(first.getId());
			log(resp, test(first.getStringValue(), title), "Read from readBus",
					": " + first.getId() + "/" + first.getStringValue());

			log(resp, true, "\nAgain updating node value:");
			title = "New updated node value";
			first.update(title);
			log(resp, test(first.getStringValue(), title), "Write/Read cycle",
					": " + first.getId() + "/" + first.getStringValue());

			first = new MemoNode(first.getId());
			log(resp, test(first.getStringValue(), title), "Read from readBus",
					": " + first.getId() + "/" + first.getStringValue());

			MemoNode.flushDB();
			log(resp, test(first.getStringValue(), title), "Read after flush",
					": " + first.getId() + "/" + first.getStringValue());

			first = new MemoNode(first.getId());
			log(resp, test(first.getStringValue(), title),
					"Read from readBus after flush", ": " + first.getId() + "/"
							+ first.getStringValue());

			log(resp, true, "\nStoring a null value into node:");
			first.update((byte[]) null);
			log(resp, test(first.getStringValue(), ""),
					"Reading string leads to empty string",
					": " + first.getId() + "/" + first.getStringValue());
			log(resp, first.getValue().length == 0,
					"Reading value leads to zero length byte array", ": "
							+ first.getId() + "/" + first.getValue());

			log(resp, true, "\nMulti nodal tests: (Adding one child)");

			title = "First node";
			first.update(title);

			String secondTitle = "Second node";
			MemoNode second = new MemoNode(secondTitle);

			first.addChild(second);
			log(resp, (first.getChildren().size() == 1),
					"Parent has one child", ":" + first.getId() + ":"
							+ first.getChildren().size());
			log(resp, (first.getParents().size() == 0),
					"Parent has no parents", ":" + first.getId() + ":"
							+ first.getParents().size());
			log(resp, (second.getParents().size() == 1),
					"Child has one parent", ":" + second.getId() + ":"
							+ second.getParents().size());
			log(resp,
					test(first.getChildren().get(0).getStringValue(),
							secondTitle), "Child found", ": " + first.getId()
							+ "|" + first.getChildren().get(0).getId() + "/"
							+ first.getChildren().get(0).getStringValue());

			MemoNode.flushDB();
			System.out.println("Running flush!");
			second = new MemoNode(first.getId()).getChildren().get(0);
			System.out.println("Got second child");
			log(resp, test(second.getStringValue(), secondTitle),
					"Child found after flush", ": " + second.getId() + "/"
							+ second.getStringValue());

			secondTitle = "Second node (new Value)";
			second.update(secondTitle);
			log(resp,
					test(first.getChildren().get(0).getStringValue(),
							secondTitle), "Updated child found",
					": " + first.getId() + "|"
							+ first.getChildren().get(0).getId() + "/"
							+ first.getChildren().get(0).getStringValue());

			log(resp, true, "\nMulti nodal tests: (Adding a second child)");
			String thirdTitle = "Third Node";
			MemoNode third = new MemoNode(thirdTitle);

			first.addChild(third);
			log(resp, (first.getChildren().size() == 2),
					"Parent has two children", ":" + first.getId() + ":"
							+ first.getChildren().size());
			log(resp, (first.getParents().size() == 0),
					"Parent has no parents", ":" + first.getId() + ":"
							+ first.getParents().size());
			log(resp, (second.getParents().size() == 1),
					"First child has one parent", ":" + second.getId() + ":"
							+ second.getParents().size());
			log(resp, (third.getParents().size() == 1),
					"Second child has one parent", ":" + third.getId() + ":"
							+ third.getParents().size());
			log(resp, true, "First child found", ": " + first.getId() + "|"
					+ first.getChildren().get(0).getId() + "/"
					+ first.getChildren().get(0).getStringValue());
			log(resp, true, "Second child found", ": " + first.getId() + "|"
					+ first.getChildren().get(1).getId() + "/"
					+ first.getChildren().get(1).getStringValue());

			log(resp, test(third.getParents().get(0).getStringValue(), title),
					"Parent found", ": "
							+ third.getParents().get(0).getStringValue());

			log(resp, true,
					"\nRemove child and re-adding it (within same Arcop shard):");
			first.delChild(third);
			log(resp, (first.getChildren().size() == 1),
					"Parent has one child", ":" + first.getId() + ":"
							+ first.getChildren().size());
			log(resp, (first.getParents().size() == 0),
					"Parent has no parents", ":" + first.getId() + ":"
							+ first.getParents().size());
			log(resp, (second.getParents().size() == 1),
					"First child has one parent", ":" + second.getId() + ":"
							+ second.getParents().size());
			log(resp, (third.getParents().size() == 0),
					"Second child has no parent", ":" + third.getId() + ":"
							+ third.getParents().size());
			log(resp, true, "Child found", ": " + first.getId() + "|"
					+ first.getChildren().get(0).getId() + "/"
					+ first.getChildren().get(0).getStringValue());

			first.addChild(third);
			log(resp, (first.getChildren().size() == 2),
					"Parent has two children", ":" + first.getId() + ":"
							+ first.getChildren().size());
			log(resp, (first.getParents().size() == 0),
					"Parent has no parents", ":" + first.getId() + ":"
							+ first.getParents().size());
			log(resp, (second.getParents().size() == 1),
					"First child has one parent", ":" + second.getId() + ":"
							+ second.getParents().size());
			log(resp, (third.getParents().size() == 1),
					"Second child has one parent", ":" + third.getId() + ":"
							+ third.getParents().size());
			log(resp, true, "First child found", ": " + first.getId() + "|"
					+ first.getChildren().get(0).getId() + "/"
					+ first.getChildren().get(0).getStringValue());
			log(resp, true, "Second child found", ": " + first.getId() + "|"
					+ first.getChildren().get(1).getId() + "/"
					+ first.getChildren().get(1).getStringValue());

			MemoNode.flushDB();
			log(resp, true,
					"\nRemove child and re-adding it (in multiple Arcop shards):");
			first.delChild(second);
			log(resp, (first.getChildren().size() == 1),
					"Parent has one child", ":" + first.getId() + ":"
							+ first.getChildren().size());
			log(resp, (first.getParents().size() == 0),
					"Parent has no parents", ":" + first.getId() + ":"
							+ first.getParents().size());
			log(resp, (second.getParents().size() == 0),
					"First child has no parent", ":" + second.getId() + ":"
							+ second.getParents().size());
			log(resp, (third.getParents().size() == 1),
					"Second child has one parent", ":" + third.getId() + ":"
							+ third.getParents().size());
			log(resp, true, "Child found", ": " + first.getId() + "|"
					+ first.getChildren().get(0).getId() + "/"
					+ first.getChildren().get(0).getStringValue());

			first.addChild(second);
			log(resp, (first.getChildren().size() == 2),
					"Parent has two children", ":" + first.getId() + ":"
							+ first.getChildren().size());
			log(resp, (first.getParents().size() == 0),
					"Parent has no parents", ":" + first.getId() + ":"
							+ first.getParents().size());
			log(resp, (second.getParents().size() == 1),
					"First child has one parent", ":" + second.getId() + ":"
							+ second.getParents().size());
			log(resp, (third.getParents().size() == 1),
					"Second child has one parent", ":" + third.getId() + ":"
							+ third.getParents().size());
			log(resp, true, "First child found", ": " + first.getId() + "|"
					+ first.getChildren().get(0).getId() + "/"
					+ first.getChildren().get(0).getStringValue());
			log(resp, true, "Second child found", ": " + first.getId() + "|"
					+ first.getChildren().get(1).getId() + "/"
					+ first.getChildren().get(1).getStringValue());

			log(resp, true, "\nMerge shards test, compacting DB:");
			MemoNode.compactDB();
			
			log(resp, (first.getChildren().size() == 2),
					"Parent has two children", ":" + first.getId() + ":"
							+ first.getChildren().size());
			log(resp, (first.getParents().size() == 0),
					"Parent has no parents", ":" + first.getId() + ":"
							+ first.getParents().size());
			log(resp, (second.getParents().size() == 1),
					"First child has one parent", ":" + second.getId() + ":"
							+ second.getParents().size());
			log(resp, (third.getParents().size() == 1),
					"Second child has one parent", ":" + third.getId() + ":"
							+ third.getParents().size());
			log(resp, true, "First child found", ": " + first.getId() + "|"
					+ first.getChildren().get(0).getId() + "/"
					+ first.getChildren().get(0).getStringValue());
			log(resp, true, "Second child found", ": " + first.getId() + "|"
					+ first.getChildren().get(1).getId() + "/"
					+ first.getChildren().get(1).getStringValue());

			log(resp, true, "\nDrop History test.");
			MemoNode.flushDB();
			MemoNode.dropHistory();
			MemoNode.flushDB();
			
			log(resp, (first.getChildren().size() == 2),
					"Parent has two children", ":" + first.getId() + ":"
							+ first.getChildren().size());
			log(resp, (first.getParents().size() == 0),
					"Parent has no parents", ":" + first.getId() + ":"
							+ first.getParents().size());
			log(resp, (second.getParents().size() == 1),
					"First child has one parent", ":" + second.getId() + ":"
							+ second.getParents().size());
			log(resp, (third.getParents().size() == 1),
					"Second child has one parent", ":" + third.getId() + ":"
							+ third.getParents().size());
			log(resp, true, "First child found", ": " + first.getId() + "|"
					+ first.getChildren().get(0).getId() + "/"
					+ first.getChildren().get(0).getStringValue());
			log(resp, true, "Second child found", ": " + first.getId() + "|"
					+ first.getChildren().get(1).getId() + "/"
					+ first.getChildren().get(1).getStringValue());
			
			log(resp, true, "\nTesting Properties:");
			first.setPropertyValue("test", "hello");
			log(resp, test(first.getPropertyValue("test"), "hello"),
					"Property write/read cycle",
					":" + first.getPropertyValue("test"));
			first.setPropertyValue("test", "hello2");
			log(resp, test(first.getPropertyValue("test"), "hello2"),
					"Property new value write/read cycle",
					":" + first.getPropertyValue("test"));
			log(resp, first.getChildrenByStringValue("test", -1).size() == 1,
					"Node has only one property", ":"
							+ first.getChildrenByStringValue("test", -1).size());

			MemoNode newPropNode = new MemoNode("Test");
			newPropNode.setPropertyValue("test", "hello");
			log(resp, test(newPropNode.getPropertyValue("test"), "hello"),
					"Property write/read cycle",
					":" + newPropNode.getPropertyValue("test"));
			newPropNode.setPropertyValue("test", "hello2");
			log(resp, test(newPropNode.getPropertyValue("test"), "hello2"),
					"Property new value write/read cycle",
					":" + newPropNode.getPropertyValue("test"));
			log(resp,
					newPropNode.getChildrenByStringValue("test", -1).size() == 1,
					"Node has only one property", ":"
							+ newPropNode.getChildrenByStringValue("test", -1)
									.size());

			newPropNode = new MemoNode("Test2");
			newPropNode.setPropertyValue("test", "hello");
			log(resp, test(newPropNode.getPropertyValue("test"), "hello"),
					"Property write/read cycle",
					":" + newPropNode.getPropertyValue("test"));
			newPropNode.setPropertyValue("test2", "hello");
			log(resp, test(newPropNode.getPropertyValue("test2"), "hello"),
					"Property write/read cycle",
					":" + newPropNode.getPropertyValue("test"));
			newPropNode.setPropertyValue("test", "hello2");
			log(resp, test(newPropNode.getPropertyValue("test"), "hello2"),
					"Property new value write/read cycle",
					":" + newPropNode.getPropertyValue("test"));
			log(resp, newPropNode.getChildren().size() == 2,
					"Node has two properties", ":"
							+ newPropNode.getChildren().size());
			
			log(resp, true, "\nTrying to recursively delete nodes:");
			first.delete();
			log(resp, first.getChildren().size() == 0, "First has no children",
					":" + first.getChildren().size());
			log(resp, second.getChildren().size() == 0,
					"Second has no children", ":" + first.getChildren().size());
			log(resp, third.getChildren().size() == 0, "Third has no children",
					":" + first.getChildren().size());
			log(resp, third.getParents().size() == 0, "Third has no parents",
					":" + first.getParents().size());

			MemoNode node;

			MemoNode.compactDB();

			long start = System.currentTimeMillis();
			log(resp, true, "\nPerformance test: Depth (" + nofNodes + ")");

			node = new MemoNode("start");
			for (int i = 0; i < nofNodes; i++) {
				MemoNode newNode = new MemoNode(new Integer(i).toString());
				node.addParent(newNode.getId());
				node = newNode;
			}
			long time = System.currentTimeMillis();
			log(resp, true,
					"Storing done in: " + (time - start)
							+ " ms");

			MemoNode startNode = node;

			int count = 0;
			while (node != null) {
				List<MemoNode> children = node.getChildren();
				if (children.isEmpty()) {
					System.out.println(node.getId() + ":"
							+ node.getStringValue() + " has no children!");
					break;
				}
				count++;
				if (children.get(0) == null) {
					System.out.println(node.getId() + ":"
							+ node.getStringValue()
							+ " has NUll node as a child!");
				}
				node = children.get(0);
			}
			log(resp, (count == nofNodes), count + " children counted in:"
					+ (System.currentTimeMillis() - time) + " ms");

			MemoNode.flushDB();//To get a clean read setup!
			time = System.currentTimeMillis();
			startNode.delete();
			log(resp, true, " Nodes deleted again in:"
					+ (System.currentTimeMillis() - time) + " ms");

			
			time = System.currentTimeMillis();
			MemoNode.flushDB();
			log(resp, true,
					" Db flushed:" + (System.currentTimeMillis() - time)
							+ " ms");
			time = System.currentTimeMillis();
			MemoNode.compactDB();
			log(resp, true,
					" Db compacted:" + (System.currentTimeMillis() - time)
							+ " ms");
			
			
			long arcStart = System.currentTimeMillis();
			log(resp, true, "\nPerformance test: Breadth (" + nofArcs + ")");

			MemoNode AstartNode = new MemoNode("start");
			for (int i = 0; i < nofArcs; i++) {
				MemoNode newNode = new MemoNode(new Integer(i).toString());
				AstartNode.addChild(newNode);
			}
			MemoNode.flushDB();//To get a clean read setup!
			long atime = System.currentTimeMillis();
			log(resp, true, "Storing done in: " + (atime - arcStart) + " ms");

			int acount = 0;
			Iterator<MemoNode> iter = AstartNode.getChildren().iterator();
			while (iter.hasNext()) {
				node = iter.next();
				// String value = node.getStringValue();
				acount++;
			}
			log(resp, (acount == nofArcs), acount + " children counted in:"
					+ (System.currentTimeMillis() - atime) + " ms");

			atime = System.currentTimeMillis();
			AstartNode.delete();
			log(resp, true,
					" Nodes deleted again in:"
							+ (System.currentTimeMillis() - atime) + " ms");

			time = System.currentTimeMillis();

			MemoNode.flushDB();

			log(resp, true,
					" Db flushed:" + (System.currentTimeMillis() - time)
							+ " ms");
			start = System.currentTimeMillis();

			log(resp, true, "\nPattern search test:");

			/*
			 * _ start \ / / \ PreAmble -- 1 -- 3 -- any >--1 2 Pattern -- 5 --
			 * * (*= argument (using 8 and 7 below) | / \ / \ (two 5's should be
			 * matched in the diagram to the left) |/ 3 4 3 | \ / \ | 5 5 5 6 |
			 * | | / \ 8 7 8 5 7 | 8
			 */
			startNode = new MemoNode("start");
			log(resp, true, "Startnode:" + startNode.getId(), "");
			MemoNode one = new MemoNode("One");
			startNode.addChild(one);

			MemoNode two = new MemoNode("Two");
			startNode.addChild(two);

			MemoNode three1 = new MemoNode("Three");
			one.addChild(three1);
			three1.addChild(one); // Loop

			MemoNode three2 = new MemoNode("Three");
			two.addChild(three2.getId());
			three2.addParent(one);// multipath

			MemoNode four = new MemoNode("Four");
			two.addChild(four);

			MemoNode six = new MemoNode("Six");
			three1.addChild(six);

			MemoNode five1 = new MemoNode("Five");
			six.addChild(five1);

			MemoNode eight1 = new MemoNode("Eight");
			five1.addChild(eight1);

			MemoNode seven1 = new MemoNode("Seven");
			six.addChild(seven1);

			MemoNode five2 = new MemoNode("Five");
			three2.addChild(five2);

			MemoNode eight2 = new MemoNode("Eight");
			five2.addChild(eight2);

			MemoNode five3 = new MemoNode("Five");
			three2.addChild(five3);
			four.addChild(five3);

			MemoNode seven2 = new MemoNode("Seven");
			five3.addChild(seven2);

			MemoNode five4 = new MemoNode("Five");
			four.addChild(five4);

			MemoNode eight3 = new MemoNode("Eight");
			five4.addChild(eight3);

			MemoNode algorithm = new MemoNode("algo");
			MemoNode pattern = new MemoNode("Pattern");
			algorithm.addChild(pattern);

			MemoNode patFive = new MemoNode("equal;Five");
			pattern.addChild(patFive);

			MemoNode patEight = new MemoNode("equal;arg(Number)");
			patFive.addChild(patEight);

			MemoNode preAmble = new MemoNode("PreAmble");
			algorithm.addChild(preAmble);

			MemoNode PreStart = new MemoNode("equal;start");
			preAmble.addChild(PreStart);

			MemoNode PreOne = new MemoNode("equal;One");
			PreStart.addChild(PreOne);

			MemoNode PreTwo = new MemoNode("equal;Three");
			PreOne.addChild(PreTwo);

			MemoNode preAny = new MemoNode("any");
			PreTwo.addChild(preAny);

			preAny.addChild(preAny); // Spannend:)

			time = System.currentTimeMillis();
			log(resp,
					true,
					"Pattern stored in " + (time - start)
							+ " ms  ->" + preAmble.getId() + " : "
							+ pattern.getId());
			start = time;

			HashMap<String, String> arguments = new HashMap<String, String>(2);
			arguments.put("Number", "Eight");
			List<MemoNode> result = startNode.search(algorithm, -1,
					arguments);
			if (debug) {
				for (MemoNode res : result) {
					log(resp, true, "Found 1: " + res.getStringValue() + "/"
							+ res.getId());
				}
			}
			time = System.currentTimeMillis();
			log(resp, result.size() == 2, "Search 1 done in "
					+ (time - start) + " ms");
			start = time;

			arguments.put("Number", "Seven");
			result = startNode.search(algorithm, 2, arguments); // topx = 2
			if (debug) {
				for (MemoNode res : result) {
					log(resp, true, "Found 2: " + res.getStringValue() + "/"
							+ res.getId());
				}
			}
			time = System.currentTimeMillis();
			log(resp, result.size() == 1, "Search 2 done in "
					+ (time - start) + " ms");

			 one.delete();
			result = startNode.search(algorithm, 2, arguments); // topx = 2
			if (debug) {
				for (MemoNode res : result) {
					log(resp, true, "Found 3: " + res.getStringValue() + "/"
							+ res.getId());
				}
			}
			time = System.currentTimeMillis();
			log(resp, result.size() == 0, "Search 3 done in "
					+ (time - start) + " ms");

			log(resp, startNode.getChildren().size() == 1,
					"StartNode has only one child left", ":"
							+ startNode.getChildren().size());
			for (MemoNode child : startNode.getChildren()) {
				log(resp,
						true,
						"child: " + child.getId() + "/"
								+ child.getStringValue());
			}
			
			total = 0;
			String searchText = "child 1";
			int num = 10;
			int props = 0;
			int depth = 2;
			if (req.getParameter("num") != null) {
				num = Integer.valueOf(req.getParameter("num"));
			}
			if (req.getParameter("props") != null) {
				props = Integer.valueOf(req.getParameter("props"));
			}
			if (req.getParameter("depth") != null) {
				depth = Integer.valueOf(req.getParameter("depth"));
			}
			System.out.println("depth=" + depth);

			log(resp,true,"\nTest broad tree: "+num+"@"+depth+" with:"+props+" textproperties per node.");
			
			// create nested nodes
			start = System.currentTimeMillis();
			long end = 0;
			MemoNode root = MemoNode.getRootNode();
			total++;
			resp.getWriter().println("root id=" + root.getId().toString());
			addChildren(root, num, depth, props);
			end = System.currentTimeMillis();
			log(resp,true,"Created " + total + " nodes" +
					" (" + (end - start) + "ms)");

			// force storing the nodes
			start = System.currentTimeMillis();
			MemoNode.flushDB();
			end = System.currentTimeMillis();
			log(resp,true,"flushed database " +
					" (" + (end - start) + "ms)");

			// create a search structure
			MemoNode preambleStart = new MemoNode("start");
			MemoNode preamble = new MemoNode("any");
			preambleStart.addChild(preamble);
			preamble.addChild(preamble);
			
			pattern = new MemoNode("pattern");
			MemoNode pattern1 = new MemoNode("equal;" + searchText);
			pattern.addChild(pattern1);
			HashMap<String, String> args = new HashMap<String, String>();
			
			// search for all nodes with name "child 1"
			List<MemoNode> children = null;
			for (int i = 0; i < 3; i++) {
				start = System.currentTimeMillis();
				children = root.search(preambleStart, pattern, 0, args);
				end = System.currentTimeMillis();
				log(resp,children.size()>0,"found " + children.size() + " nodes, round:"+i+
						" (" + (end - start) + "ms)");
			}
			log(resp, true, "\nAll tests done!");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
// recursively add children to given parent node
private void addChildren (MemoNode parent, int num, int depth, int props) {
	for (int i = 0; i < num; i++) {
		MemoNode child = new MemoNode("child " + i);
		for (int j = 0; j < props; j++) {
			child.setPropertyValue(String.valueOf(j), "bla bla bla " + j);
		}
		
		parent.addChild(child);
		total++;
		if (depth > 0) {
			addChildren(child, num, depth - 1, props);
		}
	}
}
}
