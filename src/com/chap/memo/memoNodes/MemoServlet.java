package com.chap.memo.memoNodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class MemoServlet extends HttpServlet {

	private void log(HttpServletResponse resp, boolean ok, String msg){
		try {
			resp.getWriter().println((ok?"[OK]":"!!!ERROR!!!")+"  -   "+msg);
			resp.flushBuffer();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(msg);
	}
	private boolean test(String compare, String result){
		return compare.equals(result);
	}
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		
		resp.setContentType("text/plain");
		String cleanDBParm = req.getParameter("cleanDB");
		if (cleanDBParm != null) {
			MemoWriteBus.emptyDB();
			log(resp,true,"Database cleared!");
		
			if (cleanDBParm.equals("only")) return;
		}
		//Add single node
		String title="First node";
		MemoNode first = new MemoNode(title);
		
		log(resp,test(first.getStringValue(),title),"Node written and read:"+first.getStringValue());
		
		first = MemoReadBus.getBus().find(first.getId());
		log(resp,test(first.getStringValue(),title),"Node found: "+first.getId()+"/"+first.getStringValue());
		
		MemoWriteBus.getBus().flush();
		log(resp,test(first.getStringValue(),title),"Read after flush: "+first.getId()+"/"+first.getStringValue());
		
		first = MemoReadBus.getBus().find(first.getId());
		log(resp,test(first.getStringValue(),title),"Node found after flush: "+first.getId()+"/"+first.getStringValue());		
		
		title="New Value (First node)";
		first.update(title);
		log(resp,test(first.getStringValue(),title),"Node new value and read: "+first.getId()+"/"+first.getStringValue());
		
		first = MemoReadBus.getBus().find(first.getId());
		log(resp,test(first.getStringValue(),title),"Node found: "+first.getId()+"/"+first.getStringValue());

		title="double new Value in same shard (First node)";
		first.update(title);
		log(resp,test(first.getStringValue(),title),"Node new value and read: "+first.getId()+"/"+first.getStringValue());
		
		first = MemoReadBus.getBus().find(first.getId());
		log(resp,test(first.getStringValue(),title),"Node found: "+first.getId()+"/"+first.getStringValue());
		try {
			Thread.sleep(1);
		} catch (Exception e){};
		MemoWriteBus.getBus().flush();
		log(resp,test(first.getStringValue(),title),"Read after flush: "+first.getId()+"/"+first.getStringValue());
		
		first = MemoReadBus.getBus().find(first.getId());
		log(resp,test(first.getStringValue(),title),"Node found after flush: "+first.getId()+"/"+first.getStringValue());		
		
		title = "First node";
		first.update(title);
		
		String secondTitle = "Second node";
		MemoNode second = new MemoNode(secondTitle);
		
		first.addChild(second);
		log(resp,(first.getChildren().size()==1),"Node has children:"+first.getId()+":"+first.getChildren().size());
		log(resp,(first.getParents().size()==0),"Node has parents:"+first.getId()+":"+first.getParents().size());
		log(resp,(second.getParents().size()==1),"Child has parents:"+second.getId()+":"+second.getParents().size());
		log(resp,test(first.getChildren().get(0).getStringValue(),secondTitle),"Child found: "+first.getId()+"|"+first.getChildren().get(0).getId()+"/"+first.getChildren().get(0).getStringValue());
		
		MemoWriteBus.getBus().flush();
		second = MemoReadBus.getBus().find(first.getId()).getChildren().get(0);
		log(resp,test(second.getStringValue(),secondTitle),"Child found after flush: "+second.getId()+"/"+second.getStringValue());		
		
		secondTitle = "Second node (new Value)";
		second.update(secondTitle);
		log(resp,test(first.getChildren().get(0).getStringValue(),secondTitle),"Updated child found: "+first.getId()+"|"+first.getChildren().get(0).getId()+"/"+first.getChildren().get(0).getStringValue());
		
		String thirdTitle="Third Node";
		MemoNode third = new MemoNode(thirdTitle);
		
		first.addChild(third);
		log(resp,(first.getChildren().size()==2),"Node has children:"+first.getId()+":"+first.getChildren().size());
		log(resp,(first.getParents().size()==0),"Node has parents:"+first.getId()+":"+first.getParents().size());
		log(resp,(second.getParents().size()==1),"Child has parents:"+second.getId()+":"+second.getParents().size());
		log(resp,(third.getParents().size()==1),"Child has parents:"+third.getId()+":"+third.getParents().size());
		log(resp,true,"Child found: "+first.getId()+"|"+first.getChildren().get(0).getId()+"/"+first.getChildren().get(0).getStringValue());
		log(resp,true,"Child found: "+first.getId()+"|"+first.getChildren().get(1).getId()+"/"+first.getChildren().get(1).getStringValue());
		
		log(resp,test(third.getParents().get(0).getStringValue(),title),"Parent found: "+third.getParents().get(0).getStringValue());
		
		log(resp,true,"Same shard OPS delete and add again:");
		first.delChild(third);
		log(resp,(first.getChildren().size()==1),"Node has children:"+first.getId()+":"+first.getChildren().size());
		log(resp,(first.getParents().size()==0),"Node has parents:"+first.getId()+":"+first.getParents().size());
		log(resp,(second.getParents().size()==1),"Child has parents:"+second.getId()+":"+second.getParents().size());
		log(resp,(third.getParents().size()==0),"Child has parents:"+third.getId()+":"+third.getParents().size());
		log(resp,true,"Child found: "+first.getId()+"|"+first.getChildren().get(0).getId()+"/"+first.getChildren().get(0).getStringValue());

		first.addChild(third);
		log(resp,(first.getChildren().size()==2),"Node has children:"+first.getId()+":"+first.getChildren().size());
		log(resp,(first.getParents().size()==0),"Node has parents:"+first.getId()+":"+first.getParents().size());
		log(resp,(second.getParents().size()==1),"Child has parents:"+second.getId()+":"+second.getParents().size());
		log(resp,(third.getParents().size()==1),"Child has parents:"+third.getId()+":"+third.getParents().size());
		log(resp,true,"Child found: "+first.getId()+"|"+first.getChildren().get(0).getId()+"/"+first.getChildren().get(0).getStringValue());
		log(resp,true,"Child found: "+first.getId()+"|"+first.getChildren().get(1).getId()+"/"+first.getChildren().get(1).getStringValue());

		log(resp,true,"Older shard OPS delete and add again:");
		first.delChild(second);
		log(resp,(first.getChildren().size()==1),"Node has children:"+first.getId()+":"+first.getChildren().size());
		log(resp,(first.getParents().size()==0),"Node has parents:"+first.getId()+":"+first.getParents().size());
		log(resp,(second.getParents().size()==0),"Child has parents:"+second.getId()+":"+second.getParents().size());
		log(resp,(third.getParents().size()==1),"Child has parents:"+third.getId()+":"+third.getParents().size());
		log(resp,true,"Child found: "+first.getId()+"|"+first.getChildren().get(0).getId()+"/"+first.getChildren().get(0).getStringValue());

		first.addChild(second);
		log(resp,(first.getChildren().size()==2),"Node has children:"+first.getId()+":"+first.getChildren().size());
		log(resp,(first.getParents().size()==0),"Node has parents:"+first.getId()+":"+first.getParents().size());
		log(resp,(second.getParents().size()==1),"Child has parents:"+second.getId()+":"+second.getParents().size());
		log(resp,(third.getParents().size()==1),"Child has parents:"+third.getId()+":"+third.getParents().size());
		log(resp,true,"Child found: "+first.getId()+"|"+first.getChildren().get(0).getId()+"/"+first.getChildren().get(0).getStringValue());
		log(resp,true,"Child found: "+first.getId()+"|"+first.getChildren().get(1).getId()+"/"+first.getChildren().get(1).getStringValue());
		
		int nofNodes = 10000;
		String sNofNodes = req.getParameter("nofNodes");
		if (sNofNodes != null){
			try {
				nofNodes=Integer.parseInt(sNofNodes);
			} catch (Exception e){
				System.out.println("couldn't parse nofNodes="+sNofNodes);
			}
		}
		Date start = new Date();
		log(resp,true,"Start to generate nodes:"+start.toString());
		
		MemoNode node = new MemoNode("start");
		for (int i = 0; i< nofNodes; i++) {
			MemoNode newNode = new MemoNode(new Integer(i).toString());
			node.addParent(newNode.getId());
			node = newNode;
		}
		Date time = new Date();
		log(resp,true,"Storing done in: "+(time.getTime() - start.getTime())+" ms");

		int count = 0;
		// node = Node.find("start");
		while (node != null) {
			//String value = node.getStringValue();
			//System.out.println(value+ ":" + node.getChildren().size());
			ArrayList<MemoNode> children = node.getChildren();
			if (children.isEmpty()) {
				System.out.println(node.getId() +":"+ node.getStringValue() + " has no children!");
				break;
			}
			count++;
			if (children.get(0) == null){
				System.out.println(node.getId()+":"+ node.getStringValue() + " has NUll node as a child!");
			}
			node = children.get(0);
		}
		log(resp,(count==nofNodes),
				"Count " + count + " counted in:"
						+ (new Date().getTime() - time.getTime()) + " ms");
		
		MemoWriteBus.getBus().flush();

		nofNodes = 10000;
		sNofNodes = req.getParameter("nofArcs");
		if (sNofNodes != null){
			try {
				nofNodes=Integer.parseInt(sNofNodes);
			} catch (Exception e){
				System.out.println("couldn't parse nofArcs="+sNofNodes);
			}
		}
		start = new Date();
		log(resp,true,"Start to generate nodes:"+start.toString());
		
		MemoNode startNode = new MemoNode("start");
		for (int i = 0; i< nofNodes; i++) {
			MemoNode newNode = new MemoNode(new Integer(i).toString());
			startNode.addChild(newNode);
		}
		time = new Date();
		log(resp,true,"Storing done in: "+(time.getTime() - start.getTime())+" ms");

		count = 0;
		Iterator<MemoNode> iter = startNode.getChildren().iterator();
		// node = Node.find("start");
		while (iter.hasNext()) {
			node = iter.next();
			//String value = node.getStringValue();
			count++;
		}
		log(resp,(count==nofNodes),
				"Count " + count + " counted in:"
						+ (new Date().getTime() - time.getTime()) + " ms");

		MemoWriteBus.getBus().flush();
		
		start = new Date();

		log(resp,true,"Starting to generate test set"+start.toString());

		/*                                        _
		 *      start                            \ /
		 *       /  \                PreAmble -- any
		 *   >--1    2               Pattern  -- 5 -- 8
		 *   | / \  / \                (three 5's should be matched in the diagram to the left)
		 *   |/   3    4
		 *   3    | \ / \
		 *   |    5  5   5
		 *   6    |  |   |
		 *  / \   8  7   8
		 * 5   7
		 * |
		 * 8
		*/
		startNode  = new MemoNode("start");
		MemoNode one = new MemoNode("One");
		startNode.addChild(one);
		
		MemoNode two = new MemoNode("Two");
		startNode.addChild(two);
		
		MemoNode three1 = new MemoNode("Three");
		one.addChild(three1);
		three1.addChild(one); //Loop
		
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

		time = new Date();
		log(resp,true,"Pattern Storing done in " + (time.getTime() - start.getTime())
								+ " ms");
		start = time;

		HashMap<String,String> arguments = new HashMap<String,String>(2);
		arguments.put("Number","Eight");
		ArrayList<MemoNode> result = startNode.search(algorithm, -1,arguments);
		for (MemoNode res : result) {
			log(resp,true,"Found 1: " + res.getStringValue() +"/"+ res.getId());
		}
		time = new Date();
		log(resp,result.size()==2,"Search 1 done in " + (time.getTime() - start.getTime())
						+ " ms");
		start = time;

		arguments.put("Number","Seven");
		result = startNode.search(algorithm, 2,arguments); // topx = 2
		for (MemoNode res : result) {
			log(resp,true,"Found 2: " + res.getStringValue() +"/"+ res.getId());
		}
		time = new Date();
		log(resp,result.size()==1,"Search 2 done in " + (time.getTime() - start.getTime())
						+ " ms");
	}	
}
