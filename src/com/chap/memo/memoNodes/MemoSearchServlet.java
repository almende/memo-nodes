package com.chap.memo.memoNodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class MemoSearchServlet extends HttpServlet {
	@SuppressWarnings("unused")
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {

		resp.setContentType("text/plain");
		Date start = new Date();
		
		resp.getWriter().println("Starting to generate test set");
		
		/*                                        _
		 *      start                            \ /
		 *       /  \                PreAmble -- any
		 *   >--1    2               Pattern  -- 5 -- 8
		 *   | / \  / \                (three 5's should be matched in the diagram to the left)
		 *   |/   3    4
		 *   3    |   / \
		 *   |    5  5   5
		 *   6    |  |   |
		 *  / \   8  7   8
		 * 5   7
		 * |
		 * 8
		 * 
		 */
		MemoNode startNode = new Unode(Node.store("start"));
		MemoNode one       = new Unode(Node.storeAsChild("One",startNode).child);
		MemoNode two       = new Unode(Node.storeAsChild("Two",startNode).child);
		MemoNode three1    = new Unode(Node.storeAsChild("Three",    one).child);
		three1.addChild(one);//Loop
		MemoNode three2    = new Unode(Node.storeAsChild("Three",    two).child);
		three2.addParent(one);//multipath
		MemoNode four      = new Unode(Node.storeAsChild("Four",     two).child);
		MemoNode six       = new Unode(Node.storeAsChild("Six",   three1).child);
		MemoNode five1     = new Unode(Node.storeAsChild("Five",     six).child);
		MemoNode eight1    = new Unode(Node.storeAsChild("Eight",  five1).child);
		MemoNode seven1    = new Unode(Node.storeAsChild("Seven",    six).child);
		MemoNode five2     = new Unode(Node.storeAsChild("Five",  three2).child);
		MemoNode eight2    = new Unode(Node.storeAsChild("Eight",  five2).child);
		MemoNode five3     = new Unode(Node.storeAsChild("Five",    four).child);
		MemoNode seven2    = new Unode(Node.storeAsChild("Seven",  five3).child);
		MemoNode five4     = new Unode(Node.storeAsChild("Five",    four).child);
		MemoNode eight3    = new Unode(Node.storeAsChild("Eight",  five4).child);
		
		
		
		MemoNode algorithm = new Unode(Node.store("algo"));
	    MemoNode pattern   = new Unode(Node.storeAsChild("Pattern",  algorithm).child);
		MemoNode patFive   = new Unode(Node.storeAsChild("equal;Five", pattern).child);
		MemoNode patEight  = new Unode(Node.storeAsChild("equal;Eight",patFive).child);
		MemoNode preAmble  = new Unode(Node.storeAsChild("PreAmble", algorithm).child);
		MemoNode PreOne    = new Unode(Node.storeAsChild("equal;One", preAmble).child);
		MemoNode PreThree  = new Unode(Node.storeAsChild("equal;Three", PreOne).child);
		MemoNode preAny    = new Unode(Node.storeAsChild("any",       PreThree).child);
		preAny.addChild(preAny);  //Spannend:)
		
		Date time = new Date();
		System.out.println("Storing done in "+(time.getTime()-start.getTime())+" ms");
		resp.getWriter().println("Storing done in "+(time.getTime()-start.getTime())+" ms");
		start = time;
		
		ArrayList<MemoResult> result = startNode.search(algorithm, -1);
		for (MemoResult res: result){
			System.out.println("Found 1: "+res.result.getValue());
			resp.getWriter().println("Found 1: "+res.result.getValue());
		}
		time = new Date();
		System.out.println("Search 1 done in "+(time.getTime()-start.getTime())+" ms");
		resp.getWriter().println("Search 1 done in "+(time.getTime()-start.getTime())+" ms");
		start = time;

		result = four.search(algorithm, 2); //topx = 2
		for (MemoResult res: result){
			System.out.println("Found 2: "+res.result.getValue());
			resp.getWriter().println("Found 2: "+res.result.getValue());
		}
		time = new Date();
		System.out.println("Search 2 done in "+(time.getTime()-start.getTime())+" ms");
		resp.getWriter().println("Search 2 done in "+(time.getTime()-start.getTime())+" ms");
	}
}
