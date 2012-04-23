package com.chap.memo.memoNodes.NewImpl;

import java.io.IOException;
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
		MemoWriteBus.emptyDB();
		
		log(resp,true,"Database cleared!");
		
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
		first.update(title.getBytes());
		log(resp,test(first.getStringValue(),title),"Node new value and read: "+first.getId()+"/"+first.getStringValue());
		
		first = MemoReadBus.getBus().find(first.getId());
		log(resp,test(first.getStringValue(),title),"Node found: "+first.getId()+"/"+first.getStringValue());

		title="double new Value in same shard (First node)";
		first.update(title.getBytes());
		log(resp,test(first.getStringValue(),title),"Node new value and read: "+first.getId()+"/"+first.getStringValue());
		
		first = MemoReadBus.getBus().find(first.getId());
		log(resp,test(first.getStringValue(),title),"Node found: "+first.getId()+"/"+first.getStringValue());

		MemoWriteBus.getBus().flush();
		log(resp,test(first.getStringValue(),title),"Read after flush: "+first.getId()+"/"+first.getStringValue());
		
		first = MemoReadBus.getBus().find(first.getId());
		log(resp,test(first.getStringValue(),title),"Node found after flush: "+first.getId()+"/"+first.getStringValue());		
		
		title = "First node";
		first.update(title.getBytes());
		
		String secondTitle = "Second node";
		MemoNode second = new MemoNode(secondTitle);
		
		first.addChild(second.getId());
		log(resp,test(new Integer(first.getChildren().size()).toString(),"1"),"Node has children:"+first.getId()+":"+first.getChildren().size());
		log(resp,test(new Integer(first.getParents().size()).toString(),"0"),"Node has parents:"+first.getId()+":"+first.getParents().size());
		log(resp,test(new Integer(second.getParents().size()).toString(),"1"),"Child has parents:"+second.getId()+":"+second.getParents().size());
		log(resp,test(first.getChildren().get(0).getStringValue(),secondTitle),"Child found: "+first.getId()+"|"+first.getChildren().get(0).getId()+"/"+first.getChildren().get(0).getStringValue());
		
		MemoWriteBus.getBus().flush();
		second = MemoReadBus.getBus().find(first.getId()).getChildren().get(0);
		log(resp,test(second.getStringValue(),secondTitle),"Child found after flush: "+second.getId()+"/"+second.getStringValue());		
		
		secondTitle = "Second node (new Value)";
		second.update(secondTitle.getBytes());
		log(resp,test(first.getChildren().get(0).getStringValue(),secondTitle),"Updated child found: "+first.getId()+"|"+first.getChildren().get(0).getId()+"/"+first.getChildren().get(0).getStringValue());
		
		String thirdTitle="Third Node";
		MemoNode third = new MemoNode(thirdTitle);
		
		first.addChild(third.getId());
		log(resp,test(new Integer(first.getChildren().size()).toString(),"2"),"Node has children:"+first.getId()+":"+first.getChildren().size());
		log(resp,test(new Integer(first.getParents().size()).toString(),"0"),"Node has parents:"+first.getId()+":"+first.getParents().size());
		log(resp,test(new Integer(second.getParents().size()).toString(),"1"),"Child has parents:"+second.getId()+":"+second.getParents().size());
		log(resp,test(new Integer(third.getParents().size()).toString(),"1"),"Child has parents:"+third.getId()+":"+third.getParents().size());
		log(resp,true,"Child found: "+first.getId()+"|"+first.getChildren().get(0).getId()+"/"+first.getChildren().get(0).getStringValue());
		log(resp,true,"Child found: "+first.getId()+"|"+first.getChildren().get(1).getId()+"/"+first.getChildren().get(1).getStringValue());
		
		log(resp,test(third.getParents().get(0).getStringValue(),title),"Parent found: "+third.getParents().get(0).getStringValue());
		
	}
		
		
}
