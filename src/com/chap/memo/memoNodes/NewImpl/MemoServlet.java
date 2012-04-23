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
		
	}
		
		
}
