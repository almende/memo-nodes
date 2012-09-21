package com.chap.memo.memoNodes.servlet;

import java.io.IOException;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.chap.memo.memoNodes.MemoNode;
import com.eaio.uuid.UUID;

@SuppressWarnings("serial")
final public class MemoJSONServlet extends HttpServlet {

	public static boolean get_CORS_headers(java.util.HashMap<String,String> ret, java.util.HashMap<String,String> httpHeaders)
	{
		Boolean isAllowed = true;
		String s = httpHeaders.get( "Origin" );	
		if(s==null) {
				String ref_s = httpHeaders.get( "Referer");
				if (ref_s != null){
					try {
						URL url = new URL(ref_s);
						s = url.getProtocol()+"://"+url.getAuthority();
					} catch(Exception e) {
						s=null;
					}
				}

		}
		//End of Android Hack.
		
		if( s != null )
		{
			ret.put("Access-Control-Allow-Origin", s );
	
			ret.put("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
			ret.put("Access-Control-Allow-Credentials", "true" );
			ret.put("Access-Control-Max-Age", "60" );
			
			String returnMethod = httpHeaders.get("Access-Control-Request-Headers");	//what?
			if (!"".equals(returnMethod)) {
				ret.put("Access-Control-Allow-Headers", returnMethod);
			}
		}
		
		return isAllowed;
	}
	public boolean makeCORS(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException
	{
		//copy headers into the next collection
		java.util.HashMap<String,String> headers = new java.util.HashMap<String, String>();
		@SuppressWarnings("unchecked")
		java.util.Enumeration<String> headerNames = req.getHeaderNames();
		while( headerNames.hasMoreElements() )
		{
			String name = headerNames.nextElement();
			headers.put( name, req.getHeader(name) );
		}
		
		java.util.HashMap<String,String> CORS_headers = new java.util.HashMap<String,String>();
		boolean isAllowed = get_CORS_headers( CORS_headers, headers );
		
		if(isAllowed)
			for( java.util.Map.Entry<String,String> entry : CORS_headers.entrySet() )
				res.setHeader( entry.getKey(),entry.getValue() );
	
		return isAllowed;
	}
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		
		String cleanDBParm = req.getParameter("cleanDB");
		if (cleanDBParm != null) {
			MemoNode.emptyDB();	
			if (cleanDBParm.equals("only")) return;
		}
		
		int maxdepth = 10;
		String depth = req.getParameter("maxdepth");
		if (depth != null){
			try{
				maxdepth = Integer.parseInt(depth);
			} catch (Exception e){}
		}
		
		String NodeId = req.getParameter("nodeId");
		try {
			makeCORS(req,resp);
			
			if (NodeId != null){
				MemoNode node = new MemoNode(new UUID(NodeId));
				resp.setContentType("application/json");
				resp.getWriter().println(
						node.toJSONString(maxdepth)
				);
			}
		} catch (Exception e){
			System.out.println("Warning: Error producing JSON");
			e.printStackTrace();
		}
	}
}
