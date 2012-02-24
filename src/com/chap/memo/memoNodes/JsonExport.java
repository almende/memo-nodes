package com.chap.memo.memoNodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.eaio.uuid.UUID;

@SuppressWarnings("serial")
public class JsonExport extends HttpServlet {

	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		String useDatastore = req.getParameter("useDatastore");
		if (useDatastore != null){
			NodeList.useDataStore();
		} else {
			NodeList.useMemory();
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
			if (NodeId != null){
				MemoNode node = NodeList.find(new UUID(NodeId));
				resp.setContentType("application/json");
				resp.getWriter().println(
						node.toJSON("", maxdepth)
				);
			}
		} catch (Exception e){
			System.out.println("Warning: UUID not parsible");
		}
	}
}
