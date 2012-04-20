package com.chap.memo.memoNodes.NewImpl;

import java.io.IOException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.eaio.uuid.UUID;

@SuppressWarnings("serial")
final public class JsonExport extends HttpServlet {

	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		MemoReadBus bus = MemoReadBus.getBus();
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
				MemoNode node = bus.find(new UUID(NodeId));
				resp.setContentType("application/json");
				resp.getWriter().println(
						node.toJSONString(maxdepth)
				);
			}
		} catch (Exception e){
			System.out.println("Warning: UUID not parsible");
		}
	}
}
