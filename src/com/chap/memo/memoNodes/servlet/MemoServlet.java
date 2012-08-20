package com.chap.memo.memoNodes.servlet;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.chap.memo.memoNodes.MemoNode;

public class MemoServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		doGet(req, resp);
	}

	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		resp.setContentType("text/plain");
		try {
			
			String mergeShards = req.getParameter("merge");
			if (mergeShards != null){
				MemoNode.compactDB();
				resp.getWriter().append("database compacted!\n");
			}
			String cleanDBParm = req.getParameter("cleanDB");
			if (cleanDBParm != null) {
				MemoNode.emptyDB();
				resp.getWriter().append("database cleared!\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
			resp.getWriter().append("Error:" + e.getLocalizedMessage());
		}

	}
}
