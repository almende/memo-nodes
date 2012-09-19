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
		
		String input = req.getParameter("import");
		System.out.println("Import param:'"+input+"'");
		if (input != null){
			
			if (req.getParameter("history") == null) MemoNode.emptyDB();
			MemoNode.importDB(req.getInputStream());
			MemoNode.flushDB();
		} else {
			doGet(req, resp);
		}
	}

	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		try {
			
			String mergeShards = req.getParameter("merge");
			if (mergeShards != null){
				MemoNode.compactDB();
				resp.setContentType("text/plain");
				resp.getWriter().append("database compacted!\n");
			}
			String cleanDBParm = req.getParameter("cleanDB");
			if (cleanDBParm != null) {
				MemoNode.emptyDB();
				resp.setContentType("text/plain");
				resp.getWriter().append("database cleared!\n");
			}
//			String dropHistory = req.getParameter("dropHistory");
//			if (dropHistory != null){
//				MemoNode.dropHistory();
//				resp.setContentType("text/plain");
//				resp.getWriter().append("History cleared!\n");
//			}
			String export = req.getParameter("export");
			if (export != null){
				MemoNode.flushDB();
				MemoNode.compactDB();
				resp.setHeader("Content-Disposition","attachment; filename=\"MemoNodes.zip\"");
				resp.setContentType("application/zip");
				if (req.getParameter("history") == null){
					MemoNode.exportDB(resp.getOutputStream());	
				} else {
					MemoNode.purgeHistory(resp.getOutputStream());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			resp.setContentType("text/plain");
			resp.getWriter().append("Error:" + e.getLocalizedMessage());
		}

	}
}
