package com.chap.memo.memoNodes.servlet;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;

import com.chap.memo.memoNodes.MemoNode;
import com.eaio.uuid.UUID;

/*
 * Internal API for MemoDistribution, not meant as external API. 
 */
public class MemoServlet extends HttpServlet {
	private static final long serialVersionUID = 4288460220487327301L;
	private static final Logger log = Logger
			.getLogger(com.chap.memo.memoNodes.servlet.MemoServlet.class.getName());

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// SinglePOST format:
		// command,arguments

		response.setContentType("application/binary");
		MessagePack msgpack = new MessagePack();
		Unpacker unpacker = msgpack.createUnpacker(request.getInputStream());
		UUID id = unpacker.read(UUID.class);
		if (id == null)
			response.sendError(403);
		MemoNode node = new MemoNode(id);

		int cmd = unpacker.readInt();
		try {

			switch (cmd) {
			case 0: // getValue
				response.getOutputStream().write(node.valueToMsg());
				break;
			case 1: // getParents
				response.getOutputStream().write(node.parentsToMsg());
				break;
			case 2: // getChildren
				response.getOutputStream().write(node.childrenToMsg());
				break;
			default:
				log.severe("Unknown command:" + cmd);
			}
		} catch (Exception e) {
			log.severe("API exception:" + cmd + "->" + e.getMessage());
			response.sendError(500);
		}

	}
}
