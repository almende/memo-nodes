package com.chap.memo.memoNodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


@SuppressWarnings("serial")
public class MemoServlet extends HttpServlet {
/*
	
	private final double GRIDSIZE = 22;
	public void dumpTravelMapHtml(MemoNode mapRoot,HttpServletResponse resp) throws IOException{
		for (MemoNode loc : mapRoot.getChildren()){
			String x = loc.getPropertyValue("x");
			String y = loc.getPropertyValue("y");
			Integer value = Integer.parseInt(loc.getValue());
			String color = "rgb("+(256-(2*value))+","+(50+value)+","+(50+value)+")";
			resp.getWriter().print("<div style='position:absolute;display:block;border:1px black solid;width:20px;height:20px;left:"+(Integer.parseInt(x)-(int)Math.round(GRIDSIZE/2))+"px;bottom:"+(Integer.parseInt(y)-(int)Math.round(GRIDSIZE/2))+"px;background-color:"+color+"'>"+loc.getValue()+"</div>");
		}
	}
	
	public void createTravelMap(MemoNode mapRoot, double x, double y, double h, double v){
		ArrayList<MemoNode> list = new ArrayList<MemoNode>((int)(600*800 / GRIDSIZE));
		int maxSpeed = 8;
		int maxAcceleration = 1;
		int maxDeceleration = 2;
		for (int j = (int) Math.round(GRIDSIZE/2); j < 600 ; j+=GRIDSIZE){
			for (int i = (int) Math.round(GRIDSIZE/2); i < 800; i+=GRIDSIZE){
				//first order approximation: t = d/max-v
				Long mintime = Math.round((Math.sqrt(Math.pow(x-i, 2)+Math.pow(y-j,2))/maxSpeed));
				//second order, add acceleration compensation, based on current known speed and direction
				Double angle = Math.atan2(i-x, j-y)-h;
				Double deltaV = (Math.sqrt(Math.pow(v, 2) + Math.pow(maxSpeed, 2) - 2*maxSpeed*v*Math.cos(angle)));
				Double projectedV = Math.cos(angle)*deltaV;
				Long breaktime = Math.round(projectedV >= 0?0:Math.min(v, Math.abs(projectedV))/maxDeceleration);
				Long acceltime = Math.round(Math.abs(deltaV)/maxAcceleration); //=inaccurate, but also accounts for the "overshoot" while breaking				
				System.out.println(i+"/"+j+": "+Math.round(Math.toDegrees(angle))+":"+Math.round(deltaV)+":"+Math.round(projectedV)+":"+breaktime+":"+acceltime);

				
				//if (h and v) are set, add time to make the turn/stop-revert or skip acceleration duration if pointed this way
				
				//third order, add expected behavior
				MemoNode loc = new Unode(Node.store(mintime.toString()));
				Node.storeAsChild(new Long(Math.round(new Double(i))).toString(),Node.storeAsChild("x",loc).child);
				Node.storeAsChild(new Long(Math.round(new Double(j))).toString(),Node.storeAsChild("y",loc).child);
				list.add(loc);
			}
		}
		mapRoot.bulkAddChildren(list);
		System.out.println("Adding "+list.size()+" children:"+mapRoot.getChildren().size());
	}
*/	
	@SuppressWarnings("unused")
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		String cleanDBParm=req.getParameter("cleanDB");
		if (cleanDBParm != null){
			resp.setContentType("text/plain");
			resp.getWriter().println("Database cleared!");
			MemoShardStore.emptyDB();
		}
		if (cleanDBParm != null && cleanDBParm.equals("only")){
			return;
		}
		
		resp.setContentType("text/html");
		
		Date start = new Date();
		resp.getWriter().println("<html><body>Starting to generate Nodes<br><div style='display:block;position:relative;width:1000px;height:600px'>");
	/*
		MemoNode map = new Unode(Node.store("TestMap"));
		createTravelMap(map,250,100,0,3);
		dumpTravelMapHtml(map,resp);
		resp.getWriter().println("</div><br>Done, in "+(new Date().getTime()-start.getTime())+" ms<br>");
		resp.getWriter().println("</body></html>");
*/
		
		
		//Generate 10.000 nodes in one linked list
		Node node = Node.store("start");
		for (int i=0; i< 5000000; i++){
			node = Node.storeAsParent(new Integer(i).toString(), node).parent;
		}		
		Date time = new Date();
		System.out.println("Storing done in "+(time.getTime()-start.getTime())+" ms");
		resp.getWriter().println("<br>Done in:"+(time.getTime()-start.getTime())+" ms<br>");
		int count=0;
		//node = Node.find("start");
		while (node != null ){
			String value=node.getValue();
			//System.out.println(value+ ":" + node.getChildren().size());
			ArrayList<Node> children = node.getChildren();
			if (children.isEmpty()){
				System.out.println(node.getId() + " has no children!");
				break;
			}
			count++;
			if (children.get(0) == null) System.out.println("NUll node!");
			node = children.get(0);
		}
		resp.getWriter().println("Count " + count + " counted in:"+(new Date().getTime()-time.getTime())+" ms<br>");
		//resp.getWriter().println("Start node history:"+startNode.history().toString());
		/*
		time = new Date();
		count=0;

		for (int i=0; i<10; i++){
			String nodeId = new Integer(new Random().nextInt(500000)).toString();
			node = NodeList.find(nodeId);
			if (node != null && !node.getChildren().isEmpty()){
				count++;
			} else {
				System.out.println("Node not found! :" + nodeId);
			}
		}
		resp.getWriter().println("Random accessed "+count+" in:"+(new Date().getTime()-time.getTime())+" ms");
		*/
	}
}
