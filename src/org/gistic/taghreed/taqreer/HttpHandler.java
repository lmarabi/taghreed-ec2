package org.gistic.taghreed.taqreer;
 
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.gistic.taghreed.basicgeom.MBR;
import org.gistic.taghreed.basicgeom.Point;
import org.joda.time.DateTime;


/**
 * This class is used to initialize an HTTP server
 * @author meshal
 *
 */
public class HttpHandler extends AbstractHandler {
	 
	
	/**
	 * Constructor 
	 */
	public HttpHandler(){
    }
 
	/**
	 * This method is used to handle requests
	 */
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    	response.setHeader("Access-Control-Allow-Origin", "*");
    	response.setCharacterEncoding("UTF-8");
    	response.setContentType("text/html");
    	
        baseRequest.setCharacterEncoding("UTF-8");
        String path = request.getPathInfo();
        
        if (path.equals("/create_new_report")){
        	
    		PrintWriter writer = response.getWriter();
    		
    		ServletContext sc = request.getServletContext();
    		try{
        		//create new generator
            	ReportsGenerator gen = new ReportsGenerator();
            	
            	//setting up parameters for the generator
            	//1- report name
            	gen.setReportName ( request.getParameter("reportName"));
            	
                
            	
            	//2- report region
            	//Min & Max should be in format lat;lng for example : 14.22154;37.112541
            	String reportRegionMin = request.getParameter("reportRegionMin");
            	String reportRegionMax = request.getParameter("reportRegionMax");
            	
            	double reportRegionMinLat = Double.parseDouble(reportRegionMin.split(";")[0]);
            	double reportRegionMinLng = Double.parseDouble(reportRegionMin.split(";")[1]);
            	double reportRegionMaxLat = Double.parseDouble(reportRegionMax.split(";")[0]);
            	double reportRegionMaxLng = Double.parseDouble(reportRegionMax.split(";")[1]);
            	gen.reportRegion = new MBR(new Point(reportRegionMinLng, reportRegionMinLat), new Point(reportRegionMaxLng, reportRegionMaxLat));
            	
            	//3- Temporal Range
            	//dates should be recieved in format 18-Jun-2014
            	SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy"); 
            	gen.minDate = new DateTime( sdf.parse(request.getParameter("minDate")));
            	gen.maxDate = new DateTime( sdf.parse(request.getParameter("maxDate")));
            	
            	//spatial analysis, loop through them and creat new area
            	int counter = 1;
            	while(request.getParameter("spatialAnalysisAreaName" + counter)!=null){
            		String spatialAnalysisMin = request.getParameter("spatialAnalysis" + counter + "Min");
                	String spatialAnalysisMax = request.getParameter("spatialAnalysis" + counter + "Max");
                	
                	double spatialAnalysisMinLat = Double.parseDouble(spatialAnalysisMin.split(";")[0]);
                	double spatialAnalysisMinLng = Double.parseDouble(spatialAnalysisMin.split(";")[1]);
                	double spatialAnalysisMaxLat = Double.parseDouble(spatialAnalysisMax.split(";")[0]);
                	double spatialAnalysisMaxLng = Double.parseDouble(spatialAnalysisMax.split(";")[1]);
                	
            		MBR MBR = new MBR(new Point(spatialAnalysisMinLng, spatialAnalysisMinLat), new Point(spatialAnalysisMaxLng, spatialAnalysisMaxLat));
            		Area area = new Area(request.getParameter("spatialAnalysisAreaName" + counter ), MBR);
            		gen.areas.add(area);
            		counter++;
            		
            	}
            	
            	
            	//sides analysis, loop through them and create new eventside
            	counter = 1;
            	while(request.getParameter("sideName" + counter)!=null){
            		EventSide side = new EventSide();
            		side.sideName = request.getParameter("sideName" + counter);
            		side.sideKeyword = request.getParameter("sideKeyword" + counter);
            		side.sideColor = request.getParameter("sideColor" + counter);
                
            		//add this side to the generator
            		gen.eventSides.add(side);
            		counter++;

            	}

            	
            	gen.generateReport();
                
            	//writing back the report
            	FileInputStream file = new FileInputStream(gen.getReportName());
            	int c;
                while ((c = file.read()) != -1) {
                	response.getWriter().write(c);
                }
                file.close();
                
            	//IOUtils.copy(file, response.getOutputStream());
            	
            	
            	
        	}catch (Exception ex){
        		ex.printStackTrace(System.err);
        		response.sendError(500);
    	        writer.write( ex.getMessage());
    	        writer.flush();
    		}finally{
    			writer.close();
    		}
    		
    		
    		
    		
        }else if (path.equals("/get_saved_report")){
        	ReportsGenerator gen = new ReportsGenerator();
        	gen.setReportName(request.getParameter("reportName"));
        	FileInputStream file = new FileInputStream(gen.getReportName());
        	int c;
            while ((c = file.read()) != -1) {
            	response.getWriter().write(c);
            }
            file.close();

        
        }else if (path.startsWith("/img")){
        	FileInputStream file = new FileInputStream(path.substring(1));
        	//IOUtils.copy(file, response.getOutputStream());
        }
        
    }
    
    
    /**
     * Main method will used to create server and attach a handler
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
    	Server server = new Server(8095);
        server.setHandler(new HttpHandler());
        server.start();
        server.join();
    }
}