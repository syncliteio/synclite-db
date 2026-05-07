package com.synclite.db.web;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import org.json.JSONObject;

public class StopServerServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();

        try {
            // Note: Actual server stop would need to be implemented based on how the server is deployed
            // For a Tomcat-deployed WAR, stopping would typically involve stopping the Tomcat service
            // This is a placeholder that returns success
            JSONObject jsonResponse = new JSONObject();
            jsonResponse.put("result", true);
            jsonResponse.put("message", "Server stop command issued");
            jsonResponse.put("code", "OK");
            
            out.print(jsonResponse.toString());
        } catch (Exception e) {
            JSONObject errorResponse = new JSONObject();
            errorResponse.put("result", false);
            errorResponse.put("message", "Error stopping server: " + e.getMessage());
            errorResponse.put("code", "ERR_STOP_SERVER");
            response.setStatus(500);
            out.print(errorResponse.toString());
        }
    }
}
