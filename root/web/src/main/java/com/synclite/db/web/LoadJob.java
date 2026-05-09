package com.synclite.db.web;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/loadJob")
public class LoadJob extends HttpServlet {
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			Path dbRoot = DBWebSupport.getDbRoot(request.getParameter("db-root"));
			if (!Files.exists(dbRoot) || !Files.isDirectory(dbRoot)) {
				throw new ServletException("Specified \"Database Root Directory\" does not exist");
			}
			if (!dbRoot.toFile().canRead() || !dbRoot.toFile().canWrite()) {
				throw new ServletException("Specified \"Database Root Directory\" must have read and write permission");
			}

			Path configPath = DBWebSupport.getConfigPath(dbRoot);
			if (!Files.exists(configPath)) {
				throw new ServletException("SyncLite DB configuration file does not exist in the specified Database Root Directory");
			}

			LinkedHashMap<String, String> config = DBWebSupport.readConfig(dbRoot);
			DBWebSupport.applySessionConfig(request.getSession(), config);
			request.getSession().setAttribute("job-status", DBWebSupport.findRunningServerPid(configPath) > 0 ? "STARTED" : "STOPPED");
			response.sendRedirect("dashboard.jsp");
		} catch (ServletException e) {
			response.setStatus(400);
			response.sendRedirect("loadJob.jsp?errorMsg=" + URLEncoder.encode(e.getMessage(), StandardCharsets.UTF_8.name()));
		} catch (Exception e) {
			response.setStatus(500);
			response.sendRedirect("loadJob.jsp?errorMsg=" + URLEncoder.encode("Error: " + e.getMessage(), StandardCharsets.UTF_8.name()));
		}
	}
}