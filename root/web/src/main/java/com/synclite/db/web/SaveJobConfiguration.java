/*
 * Copyright (c) 2024 mahendra.chavan@syncLite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.db.web;

import java.io.IOException;
import java.util.LinkedHashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.json.JSONObject;

/**
 * Servlet implementation class SaveJobConfiguration
 */
@WebServlet("/saveJobConfiguration")
public class SaveJobConfiguration extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor.
	 */
	public SaveJobConfiguration() {
		super();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			LinkedHashMap<String, String> config = DBWebSupport.validateConfig(request);
			DBWebSupport.persistConfig(config);

			HttpSession session = request.getSession();
			DBWebSupport.applySessionConfig(session, config);

			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");
			response.getWriter().print(new JSONObject()
				.put("result", true)
				.put("message", "Configuration saved successfully")
				.put("db-root", config.get("db-root"))
				.toString());

		} catch (ServletException e) {
			response.setStatus(400);
			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");
			response.getWriter().print(new JSONObject().put("result", false).put("message", e.getMessage()).toString());
		} catch (Exception e) {
			response.setStatus(500);
			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");
			response.getWriter().print(new JSONObject().put("result", false).put("message", "Error: " + e.getMessage()).toString());
		}
	}
}
