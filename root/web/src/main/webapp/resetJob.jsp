<%-- 
    Copyright (c) 2024 mahendra.chavan@syncLite.io, all rights reserved.

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
    in compliance with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
    or implied.  See the License for the specific language governing permissions and limitations
    under the License.
--%>

<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>Reset SyncLite DB Job</title>
</head>

<%
String errorMsg = request.getParameter("errorMsg");
String successMsg = request.getParameter("successMsg");

String keepJobConfiguration = "true";
if (request.getParameter("keep-job-configuration") != null) {
	keepJobConfiguration = request.getParameter("keep-job-configuration");
}

String keepDbFiles = "true";
if (request.getParameter("keep-db-files") != null) {
	keepDbFiles = request.getParameter("keep-db-files");
}
%>

<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Reset SyncLite DB Job</h2>
		<%
		if (session.getAttribute("db-root") == null) {
			out.println("<h4 style=\"color: red;\"> Please configure and start/load a SyncLite DB job first.</h4>");
			throw new javax.servlet.jsp.SkipPageException();		
		}

		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		if (successMsg != null) {
			out.println("<h4 style=\"color: green;\">" + successMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/resetJob" method="post">
			<table>
				<tbody>
					<tr>
						<td colspan="2">
							Please note that resetting a job implies restarting the SyncLite DB server from scratch upon reconfiguration and restarting the job.<br>
							All staging data, metadata, and logs will be deleted. Database files are preserved by default.
						</td>
					</tr>

					<tr>
						<td>Keep Job Configuration</td>
						<td><select id="keep-job-configuration" name="keep-job-configuration" title="Specify if the job configuration files should be preserved.">
								<%
								if (keepJobConfiguration.equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
									out.println("<option value=\"false\">false</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
									out.println("<option value=\"false\" selected>false</option>");
								}
								%>
						</select></td>
					</tr>

					<tr>
						<td>Keep Database Files</td>
						<td><select id="keep-db-files" name="keep-db-files" title="Specify if database (.db) files should be preserved. Default is true.">
								<%
								if (keepDbFiles.equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
									out.println("<option value=\"false\">false</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
									out.println("<option value=\"false\" selected>false</option>");
								}
								%>
						</select></td>
					</tr>
				</tbody>
			</table>
			<center>
				<button type="submit" name="reset">Reset Job</button>
			</center>
		</form>
	</div>
</body>
</html>
