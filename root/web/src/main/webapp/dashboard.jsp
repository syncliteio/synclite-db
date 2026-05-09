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

<%@page import="java.time.ZoneId"%>
<%@page import="java.time.LocalDateTime"%>
<%@page import="java.time.Instant"%>
<%@page import="com.synclite.db.web.DBWebSupport"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.nio.file.Files"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.InputStreamReader"%>
<%@page import="java.sql.*"%>
<%@page import="org.sqlite.*"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>SyncLite DB Dashboard</title>

<script type="text/javascript">

function autoRefreshSetTimeout() {
    const refreshInterval = parseInt(document.getElementById("refresh-interval").value);

    if (!isNaN(refreshInterval)) {
    	const val = refreshInterval * 1000;
    	if (val === 0) {
    		const timeoutObj = setTimeout("autoRefresh()", 1000);
    		clearTimeout(timeoutObj);
    	} else {
    		setTimeout("autoRefresh()", val);
    	}
	}
}

function autoRefresh() {
	document.forms['dashboardForm'].submit();
}

</script>
</head>

<body onload="autoRefreshSetTimeout()">
	<%@include file="html/menu.html"%>
	<div class="main">
		<h2>SyncLite DB Dashboard</h2>
		<%
		String errorMsg = request.getParameter("errorMsg");
		if (errorMsg != null && !errorMsg.isBlank()) {
			String safeErrorMsg = errorMsg.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
			out.println("<h4 style=\"color: red;\">" + safeErrorMsg + "</h4>");
		}

		if ((session.getAttribute("job-status") == null) || (session.getAttribute("db-root") == null)
				|| session.getAttribute("db-root").toString().isBlank()) {
			out.println("<h4 style=\"color: red;\">Please configure and start or load a SyncLite DB job to view dashboard statistics.</h4>");
			throw new javax.servlet.jsp.SkipPageException();
		}

 		Path dbRoot = Path.of(session.getAttribute("db-root").toString());

		Path statsFilePath = DBWebSupport.getMetadataDbPath(dbRoot);
		if (!Files.exists(statsFilePath)) {
			out.println("<h4 style=\"color: red;\">Please configure and start or load a SyncLite DB job to view dashboard statistics.</h4>");
			throw new javax.servlet.jsp.SkipPageException();
		}
		%>

		<center>
			<table>
				<tbody>
					<%
					int refreshInterval = 5;
					if (request.getParameter("refresh-interval") != null) {
						try {
							refreshInterval = Integer.valueOf(request.getParameter("refresh-interval"));
						} catch (Exception e) {
							refreshInterval = 5;
						}
					}

					long currentJobPID = 0;
					Path configPath = dbRoot.resolve("synclite_db.conf").toAbsolutePath().normalize();
					try {
						String javaHome = System.getenv("JAVA_HOME");
						String scriptPath;
						if (javaHome != null && !javaHome.isBlank()) {
							scriptPath = javaHome + (System.getProperty("os.name").startsWith("Windows") ? "\\bin\\jps" : "/bin/jps");
						} else {
							scriptPath = "jps";
						}

						String[] cmdArray = {scriptPath, "-l", "-m"};
						Process jpsProc = Runtime.getRuntime().exec(cmdArray);
						BufferedReader stdout = new BufferedReader(new InputStreamReader(jpsProc.getInputStream()));
						String line = stdout.readLine();
						String normalizedConfigPath = configPath.toString().replace('\\', '/').toLowerCase();
						while (line != null) {
							String normalizedLine = line.replace('\\', '/').toLowerCase();
							if (normalizedLine.contains("com.synclite.db.main") && normalizedLine.contains(normalizedConfigPath)) {
								currentJobPID = Long.valueOf(line.split(" ")[0]);
								break;
							}
							line = stdout.readLine();
						}
					} catch (Exception ignored) {
						currentJobPID = 0;
					}

					String bindAddress = "127.0.0.1";
					String port = "5555";
					String numThreads = "4";
					if (Files.exists(configPath)) {
						try (BufferedReader confReader = Files.newBufferedReader(configPath)) {
							String confLine = confReader.readLine();
							while (confLine != null) {
								String trimmed = confLine.trim();
								if (!trimmed.isEmpty() && !trimmed.startsWith("#") && trimmed.contains("=")) {
									String key = trimmed.substring(0, trimmed.indexOf('=')).trim().toLowerCase();
									String value = trimmed.substring(trimmed.indexOf('=') + 1).trim();
									if (key.equals("bind-address")) {
										bindAddress = value;
									} else if (key.equals("port")) {
										port = value;
									} else if (key.equals("num-threads")) {
										numThreads = value;
									}
								}
								confLine = confReader.readLine();
							}
						} catch (Exception ignored) {
						}
					}

					Class.forName("org.sqlite.JDBC");
					try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + statsFilePath);
						 Statement stat = conn.createStatement()) {

						ResultSet rs = null;
						try {
							rs = stat.executeQuery("SELECT uptime_ms, request_count, request_rate, open_connections, open_resultsets, database_count, last_heartbeat_time FROM statistics ORDER BY last_heartbeat_time DESC LIMIT 1");
						} catch (SQLException e) {
							if (e.getMessage() == null || !e.getMessage().toLowerCase().contains("no such table")) {
								throw e;
							}
							rs = stat.executeQuery("SELECT uptime_ms, request_count, request_rate, open_connections, open_resultsets, database_count, last_heartbeat_time FROM dashboard ORDER BY last_heartbeat_time DESC LIMIT 1");
						}

						if (rs.next()) {
							String processStatus = currentJobPID > 0 ? "RUNNING" : "STOPPED";
							long uptimeMs = rs.getLong("uptime_ms");
							long uptimeSeconds = uptimeMs / 1000L;
							long days = uptimeSeconds / 86400L;
							long hours = (uptimeSeconds % 86400L) / 3600L;
							long minutes = (uptimeSeconds % 3600L) / 60L;
							long seconds = uptimeSeconds % 60L;

							StringBuilder elapsed = new StringBuilder();
							if (days > 0) {
								elapsed.append(days).append("d ");
							}
							if (hours > 0 || days > 0) {
								elapsed.append(hours).append("h ");
							}
							if (minutes > 0 || hours > 0 || days > 0) {
								elapsed.append(minutes).append("m ");
							}
							elapsed.append(seconds).append("s");

							long heartbeatMillis = rs.getLong("last_heartbeat_time");
							String heartbeatStr = LocalDateTime.ofInstant(Instant.ofEpochMilli(heartbeatMillis), ZoneId.systemDefault()).toString().replace("T", " ");

							out.println("<tr>");
							out.println("<td>SyncLite DB</td>");
							out.println("<td>");
							out.println("<form name=\"dashboardForm\" method=\"post\" action=\"dashboard.jsp\">");
							out.println("<div class=\"pagination\">");
							out.println("REFRESH IN ");
							out.println("<input type=\"text\" id=\"refresh-interval\" name=\"refresh-interval\" value=\"" + refreshInterval + "\" size=\"1\" onchange=\"autoRefreshSetTimeout()\">");
							out.println(" SECONDS");
							out.println("</div>");
							out.println("</form>");
							out.println("</td>");
							out.println("</tr>");

							out.println("<tr><td>SyncLiteDB Process ID</td><td>" + (currentJobPID > 0 ? String.valueOf(currentJobPID) : "-") + "</td></tr>");
							out.println("<tr><td>Server Status</td><td>" + processStatus + "</td></tr>");
							out.println("<tr><td>DB Root Directory</td><td>" + dbRoot + "</td></tr>");
							out.println("<tr><td>Bind Address</td><td>" + bindAddress + "</td></tr>");
							out.println("<tr><td>Port</td><td>" + port + "</td></tr>");
							out.println("<tr><td>Number of Threads</td><td>" + numThreads + "</td></tr>");
							out.println("<tr><td>Server Uptime</td><td>" + elapsed.toString() + "</td></tr>");
							out.println("<tr><td>Database Count</td><td><a href=\"devices.jsp\">" + rs.getLong("database_count") + "</a></td></tr>");
							out.println("<tr><td>Total Requests</td><td>" + rs.getLong("request_count") + "</td></tr>");
							out.println("<tr><td>Request Rate</td><td>" + String.format("%.2f", rs.getDouble("request_rate")) + " req/sec</td></tr>");
							out.println("<tr><td>Open Connections</td><td>" + rs.getLong("open_connections") + "</td></tr>");
							out.println("<tr><td>Open Result Sets</td><td>" + rs.getLong("open_resultsets") + "</td></tr>");
							out.println("<tr><td>Last Heartbeat</td><td>" + heartbeatStr + "</td></tr>");
						} else {
							out.println("<h4 style=\"color: red;\">Please configure and start or load a SyncLite DB job to view dashboard statistics.</h4>");
						}
						if (rs != null) {
							rs.close();
						}
					} catch (Exception e) {
						out.println("<h4 style=\"color: red;\">Failed to read SyncLite DB statistics. Please refresh the page.</h4>");
					}
					%>
				</tbody>
			</table>
		</center>
	</div>
</body>
</html>
