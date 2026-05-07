<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<%@ page import="java.nio.file.Path" %>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="css/SyncLiteStyle.css">
<title>Load SyncLite DB Server</title>
</head>
<%
	String errorMsg = request.getParameter("errorMsg");
	String dbRoot = request.getParameter("db-root");
	if (dbRoot == null || dbRoot.trim().isEmpty()) {
		dbRoot = Path.of(System.getProperty("user.home"), "synclite", "db").toString();
	}
%>
<body>
<%@include file="html/menu.html"%>

<div class="main">
	<h2>Load Existing SyncLite DB Server</h2>
	<%
	if (errorMsg != null) {
		String safeErrorMsg = errorMsg.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
		out.println("<h4 style=\"color: red;\">" + safeErrorMsg + "</h4>");
	}
	%>

	<form action="${pageContext.request.contextPath}/loadJob" method="post">
		<input type="hidden" name="csrfToken" value="<%= session.getAttribute("csrfToken") %>">
		<table>
			<tbody>
				<tr>
					<td>Database Root Directory</td>
					<td><input type="text" size="60" id="db-root" name="db-root" value="<%=dbRoot%>" title="Directory that already contains synclite_db.conf for an existing SyncLite DB server."></td>
				</tr>
			</tbody>
		</table>
		<center>
			<button type="submit">Load</button>
		</center>
	</form>
</div>
</body>
</html>