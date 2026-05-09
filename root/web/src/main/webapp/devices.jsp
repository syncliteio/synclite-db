<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ page import="java.sql.*"%>
<%@ page import="java.nio.file.Path"%>
<%@ page import="java.nio.file.Files"%>
<%@ page import="java.net.URLEncoder"%>
<%@ page import="java.time.Instant"%>
<%@ page import="java.time.LocalDateTime"%>
<%@ page import="java.time.ZoneId"%>
<%@ page import="java.time.format.DateTimeFormatter"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="css/SyncLiteStyle.css">
<title>SyncLite DB Databases</title>
</head>

<script type="text/javascript">

    function processSort(sortColumn) {
        if (sortColumn == document.deviceForm.sortColumn.value) {
            if (document.deviceForm.sortOrder.value == "asc") {
                document.deviceForm.sortOrder.value = "desc";
            } else {
                document.deviceForm.sortOrder.value = "asc";
            }
        } else {
            document.deviceForm.sortColumn.value = sortColumn;
            document.deviceForm.sortOrder.value = "asc";
        }
        document.deviceForm.submit();
    }

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
        document.forms['deviceForm'].submit();
    }

</script>
<body onload="autoRefreshSetTimeout()">
<%@include file="html/menu.html"%>
<div class="main">
    <h2>Databases</h2>
    <%
        if ((session.getAttribute("job-status") == null) || (session.getAttribute("db-root") == null)
                || session.getAttribute("db-root").toString().isBlank()) {
            out.println("<h4 style=\"color: red;\">Please configure and start or load a SyncLite DB job to view databases.</h4>");
            throw new javax.servlet.jsp.SkipPageException();
        }

        Path dbRoot = Path.of(session.getAttribute("db-root").toString());
        Path statsFilePath = dbRoot.resolve("synclite_db_metadata.db");
        if (!Files.exists(statsFilePath)) {
            out.println("<h4 style=\"color: red;\">Please configure and start or load a SyncLite DB job to view databases.</h4>");
            throw new javax.servlet.jsp.SkipPageException();
        }

        int refreshInterval = 5;
        if (request.getParameter("refresh-interval") != null) {
            try {
                refreshInterval = Integer.valueOf(request.getParameter("refresh-interval"));
            } catch (Exception e) {
                refreshInterval = 5;
            }
        }

        long numDevicesPerPage = 10L;
        if (request.getParameter("numDevicesPerPage") != null) {
            try {
                numDevicesPerPage = Long.valueOf(request.getParameter("numDevicesPerPage").trim());
                if (numDevicesPerPage <= 0) {
                    numDevicesPerPage = 10L;
                }
            } catch (NumberFormatException e) {
                numDevicesPerPage = 10L;
            }
        }

        Long pageNumber = null;
        if (request.getParameter("pageNumber") != null) {
            try {
                pageNumber = Long.valueOf(request.getParameter("pageNumber").trim());
            } catch (NumberFormatException e) {
                pageNumber = null;
            }
        }

        String sortColumn = "database_name";
        if (request.getParameter("sortColumn") != null) {
            sortColumn = request.getParameter("sortColumn");
        }

        String sortOrder = "asc";
        if (request.getParameter("sortOrder") != null) {
            sortOrder = request.getParameter("sortOrder");
        }

        // Validate sortColumn against whitelist to prevent SQL injection
        java.util.Set<String> validColumns = new java.util.HashSet<>(java.util.Arrays.asList(
            "database_name", "database_path", "database_type", "request_count",
            "request_rate", "open_connections", "open_resultsets", "last_heartbeat_time"
        ));
        if (!validColumns.contains(sortColumn)) {
            sortColumn = "database_name";
        }
        // Validate sortOrder
        if (!"asc".equalsIgnoreCase(sortOrder) && !"desc".equalsIgnoreCase(sortOrder)) {
            sortOrder = "asc";
        }

        long numDevices = 0L;
        Class.forName("org.sqlite.JDBC");
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + statsFilePath)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet countRS = stmt.executeQuery("SELECT count(*) FROM databases")) {
                numDevices = countRS.getLong(1);
            }
        }

        long numPages = numDevices / numDevicesPerPage;
        if ((numDevices % numDevicesPerPage) > 0) {
            numPages += 1;
        }
        if (numPages == 0) {
            numPages = 1L;
        }

        long prevPageNumber;
        long nextPageNumber;
        if (pageNumber == null) {
            pageNumber = 1L;
            prevPageNumber = 1L;
            nextPageNumber = 2L;
        } else {
            if (pageNumber > numPages) {
                pageNumber = numPages;
            }
            if (pageNumber <= 0) {
                pageNumber = 1L;
            }
            prevPageNumber = pageNumber - 1;
            nextPageNumber = pageNumber + 1;
        }
        if (nextPageNumber > numPages) {
            nextPageNumber = numPages;
        }
        if (prevPageNumber <= 0) {
            prevPageNumber = 1L;
        }

        long numDevicesOnThisPage = numDevicesPerPage;
        if (pageNumber == numPages) {
            numDevicesOnThisPage = numDevices - ((pageNumber - 1) * numDevicesPerPage);
        }

        long startOffset = (pageNumber - 1) * numDevicesPerPage;
    %>
    <center>
        <form name="deviceForm" id="deviceForm" method="post" action="devices.jsp">
            <input type="hidden" name="sortColumn" id="sortColumn" value="<%=sortColumn%>">
            <input type="hidden" name="sortOrder" id="sortOrder" value="<%=sortOrder%>">
            <table>
                <tr>
                    <td>
                        <div class="pagination">
                            <%
                            if (pageNumber == 1) {
                                out.println("<input type=\"button\" name=\"Previous\" id=\"Previous\" value=\"Previous\" onclick=\"javascript: this.form.pageNumber.value = this.form.pageNumber.value - 1; this.form.submit()\" disabled>");
                            } else {
                                out.println("<input type=\"button\" name=\"Previous\" id=\"Previous\" value=\"Previous\" onclick=\"javascript: this.form.pageNumber.value = this.form.pageNumber.value - 1; this.form.submit()\">");
                            }

                            out.println("PAGE <input type=\"text\" size=2 name=\"pageNumber\" id=\"pageNumber\" value=" + pageNumber + "> OF " + numPages);
                            out.println("<input type=\"button\" name=\"Go\" id=\"Go\" value=\"Go\" onclick=\"this.form.submit()\">");

                            if (pageNumber == numPages) {
                                out.println("<input type=\"button\" name=\"Next\" id=\"Next\" value=\"Next\" onclick=\"javascript: this.form.pageNumber.value = parseInt(this.form.pageNumber.value) + 1; this.form.submit()\" disabled>");
                            } else {
                                out.println("<input type=\"button\" name=\"Next\" id=\"Next\" value=\"Next\" onclick=\"javascript: this.form.pageNumber.value = parseInt(this.form.pageNumber.value) + 1; this.form.submit()\">");
                            }
                            %>
                        </div>
                    </td>
                    <td>
                        <div class="pagination">
                            <input type="text" size=2 name="numDevicesPerPage" id="numDevicesPerPage" value=<%=numDevicesPerPage%>> PER PAGE
                            <input type="button" name="GoPerPage" id="GoPerPage" value="Go" onclick="this.form.submit()">
                        </div>
                    </td>
                    <td></td>
                    <td align="right">
                        <div class="pagination">
                            SHOWING <b><%=numDevicesOnThisPage%></b> OUT OF <b><%=numDevices%></b> DATABASES
                        </div>
                    </td>
                    <td>
                        <div class="pagination">
                            REFRESH IN
                            <input type="text" id="refresh-interval" name="refresh-interval" value="<%=refreshInterval%>" size="1" onchange="autoRefreshSetTimeout()">
                            SECONDS
                        </div>
                    </td>
                </tr>
            </table>
            <table>
                <tbody>
                    <tr>
                        <th onclick="processSort('database_name');">
                            <%
                            if (sortColumn.equals("database_name")) {
                                out.println(sortOrder.equals("asc") ? "Database Name &#9650;" : "Database Name &#9660;");
                            } else {
                                out.println("Database Name");
                            }
                            %>
                        </th>
                        <th onclick="processSort('database_path');">
                            <%
                            if (sortColumn.equals("database_path")) {
                                out.println(sortOrder.equals("asc") ? "Database Path &#9650;" : "Database Path &#9660;");
                            } else {
                                out.println("Database Path");
                            }
                            %>
                        </th>
                        <th onclick="processSort('database_type');">
                            <%
                            if (sortColumn.equals("database_type")) {
                                out.println(sortOrder.equals("asc") ? "Database Type &#9650;" : "Database Type &#9660;");
                            } else {
                                out.println("Database Type");
                            }
                            %>
                        </th>
                        <th onclick="processSort('request_count');">
                            <%
                            if (sortColumn.equals("request_count")) {
                                out.println(sortOrder.equals("asc") ? "Request Count &#9650;" : "Request Count &#9660;");
                            } else {
                                out.println("Request Count");
                            }
                            %>
                        </th>
                        <th onclick="processSort('request_rate');">
                            <%
                            if (sortColumn.equals("request_rate")) {
                                out.println(sortOrder.equals("asc") ? "Request Rate &#9650;" : "Request Rate &#9660;");
                            } else {
                                out.println("Request Rate");
                            }
                            %>
                        </th>
                        <th onclick="processSort('open_connections');">
                            <%
                            if (sortColumn.equals("open_connections")) {
                                out.println(sortOrder.equals("asc") ? "Open Connections &#9650;" : "Open Connections &#9660;");
                            } else {
                                out.println("Open Connections");
                            }
                            %>
                        </th>
                        <th onclick="processSort('open_resultsets');">
                            <%
                            if (sortColumn.equals("open_resultsets")) {
                                out.println(sortOrder.equals("asc") ? "Open Result Sets &#9650;" : "Open Result Sets &#9660;");
                            } else {
                                out.println("Open Result Sets");
                            }
                            %>
                        </th>
                        <th onclick="processSort('last_heartbeat_time');">
                            <%
                            if (sortColumn.equals("last_heartbeat_time")) {
                                out.println(sortOrder.equals("asc") ? "Last Heartbeat &#9650;" : "Last Heartbeat &#9660;");
                            } else {
                                out.println("Last Heartbeat");
                            }
                            %>
                        </th>
                    </tr>

                    <%
                    try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + statsFilePath)) {
                        String query = "SELECT database_name, database_path, database_type, request_count, request_rate, open_connections, open_resultsets, last_heartbeat_time FROM databases ORDER BY "
                                + sortColumn + " " + sortOrder + " LIMIT " + startOffset + ", " + numDevicesPerPage;
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(query)) {
                            while (rs.next()) {
                                String dbName = rs.getString("database_name");
                                String encodedDbName = URLEncoder.encode(dbName, "UTF-8");
                                String deviceStatsURL = "deviceStats.jsp?database_name=" + encodedDbName;

                                out.println("<tr>");
                                out.println("<td><a href=\"" + deviceStatsURL + "\">" + dbName + "</a></td>");
                                out.println("<td>" + rs.getString("database_path") + "</td>");
                                out.println("<td>" + rs.getString("database_type") + "</td>");
                                out.println("<td>" + rs.getLong("request_count") + "</td>");
                                out.println("<td>" + String.format("%.2f", rs.getDouble("request_rate")) + " req/sec</td>");
                                out.println("<td>" + rs.getLong("open_connections") + "</td>");
                                out.println("<td>" + rs.getLong("open_resultsets") + "</td>");

                                long hbMs = rs.getLong("last_heartbeat_time");
                                String hbStr = "-";
                                if (hbMs > 0) {
                                    LocalDateTime ldt = LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(hbMs), ZoneId.systemDefault());
                                    hbStr = ldt.format(DateTimeFormatter.ofPattern("M/d/yyyy, h:mm:ss a"));
                                }
                                out.println("<td>" + hbStr + "</td>");
                                out.println("</tr>");
                            }
                        }
                    } catch (Exception e) {
                        out.println("<tr><td colspan=\"8\"><h4 style=\"color: red;\">Failed to read database statistics. Please refresh the page.</h4></td></tr>");
                    }
                    %>
                </tbody>
            </table>
        </form>
    </center>
</div>
</body>
</html>
