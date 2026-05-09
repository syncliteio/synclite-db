<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="css/SyncLiteStyle.css">
<title>SyncLite DB Database Statistics</title>
</head>
<body>
<%@include file="html/menu.html"%>
<div class="main">
    <h2>Database Statistics</h2>
    <h4 id="device-title">-</h4>
    <h4 id="device-error" style="color: red;"></h4>

    <center>
        <table>
            <tbody>
                <tr>
                    <td>Database Name</td>
                    <td id="database-name">-</td>
                </tr>
                <tr>
                    <td>Database Type</td>
                    <td id="database-type">-</td>
                </tr>
                <tr>
                    <td>Database Path</td>
                    <td id="database-path">-</td>
                </tr>
                <tr>
                    <td>Database Uptime</td>
                    <td id="uptime">-</td>
                </tr>
                <tr>
                    <td>Total Requests</td>
                    <td id="requests">-</td>
                </tr>
                <tr>
                    <td>Request Rate</td>
                    <td id="rate">-</td>
                </tr>
                <tr>
                    <td>Open Connections</td>
                    <td id="connections">-</td>
                </tr>
                <tr>
                    <td>Open Result Sets</td>
                    <td id="resultsets">-</td>
                </tr>
                <tr>
                    <td>Last Heartbeat</td>
                    <td id="last-heartbeat">-</td>
                </tr>
                <tr>
                    <td>Last Job Start Time</td>
                    <td id="last-job-start">-</td>
                </tr>
            </tbody>
        </table>
    </center>
</div>

<script>
    const contextPath = '${pageContext.request.contextPath}';

    function setError(message) {
        document.getElementById('device-error').innerText = message || '';
    }

    function formatTimestamp(ms) {
        if (!ms || ms <= 0) {
            return '-';
        }
        return new Date(ms).toLocaleString();
    }

    function formatUptime(ms) {
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);

        if (days > 0) return days + 'd ' + (hours % 24) + 'h';
        if (hours > 0) return hours + 'h ' + (minutes % 60) + 'm';
        if (minutes > 0) return minutes + 'm ' + (seconds % 60) + 's';
        return seconds + 's';
    }

    function getDatabaseName() {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.get('database_name') || '';
    }

    function loadDeviceStats() {
        const databaseName = getDatabaseName();
        if (!databaseName) {
            setError('database_name parameter is missing');
            return;
        }

        document.getElementById('device-title').innerText = databaseName;

        fetch(contextPath + '/api/device-stats?database_name=' + encodeURIComponent(databaseName))
            .then(response => response.json())
            .then(data => {
                if (!data.result) {
                    setError(data.message || 'Failed to load database stats');
                    return;
                }

                const row = data.resultset && data.resultset.length > 0 ? data.resultset[0] : null;
                if (!row) {
                    setError('No database statistics found for: ' + databaseName);
                    return;
                }

                const selectedDbName = row.database_name || databaseName;
                document.getElementById('database-name').innerHTML = '<a href="connect.jsp?database_name=' + encodeURIComponent(selectedDbName) + '">' + selectedDbName + '</a>';
                document.getElementById('database-type').innerText = row.database_type || '-';
                document.getElementById('database-path').innerText = row.database_path || '-';
                document.getElementById('uptime').innerText = formatUptime(row.uptime_ms || 0);
                document.getElementById('requests').innerText = row.request_count || 0;
                document.getElementById('rate').innerText = Number(row.request_rate || 0).toFixed(2) + ' req/sec';
                document.getElementById('connections').innerText = row.open_connections || 0;
                document.getElementById('resultsets').innerText = row.open_resultsets || 0;
                document.getElementById('last-heartbeat').innerText = formatTimestamp(row.last_heartbeat_time);
                document.getElementById('last-job-start').innerText = formatTimestamp(row.last_job_start_time);
            })
            .catch(error => setError('Error fetching database stats: ' + error));
    }

    loadDeviceStats();
</script>
</body>
</html>
