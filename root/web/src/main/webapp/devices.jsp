<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="css/SyncLiteStyle.css">
<title>SyncLite DB Databases</title>
<style>
    #databases-table thead tr {
        background-color: #f2f2f2;
    }

    #devices-body tr:nth-child(odd) {
        background-color: #f2f2f2;
    }

    #devices-body tr:nth-child(even) {
        background-color: #ffffff;
    }
</style>
</head>
<body>
<%@include file="html/menu.html"%>
<div class="main">
    <h2>Databases</h2>
    <h4 id="devices-error" style="color: red;"></h4>

    <div class="pagination" style="margin-bottom: 10px;">
        PAGE SIZE
        <input type="text" id="page-size" value="10" size="2" onchange="applyPageSize()">
        | PAGE
        <span id="page-indicator">1</span>
        OF
        <span id="total-pages">0</span>
        |
        <a href="#" id="prev-link" onclick="goPrevPage(); return false;">Prev</a>
        |
        <a href="#" id="next-link" onclick="goNextPage(); return false;">Next</a>
    </div>

    <div class="container">
        <table id="databases-table">
            <thead>
                <tr>
                    <th onclick="changeSort('database_name')">Database Name</th>
                    <th onclick="changeSort('database_path')">Database Path</th>
                    <th onclick="changeSort('database_type')">Database Type</th>
                    <th onclick="changeSort('request_count')">Request Count</th>
                    <th onclick="changeSort('request_rate')">Request Rate</th>
                    <th onclick="changeSort('open_connections')">Open Connections</th>
                    <th onclick="changeSort('open_resultsets')">Open Result Sets</th>
                    <th onclick="changeSort('last_heartbeat_time')">Last Heartbeat</th>
                </tr>
            </thead>
            <tbody id="devices-body"></tbody>
        </table>
    </div>
</div>

<script>
    const contextPath = '${pageContext.request.contextPath}';
    let page = 1;
    let pageSize = 10;
    let sortBy = 'database_name';
    let sortDir = 'ASC';
    let totalPages = 0;

    function setError(message) {
        document.getElementById('devices-error').innerText = message || '';
    }

    function formatTimestamp(ms) {
        if (!ms || ms <= 0) {
            return '-';
        }
        return new Date(ms).toLocaleString();
    }

    function updatePager() {
        document.getElementById('page-indicator').innerText = String(page);
        document.getElementById('total-pages').innerText = String(totalPages);
        const prevLink = document.getElementById('prev-link');
        const nextLink = document.getElementById('next-link');

        prevLink.className = page > 1 ? '' : 'disabled';
        nextLink.className = page < totalPages ? '' : 'disabled';
    }

    function renderRows(rows) {
        const body = document.getElementById('devices-body');
        body.innerHTML = '';

        if (!rows || rows.length === 0) {
            const tr = document.createElement('tr');
            const td = document.createElement('td');
            td.colSpan = 8;
            td.innerText = 'No databases found';
            tr.appendChild(td);
            body.appendChild(tr);
            return;
        }

        rows.forEach((row) => {
            const tr = document.createElement('tr');

            const nameTd = document.createElement('td');
            const nameLink = document.createElement('a');
            nameLink.href = 'deviceStats.jsp?database_name=' + encodeURIComponent(row.database_name);
            nameLink.innerText = row.database_name || '-';
            nameTd.appendChild(nameLink);
            tr.appendChild(nameTd);

            const pathTd = document.createElement('td');
            pathTd.innerText = row.database_path || '-';
            tr.appendChild(pathTd);

            const typeTd = document.createElement('td');
            typeTd.innerText = row.database_type || '-';
            tr.appendChild(typeTd);

            const requestCountTd = document.createElement('td');
            requestCountTd.innerText = row.request_count;
            tr.appendChild(requestCountTd);

            const requestRateTd = document.createElement('td');
            requestRateTd.innerText = Number(row.request_rate || 0).toFixed(2) + ' req/sec';
            tr.appendChild(requestRateTd);

            const connTd = document.createElement('td');
            connTd.innerText = row.open_connections;
            tr.appendChild(connTd);

            const rsTd = document.createElement('td');
            rsTd.innerText = row.open_resultsets;
            tr.appendChild(rsTd);

            const hbTd = document.createElement('td');
            hbTd.innerText = formatTimestamp(row.last_heartbeat_time);
            tr.appendChild(hbTd);

            body.appendChild(tr);
        });
    }

    function refreshDevices() {
        setError('');
        const url = contextPath + '/api/devices?page=' + page + '&pageSize=' + pageSize + '&sortBy=' + encodeURIComponent(sortBy) + '&sortDir=' + encodeURIComponent(sortDir);
        fetch(url)
            .then(response => response.json())
            .then(data => {
                if (!data.result) {
                    setError(data.message || 'Failed to load databases');
                    renderRows([]);
                    return;
                }

                totalPages = Number(data.totalPages || 0);
                if (totalPages > 0 && page > totalPages) {
                    page = totalPages;
                    refreshDevices();
                    return;
                }

                renderRows(data.resultset || []);
                updatePager();
            })
            .catch(error => {
                setError('Error fetching databases: ' + error);
                renderRows([]);
                updatePager();
            });
    }

    function goPrevPage() {
        if (page > 1) {
            page -= 1;
            refreshDevices();
        }
    }

    function goNextPage() {
        if (page < totalPages) {
            page += 1;
            refreshDevices();
        }
    }

    function applyPageSize() {
        const value = parseInt(document.getElementById('page-size').value || '10', 10);
        pageSize = (!isNaN(value) && value > 0) ? value : 10;
        page = 1;
        refreshDevices();
    }

    function changeSort(column) {
        if (sortBy === column) {
            sortDir = sortDir === 'ASC' ? 'DESC' : 'ASC';
        } else {
            sortBy = column;
            sortDir = 'ASC';
        }
        page = 1;
        refreshDevices();
    }

    refreshDevices();
</script>
</body>
</html>
