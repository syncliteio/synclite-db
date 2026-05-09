<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="css/SyncLiteStyle.css">
<title>Configure SyncLite DB Server</title>
<style>
	#configForm table {
		width: 100%;
		table-layout: fixed;
	}

	#configForm td:first-child {
		width: 48%;
	}

	#configForm td:last-child {
		width: 52%;
	}

	#configForm input[type="text"],
	#configForm input[type="number"],
	#configForm select,
	#configForm textarea {
		width: 340px;
		max-width: 100%;
		box-sizing: border-box;
	}

	#configForm .wide-field {
		width: 560px;
	}

	#configForm textarea {
		resize: vertical;
	}

	@media (max-width: 1100px) {
		#configForm td:first-child,
		#configForm td:last-child {
			width: auto;
		}

		#configForm input[type="text"],
		#configForm input[type="number"],
		#configForm select,
		#configForm textarea,
		#configForm .wide-field {
			width: 100%;
		}
	}
</style>
</head>
<body>
<%@include file="html/menu.html"%>

<div class="main">
	<h2>Configure SyncLite DB Server</h2>
	<h4 id="message"></h4>
	<p id="server-status">Loading server configuration...</p>

	<form id="configForm">
		<input type="hidden" id="csrfToken" name="csrfToken" value="<%= session.getAttribute("csrfToken") %>">
		<table>
			<tbody>
				<tr>
					<td>Database Root Directory</td>
					<td><input type="text" id="db-root" name="db-root" class="wide-field" title="Root directory for SyncLite DB. Configuration, statistics, and database files are stored under this path. Default: &lt;userHome&gt;/synclite/job1/db."></td>
				</tr>
				<tr>
					<td>Bind Address</td>
					<td><input type="text" id="bind-address" name="bind-address" title="Network interface address the HTTP/JSON server binds to, for example 127.0.0.1 or 0.0.0.0."></td>
				</tr>
				<tr>
					<td>Port</td>
					<td><input type="number" id="port" name="port" title="TCP port used by SyncLite DB to accept HTTP requests."></td>
				</tr>
				<tr>
					<td>Number of Threads</td>
					<td><input type="number" id="num-threads" name="num-threads" title="Worker thread count used by the Netty request processing pool."></td>
				</tr>
				<tr>
					<td>Idle Connection Timeout (ms)</td>
					<td><input type="number" id="idle-connection-timeout-ms" name="idle-connection-timeout-ms" title="Maximum idle time before open DB connections are closed automatically."></td>
				</tr>
				<tr>
					<td>Max Request Size (bytes)</td>
					<td><input type="number" id="max-request-size-bytes" name="max-request-size-bytes" title="Maximum HTTP payload size accepted for a request body."></td>
				</tr>
				<tr>
					<td>Resultset Pagination Size</td>
					<td><input type="number" id="resultset-pagination-size" name="resultset-pagination-size" title="Default page size returned for paginated resultsets."></td>
				</tr>
				<tr>
					<td>Resultset Handle Timeout (ms)</td>
					<td><input type="number" id="resultset-handle-timeout-ms" name="resultset-handle-timeout-ms" title="How long an open resultset handle stays valid before it is expired."></td>
				</tr>
				<tr>
					<td>Auth Token</td>
					<td><input type="text" id="auth-token" name="auth-token" title="Optional global bearer token accepted by SyncLite DB clients."></td>
				</tr>
				<tr>
					<td>Enable App Auth</td>
					<td>
						<select id="enable-app-auth" name="enable-app-auth" onchange="toggleAppAuthFields()" title="Enables signed per-application authentication using app secrets and operation allow lists.">
							<option value="false">false</option>
							<option value="true">true</option>
						</select>
					</td>
				</tr>
				<tr>
					<td>Authorized Apps</td>
					<td><input type="text" id="authorized-apps" name="authorized-apps" class="wide-field" title="Comma-separated application identifiers allowed when app authentication is enabled."></td>
				</tr>
				<tr>
					<td>App Secrets</td>
					<td>
						<textarea id="authorized-app-secrets" name="authorized-app-secrets" rows="4" cols="60"
							title="One authorized app secret per line using appId=secret format."
							placeholder="salesApp=my-secret-key-123"></textarea>
						<div style="color: #777; font-size: 12px; margin-top: 4px;">
							Format: <code>appId=secret</code><br>
							One app per line
						</div>
					</td>
				</tr>
				<tr>
					<td>App Allowed Operations</td>
					<td>
						<textarea id="authorized-app-allowed-ops" name="authorized-app-allowed-ops" rows="4" cols="60"
							title="Optional per-app allow list. One app per line using appId=op1,op2,... format. If this field is left blank for an authorized app, all supported operations are allowed."
							placeholder="salesApp=initialize,select,execute"></textarea>
						<div style="color: #777; font-size: 12px; margin-top: 4px;">
							Format: <code>appId=op1,op2,op3</code><br>
							Supported ops: <code>initialize, close, begin, commit, rollback, select, execute, next</code>
						</div>
					</td>
				</tr>
				<tr>
					<td>App Auth Timestamp Skew (ms)</td>
					<td><input type="number" id="app-auth-timestamp-skew-ms" name="app-auth-timestamp-skew-ms" title="Maximum clock skew tolerated for signed application requests."></td>
				</tr>
				<tr>
					<td>App Auth Nonce TTL (ms)</td>
					<td><input type="number" id="app-auth-nonce-ttl-ms" name="app-auth-nonce-ttl-ms" title="How long a nonce is retained to prevent replay of signed application requests."></td>
				</tr>
				<tr>
					<td>App Auth Nonce Cache Max Entries</td>
					<td><input type="number" id="app-auth-nonce-cache-max-entries" name="app-auth-nonce-cache-max-entries" title="Maximum number of recently seen nonces retained in memory."></td>
				</tr>
				<tr>
					<td>Trace Level</td>
					<td>
						<select id="trace-level" name="trace-level" title="Logging verbosity for the SyncLite DB server tracer.">
							<option value="INFO">INFO</option>
							<option value="DEBUG">DEBUG</option>
							<option value="TRACE">TRACE</option>
							<option value="WARN">WARN</option>
							<option value="ERROR">ERROR</option>
							<option value="FATAL">FATAL</option>
							<option value="OFF">OFF</option>
							<option value="ALL">ALL</option>
						</select>
					</td>
				</tr>
				<tr>
					<td>JVM Arguments</td>
					<td><input type="text" id="jvm-arguments" name="jvm-arguments" class="wide-field" title="Optional extra JVM options passed through synclite-variables.bat/.sh when the server is started from the UI."></td>
				</tr>
			</tbody>
		</table>
		<center>
			<button type="button" onclick="saveAndStartAndGoDashboard()">Save And Start Server</button>
		</center>
	</form>
</div>

<script type="text/javascript">
	const contextPath = '${pageContext.request.contextPath}';
	const fieldIds = [
		'db-root',
		'bind-address',
		'port',
		'num-threads',
		'idle-connection-timeout-ms',
		'max-request-size-bytes',
		'resultset-pagination-size',
		'resultset-handle-timeout-ms',
		'auth-token',
		'enable-app-auth',
		'authorized-apps',
		'authorized-app-secrets',
		'authorized-app-allowed-ops',
		'app-auth-timestamp-skew-ms',
		'app-auth-nonce-ttl-ms',
		'app-auth-nonce-cache-max-entries',
		'trace-level',
		'jvm-arguments'
	];

	function setMessage(message, isError) {
		const messageEl = document.getElementById('message');
		messageEl.textContent = message;
		messageEl.style.color = isError ? 'red' : 'green';
	}

	function toggleAppAuthFields() {
		const enabled = document.getElementById('enable-app-auth').value === 'true';
		['authorized-apps', 'authorized-app-secrets', 'authorized-app-allowed-ops', 'app-auth-timestamp-skew-ms', 'app-auth-nonce-ttl-ms', 'app-auth-nonce-cache-max-entries']
			.forEach(function(id) {
				document.getElementById(id).disabled = !enabled;
			});
	}

	function applyConfig(config) {
		fieldIds.forEach(function(id) {
			if (Object.prototype.hasOwnProperty.call(config, id)) {
				document.getElementById(id).value = config[id];
			}
		});
		document.getElementById('server-status').textContent = config.running ? 'Server Status : RUNNING' : 'Server Status : STOPPED';
		toggleAppAuthFields();
	}

	function formBody() {
		return new URLSearchParams(new FormData(document.getElementById('configForm')));
	}

	async function loadConfig(showMessage) {
		try {
			const response = await fetch(contextPath + '/api/config');
			const data = await response.json();
			if (!data.result || !data.resultset || data.resultset.length === 0) {
				setMessage(data.message || 'Failed to load configuration', true);
				return;
			}
			applyConfig(data.resultset[0]);
			if (showMessage) {
				setMessage('Configuration loaded successfully', false);
			}
		} catch (error) {
			setMessage('Error loading configuration: ' + error, true);
		}
	}

	async function saveConfiguration(startAfterSave) {
		try {
			const response = await fetch(contextPath + '/saveJobConfiguration', {
				method: 'POST',
				body: formBody()
			});
			const data = await response.json();
			if (!data.result) {
				setMessage(data.message, true);
				return;
			}
			setMessage(data.message, false);
			await loadConfig(false);
			if (startAfterSave) {
				await startServer();
			}
		} catch (error) {
			setMessage('Error saving configuration: ' + error, true);
		}
	}

	async function saveAndStartAndGoDashboard() {
		try {
			const saveResponse = await fetch(contextPath + '/saveJobConfiguration', {
				method: 'POST',
				body: formBody()
			});
			const saveData = await saveResponse.json();
			if (!saveData.result) {
				window.location.href = 'dashboard.jsp?errorMsg=' + encodeURIComponent(saveData.message || 'Failed to save configuration');
				return;
			}

			const startResponse = await fetch(contextPath + '/startServer', {
				method: 'POST',
				body: formBody()
			});
			const startData = await startResponse.json();
			if (!startData.result) {
				window.location.href = 'dashboard.jsp?errorMsg=' + encodeURIComponent(startData.message || 'Failed to start server');
				return;
			}

			window.location.href = 'dashboard.jsp';
		} catch (error) {
			window.location.href = 'dashboard.jsp?errorMsg=' + encodeURIComponent('Error starting server: ' + error);
		}
	}

	async function startServer() {
		try {
			const response = await fetch(contextPath + '/startServer', {
				method: 'POST',
				body: formBody()
			});
			const data = await response.json();
			setMessage(data.message, !data.result);
			await loadConfig(false);
		} catch (error) {
			setMessage('Error starting server: ' + error, true);
		}
	}

	async function stopServer() {
		if (!confirm('Are you sure you want to stop the SyncLite DB Server?')) {
			return;
		}
		try {
			const response = await fetch(contextPath + '/stopServer', {
				method: 'POST',
				body: formBody()
			});
			const data = await response.json();
			setMessage(data.message, !data.result);
			await loadConfig(false);
		} catch (error) {
			setMessage('Error stopping server: ' + error, true);
		}
	}

	window.addEventListener('load', function() { loadConfig(false); });
</script>
</body>
</html>
