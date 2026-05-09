<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="css/SyncLiteStyle.css">
<title>Execute SQL</title>
<style>
    .connect-wrap {
        height: calc(100vh - 110px);
        display: flex;
        flex-direction: column;
        gap: 8px;
    }

    .connect-layout {
        flex: 1;
        display: flex;
        gap: 16px;
        align-items: stretch;
    }

    .json-panel {
        flex: 1;
        display: flex;
        flex-direction: column;
        min-width: 0;
    }

    .section-label {
        font-weight: 600;
        margin: 0 0 8px 0;
    }

    .json-editor {
        width: 100%;
        flex: 1;
        font-family: Consolas, monospace;
        font-size: 13px;
        overflow: auto;
        box-sizing: border-box;
        resize: none;
    }

    .button-column {
        width: 140px;
        display: flex;
        align-items: center;
        justify-content: center;
    }

    .json-pretty {
        width: 100%;
        flex: 1;
        overflow: auto;
        border: 1px solid #ccc;
        background: #fafafa;
        padding: 8px;
        box-sizing: border-box;
        font-family: Consolas, monospace;
        font-size: 12px;
        white-space: pre-wrap;
        word-break: break-word;
    }

    .json-key { color: #8b0000; }
    .json-string { color: #0b7500; }
    .json-number { color: #1f4fb2; }
    .json-boolean { color: #7a3db8; }
    .json-null { color: #777; }

    @media (max-width: 1000px) {
        .connect-layout {
            flex-direction: column;
        }

        .button-column {
            width: 100%;
            min-height: 48px;
        }

        .json-panel {
            min-height: 240px;
        }
    }
</style>
</head>
<body>
<%@include file="html/menu.html"%>
<div class="main">
    <h2>Execute SQL</h2>
    <h4 id="connect-error" style="color: red;"></h4>

    <div class="connect-wrap">
        <div class="connect-layout">
            <div class="json-panel">
                <div class="section-label">Request JSON</div>
                <textarea id="request-json" class="json-editor"></textarea>
            </div>

            <div class="button-column">
                <button type="button" onclick="executeRequest()">Execute</button>
            </div>

            <div class="json-panel">
                <div class="section-label">Response JSON</div>
                <div id="response-color" class="json-pretty">{}</div>
            </div>
        </div>
    </div>
</div>

<script>
    const contextPath = '${pageContext.request.contextPath}';
    const requestEditor = document.getElementById('request-json');
    const responseColor = document.getElementById('response-color');

    function setError(message) {
        document.getElementById('connect-error').innerText = message || '';
    }

    function getDefaultRequestJson(stageDir, databaseName, databaseType, loggerOptions) {
        const effectiveDbName = databaseName || 'testdb';
        const effectiveDbType = databaseType || 'SQLITE';
        const effectiveLoggerOptions = loggerOptions && Object.keys(loggerOptions).length > 0
            ? loggerOptions
            : {
                "local-data-stage-directory": stageDir,
                "destination-type": "FS"
            };

        const payload = {
            "db-type": effectiveDbType,
            "db-name": effectiveDbName,
            "synclite-logger-options": effectiveLoggerOptions,
            "sql": "select sqlite_version()"
        };
        return JSON.stringify(payload, null, 2);
    }

    function getQueryParam(name) {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.get(name) || '';
    }

    function escapeHtml(text) {
        return (text || '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    function colorizeJson(text) {
        const escaped = escapeHtml(text);
        return escaped.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\\"])*"\s*:)|("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\\"])*")|\b(true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?/g,
            function (match) {
                if (/"\s*:$/.test(match)) {
                    return '<span class="json-key">' + match + '</span>';
                }
                if (/^"/.test(match)) {
                    return '<span class="json-string">' + match + '</span>';
                }
                if (/true|false/.test(match)) {
                    return '<span class="json-boolean">' + match + '</span>';
                }
                if (/null/.test(match)) {
                    return '<span class="json-null">' + match + '</span>';
                }
                return '<span class="json-number">' + match + '</span>';
            });
    }

    function refreshColorPreview(inputText, target) {
        try {
            const pretty = JSON.stringify(JSON.parse(inputText || '{}'), null, 2);
            target.innerHTML = colorizeJson(pretty);
        } catch (e) {
            target.innerText = inputText || '';
        }
    }

    function toUnixSlashes(pathValue) {
        return (pathValue || '').replace(/\\/g, '/');
    }

    function buildStageDirFromDbRoot(dbRoot) {
        const normalized = toUnixSlashes(dbRoot).replace(/\/+$/, '');
        const idx = normalized.lastIndexOf('/');
        const parent = idx > 0 ? normalized.substring(0, idx) : normalized;
        return (parent || normalized) + '/stageDir';
    }

    async function initDefaultRequest() {
        const selectedDbName = getQueryParam('database_name');
        let dbRoot = '';
        try {
            const resp = await fetch(contextPath + '/api/config');
            const data = await resp.json();
            const row = data.resultset && data.resultset.length > 0 ? data.resultset[0] : null;
            if (row && row['db-root']) {
                dbRoot = row['db-root'];
            }
        } catch (e) {
            dbRoot = '';
        }

        if (!dbRoot) {
            dbRoot = 'C:/Users/<user>/synclite/job1/db';
        }

        const stageDir = buildStageDirFromDbRoot(dbRoot);

        if (selectedDbName) {
            try {
                const statsResp = await fetch(contextPath + '/api/device-stats?database_name=' + encodeURIComponent(selectedDbName));
                const statsData = await statsResp.json();
                const row = statsData.resultset && statsData.resultset.length > 0 ? statsData.resultset[0] : null;
                if (row) {
                    let loggerOptions = null;
                    if (row.logger_options_json) {
                        try {
                            loggerOptions = JSON.parse(row.logger_options_json);
                        } catch (e) {
                            loggerOptions = null;
                        }
                    }
                    requestEditor.value = getDefaultRequestJson(stageDir, selectedDbName, row.database_type || 'SQLITE', loggerOptions);
                    return;
                }
            } catch (e) {
                // Fall through to generic default payload.
            }
        }

        requestEditor.value = getDefaultRequestJson(stageDir);
    }

    async function executeRequest() {
        setError('');
        refreshColorPreview('{}', responseColor);

        const payload = requestEditor.value || '';
        if (!payload.trim()) {
            setError('Please provide request JSON.');
            return;
        }

        try {
            JSON.parse(payload);
        } catch (e) {
            setError('Request JSON is invalid: ' + e.message);
            return;
        }

        try {
            const resp = await fetch(contextPath + '/api/connect/execute', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json; charset=UTF-8'
                },
                body: payload
            });

            const data = await resp.json();
            if (!data.result) {
                setError(data.message || 'Execution failed');
                refreshColorPreview('{}', responseColor);
                return;
            }

            refreshColorPreview(data['response-json'] || '{}', responseColor);
        } catch (error) {
            setError('Failed to execute request: ' + error);
            refreshColorPreview('{}', responseColor);
        }
    }

    initDefaultRequest();
</script>
</body>
</html>
