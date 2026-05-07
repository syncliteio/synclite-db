package com.synclite.db.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

public final class DBWebSupport {
	public static final String METADATA_DB_FILE_NAME = "synclite_db_metadata.db";
	public static final String LEGACY_STATISTICS_DB_FILE_NAME = "synclite_db_statistics.db";

	private static final List<String> CONFIG_KEYS = Arrays.asList(
		"bind-address",
		"port",
		"num-threads",
		"idle-connection-timeout-ms",
		"max-request-size-bytes",
		"resultset-pagination-size",
		"resultset-handle-timeout-ms",
		"auth-token",
		"enable-app-auth",
		"authorized-apps",
		"app-auth-timestamp-skew-ms",
		"app-auth-nonce-ttl-ms",
		"app-auth-nonce-cache-max-entries",
		"trace-level",
		"jvm-arguments"
	);

	private static final Set<String> SUPPORTED_OPERATIONS = Set.of(
		"initialize", "close", "begin", "commit", "rollback", "select", "execute", "next"
	);

	private DBWebSupport() {
	}

	static Path getDefaultDbRoot() {
		Path userHomeDefault = Path.of(System.getProperty("user.home"), "synclite", "job1", "db").toAbsolutePath().normalize();
		Path detected = detectDbRootFromKnownLocations(userHomeDefault);
		return detected != null ? detected : userHomeDefault;
	}

	private static Path detectDbRootFromKnownLocations(Path userHomeDefault) {
		Path best = null;
		long bestScore = Long.MIN_VALUE;

		List<Path> candidates = new ArrayList<Path>();
		candidates.add(userHomeDefault);

		if (isWindows()) {
			Path usersDir = Path.of("C:\\Users");
			if (Files.isDirectory(usersDir)) {
				try (DirectoryStream<Path> stream = Files.newDirectoryStream(usersDir)) {
					for (Path userDir : stream) {
						if (Files.isDirectory(userDir)) {
							candidates.add(userDir.resolve("synclite").resolve("job1").resolve("db"));
						}
					}
				} catch (IOException ignored) {
				}
			}
		}

		for (Path candidate : candidates) {
			long score = scoreDbRootCandidate(candidate);
			if (score > bestScore) {
				bestScore = score;
				best = candidate;
			}
		}

		return bestScore >= 0 ? best.toAbsolutePath().normalize() : null;
	}

	private static long scoreDbRootCandidate(Path dbRoot) {
		Path metadataPath = dbRoot.resolve(METADATA_DB_FILE_NAME);
		Path legacyStatsPath = dbRoot.resolve(LEGACY_STATISTICS_DB_FILE_NAME);
		Path configPath = dbRoot.resolve("synclite_db.conf");
		long score = -1;

		try {
			if (Files.exists(metadataPath)) {
				score = Math.max(score, 2_000_000_000_000L + Files.getLastModifiedTime(metadataPath).toMillis());
			}
			if (Files.exists(legacyStatsPath)) {
				score = Math.max(score, 2_000_000_000_000L + Files.getLastModifiedTime(legacyStatsPath).toMillis());
			}
			if (Files.exists(configPath)) {
				score = Math.max(score, 1_000_000_000_000L + Files.getLastModifiedTime(configPath).toMillis());
			}
		} catch (IOException ignored) {
		}

		return score;
	}

	static Path getDbRoot(HttpServletRequest request) {
		HttpSession session = request.getSession(false);
		if (session != null) {
			Object dbRoot = session.getAttribute("db-root");
			if (dbRoot != null && !dbRoot.toString().isBlank()) {
				return Path.of(dbRoot.toString()).toAbsolutePath().normalize();
			}
		}
		return getDefaultDbRoot();
	}

	static Path getDbRoot(String rawDbRoot) {
		if (rawDbRoot == null || rawDbRoot.trim().isEmpty()) {
			return getDefaultDbRoot();
		}
		return Path.of(rawDbRoot.trim()).toAbsolutePath().normalize();
	}

	static Path getConfigPath(Path dbRoot) {
		return dbRoot.resolve("synclite_db.conf");
	}

	public static Path getMetadataDbPath(Path dbRoot) {
		Path metadataPath = dbRoot.resolve(METADATA_DB_FILE_NAME);
		if (Files.exists(metadataPath)) {
			return metadataPath;
		}
		Path legacyStatsPath = dbRoot.resolve(LEGACY_STATISTICS_DB_FILE_NAME);
		if (Files.exists(legacyStatsPath)) {
			return legacyStatsPath;
		}
		return metadataPath;
	}

	static LinkedHashMap<String, String> getDefaultConfig(Path dbRoot) {
		LinkedHashMap<String, String> config = new LinkedHashMap<String, String>();
		config.put("db-root", dbRoot.toString());
		config.put("bind-address", "127.0.0.1");
		config.put("port", "5555");
		config.put("num-threads", "4");
		config.put("idle-connection-timeout-ms", "30000");
		config.put("max-request-size-bytes", "1048576");
		config.put("resultset-pagination-size", "1000");
		config.put("resultset-handle-timeout-ms", "300000");
		config.put("auth-token", "");
		config.put("enable-app-auth", "false");
		config.put("authorized-apps", "");
		config.put("app-auth-timestamp-skew-ms", "300000");
		config.put("app-auth-nonce-ttl-ms", "600000");
		config.put("app-auth-nonce-cache-max-entries", "10000");
		config.put("trace-level", "INFO");
		config.put("jvm-arguments", "");
		config.put("authorized-app-secrets", "");
		config.put("authorized-app-allowed-ops", "");
		return config;
	}

	static LinkedHashMap<String, String> readConfig(Path dbRoot) throws IOException, ServletException {
		Path configPath = getConfigPath(dbRoot);
		LinkedHashMap<String, String> config = getDefaultConfig(dbRoot);
		if (!Files.exists(configPath)) {
			return config;
		}

		Map<String, String> rawConfig = new LinkedHashMap<String, String>();
		try (BufferedReader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
			String line = reader.readLine();
			while (line != null) {
				String trimmed = line.trim();
				if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
					String[] tokens = trimmed.split("=", 2);
					if (tokens.length != 2 || tokens[0].trim().isEmpty()) {
						throw new ServletException("Invalid line in configuration file : " + trimmed);
					}
					rawConfig.put(tokens[0].trim().toLowerCase(), tokens[1].trim());
				}
				line = reader.readLine();
			}
		}

		for (String key : CONFIG_KEYS) {
			if (rawConfig.containsKey(key)) {
				config.put(key, rawConfig.get(key));
			}
		}

		TreeMap<String, String> appSecrets = new TreeMap<String, String>();
		TreeMap<String, String> appAllowedOps = new TreeMap<String, String>();
		for (Map.Entry<String, String> entry : rawConfig.entrySet()) {
			String key = entry.getKey();
			if (key.startsWith("app.") && key.endsWith(".secret")) {
				String appId = key.substring(4, key.length() - ".secret".length());
				appSecrets.put(appId, entry.getValue());
			} else if (key.startsWith("app.") && key.endsWith(".allowed-ops")) {
				String appId = key.substring(4, key.length() - ".allowed-ops".length());
				appAllowedOps.put(appId, entry.getValue());
			}
		}

		config.put("authorized-app-secrets", toAssignmentText(appSecrets));
		config.put("authorized-app-allowed-ops", toAssignmentText(appAllowedOps));
		return config;
	}

	static LinkedHashMap<String, String> validateConfig(HttpServletRequest request) throws ServletException {
		Path dbRoot = getDbRoot(request.getParameter("db-root"));
		LinkedHashMap<String, String> config = getDefaultConfig(dbRoot);
		config.put("db-root", dbRoot.toString());

		config.put("bind-address", requireText(request, "bind-address", "Bind Address", "127.0.0.1"));
		config.put("port", String.valueOf(requirePositiveInt(request, "port", "Port", 5555)));
		config.put("num-threads", String.valueOf(requirePositiveInt(request, "num-threads", "Number of Threads", 4)));
		config.put("idle-connection-timeout-ms", String.valueOf(requirePositiveLong(request, "idle-connection-timeout-ms", "Idle Connection Timeout (ms)", 30000L)));
		config.put("max-request-size-bytes", String.valueOf(requirePositiveLong(request, "max-request-size-bytes", "Max Request Size (bytes)", 1048576L)));
		config.put("resultset-pagination-size", String.valueOf(requirePositiveInt(request, "resultset-pagination-size", "Resultset Pagination Size", 1000)));
		config.put("resultset-handle-timeout-ms", String.valueOf(requirePositiveLong(request, "resultset-handle-timeout-ms", "Resultset Handle Timeout (ms)", 300000L)));
		config.put("auth-token", optionalText(request, "auth-token"));
		config.put("enable-app-auth", String.valueOf(requireBoolean(request, "enable-app-auth", false)));
		config.put("authorized-apps", optionalText(request, "authorized-apps"));
		config.put("app-auth-timestamp-skew-ms", String.valueOf(requirePositiveLong(request, "app-auth-timestamp-skew-ms", "App Auth Timestamp Skew (ms)", 300000L)));
		config.put("app-auth-nonce-ttl-ms", String.valueOf(requirePositiveLong(request, "app-auth-nonce-ttl-ms", "App Auth Nonce TTL (ms)", 600000L)));
		config.put("app-auth-nonce-cache-max-entries", String.valueOf(requirePositiveInt(request, "app-auth-nonce-cache-max-entries", "App Auth Nonce Cache Max Entries", 10000)));
		config.put("trace-level", requireTraceLevel(request, "trace-level", "INFO"));
		config.put("jvm-arguments", optionalText(request, "jvm-arguments"));
		config.put("authorized-app-secrets", optionalText(request, "authorized-app-secrets"));
		config.put("authorized-app-allowed-ops", optionalText(request, "authorized-app-allowed-ops"));

		if (Files.exists(dbRoot) && !Files.isDirectory(dbRoot)) {
			throw new ServletException("Specified \"Database Root Directory\" must be a directory");
		}
		if (Files.exists(dbRoot) && (!dbRoot.toFile().canRead() || !dbRoot.toFile().canWrite())) {
			throw new ServletException("Specified \"Database Root Directory\" must have read and write permission");
		}

		validateAuthorizedApps(config);
		return config;
	}

	static void persistConfig(LinkedHashMap<String, String> config) throws IOException, ServletException {
		Path dbRoot = getDbRoot(config.get("db-root"));
		Files.createDirectories(dbRoot);
		String content = toConfigText(config);
		Files.writeString(getConfigPath(dbRoot), content, StandardCharsets.UTF_8,
			StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
	}

	static void applySessionConfig(HttpSession session, LinkedHashMap<String, String> config) {
		for (Map.Entry<String, String> entry : config.entrySet()) {
			session.setAttribute(entry.getKey(), entry.getValue());
		}
	}

	static long findRunningServerPid(Path configPath) throws IOException {
		Process process = Runtime.getRuntime().exec(buildJpsCommand());
		long pid = 0;
		String normalizedConfigPath = normalizePathForMatch(configPath.toAbsolutePath().normalize().toString());
		try (BufferedReader stdout = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
			String line = stdout.readLine();
			while (line != null) {
				String normalizedLine = normalizePathForMatch(line);
				if (normalizedLine.contains("com.synclite.db.main") && normalizedLine.contains(normalizedConfigPath)) {
					pid = Long.parseLong(line.split(" ")[0]);
					break;
				}
				line = stdout.readLine();
			}
		}
		return pid;
	}

	private static String normalizePathForMatch(String value) {
		return value == null ? "" : value.replace('\\', '/').toLowerCase();
	}

	static void writeJvmArgsFiles(Path libDir, String jvmArgs) throws IOException {
		if (isWindows()) {
			Path varFilePath = libDir.resolve("synclite-variables.bat");
			if (jvmArgs == null || jvmArgs.isBlank()) {
				Files.deleteIfExists(varFilePath);
			} else {
				Files.writeString(varFilePath, "set \"JVM_ARGS=" + jvmArgs.trim() + "\"", StandardCharsets.UTF_8,
					StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
			}
		} else {
			Path varFilePath = libDir.resolve("synclite-variables.sh");
			if (jvmArgs == null || jvmArgs.isBlank()) {
				Files.deleteIfExists(varFilePath);
			} else {
				Files.writeString(varFilePath, "JVM_ARGS=\"" + jvmArgs.trim() + "\"", StandardCharsets.UTF_8,
					StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
				Set<PosixFilePermission> perms = Files.getPosixFilePermissions(varFilePath);
				perms.add(PosixFilePermission.OWNER_EXECUTE);
				Files.setPosixFilePermissions(varFilePath, perms);
			}
		}
	}

	static String getVersion() {
		try (InputStream inputStream = DBWebSupport.class.getClassLoader().getResourceAsStream("synclite.version")) {
			if (inputStream == null) {
				return "UNKNOWN";
			}
			Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name());
			return scanner.useDelimiter("\\A").hasNext() ? scanner.next().trim() : "UNKNOWN";
		} catch (Exception e) {
			return "UNKNOWN";
		}
	}

	static boolean isWindows() {
		return System.getProperty("os.name").toLowerCase().contains("win");
	}

	private static String[] buildJpsCommand() {
		String javaHome = System.getenv("JAVA_HOME");
		String scriptPath = "jps";
		if (javaHome != null && !javaHome.isBlank()) {
			scriptPath = isWindows() ? javaHome + "\\bin\\jps" : javaHome + "/bin/jps";
		}
		return new String[] {scriptPath, "-l", "-m"};
	}

	private static String requireText(HttpServletRequest request, String name, String displayName, String defaultValue) throws ServletException {
		String value = request.getParameter(name);
		if (value == null || value.trim().isEmpty()) {
			value = defaultValue;
		}
		if (value == null || value.trim().isEmpty()) {
			throw new ServletException("\"" + displayName + "\" must be specified");
		}
		return value.trim();
	}

	private static String optionalText(HttpServletRequest request, String name) {
		String value = request.getParameter(name);
		return value == null ? "" : value.trim();
	}

	private static int requirePositiveInt(HttpServletRequest request, String name, String displayName, int defaultValue) throws ServletException {
		String raw = request.getParameter(name);
		if (raw == null || raw.trim().isEmpty()) {
			return defaultValue;
		}
		try {
			int value = Integer.parseInt(raw.trim());
			if (value <= 0) {
				throw new ServletException("\"" + displayName + "\" must be a positive integer");
			}
			return value;
		} catch (NumberFormatException e) {
			throw new ServletException("\"" + displayName + "\" must be a positive integer", e);
		}
	}

	private static long requirePositiveLong(HttpServletRequest request, String name, String displayName, long defaultValue) throws ServletException {
		String raw = request.getParameter(name);
		if (raw == null || raw.trim().isEmpty()) {
			return defaultValue;
		}
		try {
			long value = Long.parseLong(raw.trim());
			if (value <= 0) {
				throw new ServletException("\"" + displayName + "\" must be a positive number");
			}
			return value;
		} catch (NumberFormatException e) {
			throw new ServletException("\"" + displayName + "\" must be a positive number", e);
		}
	}

	private static boolean requireBoolean(HttpServletRequest request, String name, boolean defaultValue) throws ServletException {
		String raw = request.getParameter(name);
		if (raw == null || raw.trim().isEmpty()) {
			return defaultValue;
		}
		String normalized = raw.trim().toLowerCase();
		if (!"true".equals(normalized) && !"false".equals(normalized)) {
			throw new ServletException("\"" + name + "\" must be true or false");
		}
		return Boolean.parseBoolean(normalized);
	}

	private static String requireTraceLevel(HttpServletRequest request, String name, String defaultValue) throws ServletException {
		String value = optionalText(request, name);
		if (value.isEmpty()) {
			value = defaultValue;
		}
		String normalized = value.toUpperCase();
		List<String> supportedLevels = Arrays.asList("ALL", "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF");
		if (!supportedLevels.contains(normalized)) {
			throw new ServletException("\"Trace Level\" must be one of " + supportedLevels);
		}
		return normalized;
	}

	private static void validateAuthorizedApps(LinkedHashMap<String, String> config) throws ServletException {
		boolean enableAppAuth = Boolean.parseBoolean(config.get("enable-app-auth"));
		String authorizedApps = config.get("authorized-apps");
		Map<String, String> secrets = parseAssignments(config.get("authorized-app-secrets"));
		Map<String, String> allowedOps = parseAssignments(config.get("authorized-app-allowed-ops"));

		if (!enableAppAuth) {
			return;
		}

		if (authorizedApps == null || authorizedApps.isBlank()) {
			throw new ServletException("\"Authorized Apps\" must be specified when app authentication is enabled");
		}

		for (String rawAppId : authorizedApps.split(",")) {
			String appId = rawAppId.trim().toLowerCase();
			if (appId.isEmpty()) {
				continue;
			}
			if (!secrets.containsKey(appId) || secrets.get(appId).isBlank()) {
				throw new ServletException("Missing secret for authorized app : " + appId);
			}
			if (allowedOps.containsKey(appId)) {
				for (String rawOperation : allowedOps.get(appId).split(",")) {
					String operation = rawOperation.trim().toLowerCase();
					if (!operation.isEmpty() && !SUPPORTED_OPERATIONS.contains(operation)) {
						throw new ServletException("Invalid allowed operation for app " + appId + " : " + operation);
					}
				}
			}
		}
	}

	private static String toConfigText(LinkedHashMap<String, String> config) throws ServletException {
		String lineSeparator = System.lineSeparator();
		StringBuilder builder = new StringBuilder();
		builder.append("#==============SyncLite DB Configurations==================").append(lineSeparator);
		for (String key : CONFIG_KEYS) {
			if ("jvm-arguments".equals(key)) {
				continue;
			}
			String value = config.get(key);
			if (value != null && !value.isBlank()) {
				builder.append(key).append("=").append(value).append(lineSeparator);
			}
		}
		String jvmArguments = config.get("jvm-arguments");
		if (jvmArguments != null && !jvmArguments.isBlank()) {
			builder.append("jvm-arguments=").append(jvmArguments).append(lineSeparator);
		}

		Map<String, String> secrets = parseAssignments(config.get("authorized-app-secrets"));
		Map<String, String> allowedOps = parseAssignments(config.get("authorized-app-allowed-ops"));
		String authorizedApps = config.get("authorized-apps");
		if (authorizedApps != null && !authorizedApps.isBlank()) {
			for (String rawAppId : authorizedApps.split(",")) {
				String appId = rawAppId.trim().toLowerCase();
				if (appId.isEmpty()) {
					continue;
				}
				String secret = secrets.get(appId);
				if (secret != null && !secret.isBlank()) {
					builder.append("app.").append(appId).append(".secret=").append(secret).append(lineSeparator);
				}
				String ops = allowedOps.get(appId);
				if (ops != null && !ops.isBlank()) {
					builder.append("app.").append(appId).append(".allowed-ops=").append(ops).append(lineSeparator);
				}
			}
		}
		return builder.toString();
	}

	private static Map<String, String> parseAssignments(String rawText) throws ServletException {
		if (rawText == null || rawText.isBlank()) {
			return Collections.emptyMap();
		}

		Map<String, String> values = new HashMap<String, String>();
		String[] lines = rawText.split("\\r?\\n");
		for (String rawLine : lines) {
			String line = rawLine.trim();
			if (line.isEmpty() || line.startsWith("#")) {
				continue;
			}
			String[] tokens = line.split("=", 2);
			if (tokens.length != 2 || tokens[0].trim().isEmpty()) {
				throw new ServletException("Invalid assignment line : " + line + ". Use appId=value format.");
			}
			values.put(tokens[0].trim().toLowerCase(), tokens[1].trim());
		}
		return values;
	}

	private static String toAssignmentText(Map<String, String> values) {
		if (values.isEmpty()) {
			return "";
		}
		List<String> lines = new ArrayList<String>();
		for (Map.Entry<String, String> entry : values.entrySet()) {
			lines.add(entry.getKey() + "=" + entry.getValue());
		}
		return String.join(System.lineSeparator(), lines);
	}
}