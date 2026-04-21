package com.synclite.db;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

final class ServerRuntime {

	private ServerRuntime() {
	}

	static void initDB() {
		try {
			initPaths();
			if (Main.dbConfigFilePath == null) {
				createDefaultDBConf();
			}
			ConfLoader.getInstance().loadDBConfigProperties(Main.dbConfigFilePath);
			createDefaultSyncLiteLoggerConf();
			initLogger();
			dumpHeader();
		} catch (Exception e) {
			error(new Exception("Error initializing database : " + e.getMessage(), e));
		}
	}

	private static void initPaths() throws SQLException {
		Main.dbDir = Path.of(System.getProperty("user.home"), "synclite", "job1", "db");
		Main.stageDir = Path.of(System.getProperty("user.home"), "synclite", "job1", "stageDir");
		try {
			Files.createDirectories(Main.dbDir);
		} catch (IOException e) {
			throw new SQLException("Failed to create default db directory : " + Main.dbDir, e);
		}

		try {
			Files.createDirectories(Main.stageDir);
		} catch (IOException e) {
			throw new SQLException("Failed to create default stage directory : " + Main.stageDir, e);
		}
	}

	private static void createDefaultDBConf() throws SQLException {
		String currentDirectory = System.getProperty("user.dir");
		Main.dbConfigFilePath = Path.of(currentDirectory, "synclite_db.conf");
		if (Files.exists(Main.dbConfigFilePath)) {
			return;
		}

		StringBuilder confBuilder = new StringBuilder();
		String newLine = System.getProperty("line.separator");

		confBuilder.append("#==============SyncLiteDB Configurations==================");
		confBuilder.append(newLine);
		confBuilder.append("port=").append("5555");
		confBuilder.append(newLine);
		confBuilder.append("num-threads=4");
		confBuilder.append(newLine);
		confBuilder.append("idle-connection-timeout-ms=30000");
		confBuilder.append(newLine);
		confBuilder.append("bind-address=127.0.0.1");
		confBuilder.append(newLine);
		confBuilder.append("max-request-size-bytes=1048576");
		confBuilder.append(newLine);
		confBuilder.append("#auth-token=");
		confBuilder.append(newLine);
		confBuilder.append("enable-app-auth=false");
		confBuilder.append(newLine);
		confBuilder.append("app-auth-timestamp-skew-ms=300000");
		confBuilder.append(newLine);
		confBuilder.append("app-auth-nonce-ttl-ms=600000");
		confBuilder.append(newLine);
		confBuilder.append("app-auth-nonce-cache-max-entries=10000");
		confBuilder.append(newLine);
		confBuilder.append("resultset-pagination-size=1000");
		confBuilder.append(newLine);
		confBuilder.append("resultset-handle-timeout-ms=300000");
		confBuilder.append(newLine);
		confBuilder.append("#authorized-apps=app1,app2");
		confBuilder.append(newLine);
		confBuilder.append("#app.app1.secret=replace-with-long-random-secret");
		confBuilder.append(newLine);
		confBuilder.append("#app.app1.allowed-ops=initialize,begin,commit,rollback,select,execute,next,close");
		confBuilder.append(newLine);
		confBuilder.append("#app.app2.secret=replace-with-long-random-secret");
		confBuilder.append(newLine);
		confBuilder.append("#app.app2.allowed-ops=select,next,execute");
		confBuilder.append(newLine);
		confBuilder.append("trace-directory=").append(currentDirectory);
		confBuilder.append(newLine);
		confBuilder.append("trace-level=INFO");
		String confStr = confBuilder.toString();

		try {
			Files.writeString(Main.dbConfigFilePath, confStr);
		} catch (IOException e) {
			throw new SQLException("Failed to create a default SyncLiteDB configuration file : " + Main.dbConfigFilePath, e);
		}
	}

	private static void createDefaultSyncLiteLoggerConf() throws SQLException {
		Path confPath = Main.dbDir.resolve("synclite_logger.conf");
		if (Files.exists(confPath)) {
			return;
		}

		StringBuilder confBuilder = new StringBuilder();
		String newLine = System.getProperty("line.separator");

		confBuilder.append("#==============Device Stage Properties==================");
		confBuilder.append(newLine);
		confBuilder.append("local-data-stage-directory=").append(Main.stageDir);
		confBuilder.append(newLine);
		confBuilder.append("#local-data-stage-directory=<path/to/local/stage/directory>");
		confBuilder.append(newLine);
		confBuilder.append("destination-type=FS");
		confBuilder.append(newLine);
		confBuilder.append("#destination-type=<FS|MS_ONEDRIVE|GOOGLE_DRIVE|SFTP|MINIO|KAFKA|S3>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============SFTP Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#sftp:host=<host name of remote host for shipping device log files>");
		confBuilder.append(newLine);
		confBuilder.append("#sftp:user-name=<user name to connect to remote host>");
		confBuilder.append(newLine);
		confBuilder.append("#sftp:password=<password>");
		confBuilder.append(newLine);
		confBuilder.append("#sftp:remote-data-stage-directory=<remote data directory name which will host the device directory>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============MinIO  Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#minio:endpoint=<MinIO endpoint to upload devices>");
		confBuilder.append(newLine);
		confBuilder.append("#minio:bucket-name=<MinIO bucket name>");
		confBuilder.append(newLine);
		confBuilder.append("#minio:access-key=<MinIO access key>");
		confBuilder.append(newLine);
		confBuilder.append("#minio:secret-key=<MinIO secret key>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============S3 Configuration=====================");
		confBuilder.append(newLine);
		confBuilder.append("#s3:endpoint=https://s3-<region>.amazonaws.com");
		confBuilder.append(newLine);
		confBuilder.append("#s3:bucket-name=<S3 bucket name>");
		confBuilder.append(newLine);
		confBuilder.append("#s3:access-key=<S3 access key>");
		confBuilder.append(newLine);
		confBuilder.append("#s3:secret-key=<S3 secret key>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Kafka Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#kafka:bootstrap.servers=localhost:9092,localhost:9093,localhost:9094");
		confBuilder.append(newLine);
		confBuilder.append("#kafka:<any_other_kafka_producer_property> = <kafka_producer_property_value>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Table filtering Configuration=================");
		confBuilder.append(newLine);
		confBuilder.append("#include-tables=<comma separate table list>");
		confBuilder.append(newLine);
		confBuilder.append("#exclude-tables=<comma separate table list>");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Logger Configuration==================");
		confBuilder.append("#log-queue-size=2147483647");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-flush-batch-size=1000000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-switch-log-count-threshold=1000000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-switch-duration-threshold-ms=5000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-shipping-frequency-ms=5000");
		confBuilder.append(newLine);
		confBuilder.append("#log-segment-page-size=4096");
		confBuilder.append(newLine);
		confBuilder.append("#log-max-inlined-arg-count=16");
		confBuilder.append(newLine);
		confBuilder.append("#use-precreated-data-backup=false");
		confBuilder.append(newLine);
		confBuilder.append("#vacuum-data-backup=true");
		confBuilder.append(newLine);
		confBuilder.append("#skip-restart-recovery=false");
		confBuilder.append(newLine);
		confBuilder.append(newLine);
		confBuilder.append("#==============Device Configuration==================");
		confBuilder.append(newLine);
		String deviceEncryptionKeyFile = Path.of(System.getProperty("user.home"), ".ssh", "synclite_public_key.der").toString();
		confBuilder.append("#device-encryption-key-file=" + deviceEncryptionKeyFile);
		confBuilder.append(newLine);
		confBuilder.append("#device-name=");
		confBuilder.append(newLine);

		String confStr = confBuilder.toString();

		try {
			Files.writeString(confPath, confStr);
		} catch (IOException e) {
			throw new SQLException("Failed to create a default SyncLite logger configuration file : " + confPath, e);
		}
	}

	static void error(Exception e) {
		System.out.println("ERROR : " + e.getMessage());
		System.exit(1);
	}

	static void usage() {
		if (isWindows()) {
			System.out.println("Usage:");
			System.out.println();
			System.out.println("synclite-db.bat");
			System.out.println();
			System.out.println("synclite-db.bat --config <path/to/config-file>");
		} else {
			System.out.println("Usage:");
			System.out.println();
			System.out.println("synclite-db.sh");
			System.out.println();
			System.out.println("synclite-db.sh --config <path/to/config-file>");
		}
		System.exit(1);
	}

	private static boolean isWindows() {
		String osName = System.getProperty("os.name").toLowerCase();
		return osName.contains("win");
	}

	private static void dumpHeader() {
		ClassLoader classLoader = Main.class.getClassLoader();
		String version = "UNKNOWN";
		try (InputStream inputStream = classLoader.getResourceAsStream("synclite.version")) {
			if (inputStream != null) {
				Scanner scanner = new Scanner(inputStream, "UTF-8");
				version = scanner.useDelimiter("\\A").next();
			}
		} catch (Exception e) {
		}

		StringBuilder builder = new StringBuilder();

		builder.append("\n");
		builder.append("===================SyncLiteDB " + version + "==========================");
		builder.append("\n");
		builder.append("Starting with configuration");
		builder.append("\n");
		builder.append("port : " + ConfLoader.getInstance().getPort());
		builder.append("\n");
		builder.append("num-threads : " + ConfLoader.getInstance().getNumThreads());
		builder.append("\n");
		builder.append("idle-connection-timeout-ms : " + ConfLoader.getInstance().getIdleConnectionTimeout());
		builder.append("\n");
		builder.append("bind-address : " + ConfLoader.getInstance().getBindAddress());
		builder.append("\n");
		builder.append("max-request-size-bytes : " + ConfLoader.getInstance().getMaxRequestSizeBytes());
		builder.append("\n");
		builder.append("auth-token configured : " + (!ConfLoader.getInstance().getAuthToken().isEmpty()));
		builder.append("\n");
		builder.append("app-auth enabled : " + ConfLoader.getInstance().isAppAuthEnabled());
		builder.append("\n");
		builder.append("authorized-app-count : " + ConfLoader.getInstance().getAuthorizedAppCount());
		builder.append("\n");
		builder.append("trace-level : " + ConfLoader.getInstance().getTraceLevel());
		builder.append("\n");
		builder.append("trace-directory : " + ConfLoader.getInstance().getTraceDirectory());
		builder.append("\n");
		builder.append("========================================================================");
		builder.append("\n");

		System.out.print(builder.toString());
		Main.globalTracer.info(builder.toString());
	}

	private static void initLogger() {
		Main.globalTracer = Logger.getLogger(Main.class);
		Main.globalTracer.setLevel(ConfLoader.getInstance().getTraceLevel());
		RollingFileAppender fa = new RollingFileAppender();
		fa.setName("SyncLiteDBTracer");
		String traceDirectory = ConfLoader.getInstance().getTraceDirectory();
		try {
			Files.createDirectories(Path.of(traceDirectory));
		} catch (IOException e) {
			throw new RuntimeException("Failed to create trace-directory : " + traceDirectory, e);
		}
		fa.setFile(Path.of(traceDirectory, "synclite_db.trace").toString());
		fa.setLayout(new PatternLayout("%d %-5p [%c{1}] [%t] %m%n"));
		fa.setMaxBackupIndex(10);
		fa.setMaxFileSize("10MB");
		fa.setAppend(true);
		fa.activateOptions();
		Main.globalTracer.addAppender(fa);
	}
}
