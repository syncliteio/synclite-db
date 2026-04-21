package com.synclite.db;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import io.netty.handler.codec.http.HttpRequest;

public final class RequestAuth {

	private static ConcurrentHashMap<String, Long> observedNonces = new ConcurrentHashMap<String, Long>();

	private RequestAuth() {
	}

	public static boolean isAuthorized(HttpRequest request) {
		String configuredToken = ConfLoader.getInstance().getAuthToken();
		if (configuredToken == null || configuredToken.isEmpty()) {
			return true;
		}

		String requestToken = request.headers().get("X-SyncLite-Token");
		if (requestToken == null) {
			return false;
		}

		return MessageDigest.isEqual(
			configuredToken.getBytes(StandardCharsets.UTF_8),
			requestToken.getBytes(StandardCharsets.UTF_8)
		);
	}

	public static String getRequesterAppId(HttpRequest request) {
		if (!ConfLoader.getInstance().isAppAuthEnabled()) {
			return null;
		}

		String appId = request.headers().get("X-SyncLite-App-Id");
		if (appId == null) {
			return null;
		}

		appId = appId.trim().toLowerCase();
		if (appId.isEmpty()) {
			return null;
		}
		return appId;
	}

	public static String getRequesterPrincipal(HttpRequest request) {
		String appId = getRequesterAppId(request);
		if (appId != null) {
			return "app:" + appId;
		}

		if (!ConfLoader.getInstance().getAuthToken().isEmpty()) {
			return "token:global";
		}

		return "anonymous";
	}

	public static boolean isAppRequestAuthorized(HttpRequest request, String path, String requestBody) {
		if (!ConfLoader.getInstance().isAppAuthEnabled()) {
			return true;
		}

		String appId = request.headers().get("X-SyncLite-App-Id");
		String timestampHeader = request.headers().get("X-SyncLite-Timestamp");
		String nonce = request.headers().get("X-SyncLite-Nonce");
		String signature = request.headers().get("X-SyncLite-Signature");

		if (appId == null || timestampHeader == null || nonce == null || signature == null) {
			return false;
		}

		appId = appId.trim().toLowerCase();
		nonce = nonce.trim();
		signature = signature.trim();
		if (appId.isEmpty() || nonce.isEmpty() || signature.isEmpty()) {
			return false;
		}

		long timestamp;
		try {
			timestamp = Long.parseLong(timestampHeader.trim());
		} catch (NumberFormatException e) {
			return false;
		}

		long now = System.currentTimeMillis();
		if (Math.abs(now - timestamp) > ConfLoader.getInstance().getAppAuthTimestampSkewMs()) {
			return false;
		}

		String appSecret = ConfLoader.getInstance().getAppSecret(appId);
		if (appSecret == null || appSecret.isEmpty()) {
			return false;
		}

		pruneExpiredNonces(now);

		String canonicalPayload = request.method().name().toUpperCase() + "\n"
				+ path + "\n"
				+ timestamp + "\n"
				+ nonce + "\n"
				+ sha256Hex(requestBody);
		String expectedSignature = signPayload(appSecret, canonicalPayload);
		if (!MessageDigest.isEqual(
			expectedSignature.getBytes(StandardCharsets.UTF_8),
			signature.getBytes(StandardCharsets.UTF_8)
		)) {
			return false;
		}

		String nonceKey = appId + ":" + nonce;
		if (observedNonces.putIfAbsent(nonceKey, now) != null) {
			return false;
		}

		if (observedNonces.size() > ConfLoader.getInstance().getAppAuthNonceCacheMaxEntries()) {
			observedNonces.remove(nonceKey);
			return false;
		}

		return true;
	}

	private static String sha256Hex(String value) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
			StringBuilder sb = new StringBuilder();
			for (byte b : hash) {
				sb.append(String.format("%02x", b));
			}
			return sb.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("SHA-256 is not available", e);
		}
	}

	private static String signPayload(String secret, String payload) {
		try {
			Mac mac = Mac.getInstance("HmacSHA256");
			mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
			byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
			return Base64.getEncoder().encodeToString(raw);
		} catch (Exception e) {
			throw new RuntimeException("Failed to compute request signature", e);
		}
	}

	private static void pruneExpiredNonces(long now) {
		long nonceTtl = ConfLoader.getInstance().getAppAuthNonceTtlMs();
		for (Map.Entry<String, Long> nonceEntry : observedNonces.entrySet()) {
			if (now - nonceEntry.getValue() > nonceTtl) {
				observedNonces.remove(nonceEntry.getKey());
			}
		}
	}
}
