package com.king.gmms.util;

/**
 * The modules a message went through, for tracing only (GmmsMessage.modulePath): module names joined by '>',
 * e.g. MultiSmppServer1>CoreEngine1>MultiSmppClient1. A module adds itself when it writes the message to Redis
 * and when it logs or writes a CDR; the same module twice in a row is written once. A DR starts its own path
 * from the vendor DR (the Client that received it). Nothing routes by it.
 */
public final class ModulePath {
	public static final String SEP = ">";
	/** longer paths (a message retried over and over) keep their end */
	static final int MAX_LENGTH = 255;

	private ModulePath() {
	}

	/** this process's module, null when not set */
	public static String self() {
		String m = System.getProperty("module");
		return m == null || m.trim().isEmpty() ? null : m.trim();
	}

	/** path with this module at its end */
	public static String withSelf(String path) {
		return append(path, self());
	}

	public static String append(String path, String module) {
		if (module == null || module.isEmpty()) {
			return path;
		}
		if (path == null || path.isEmpty()) {
			return module;
		}
		if (path.equals(module) || path.endsWith(SEP + module)) {
			return path;
		}
		String p = path + SEP + module;
		if (p.length() > MAX_LENGTH) {
			int cut = p.indexOf(SEP, p.length() - MAX_LENGTH + 3);
			p = cut < 0 ? p.substring(p.length() - MAX_LENGTH) : ".." + p.substring(cut);
		}
		return p;
	}
}
