package com.king.gmms.mqm;

public final class MqmActiveState {
	private static volatile boolean active = true;
	private static volatile String reason = "standalone";

	private MqmActiveState() {
	}

	public static boolean isActive() {
		return active;
	}

	public static void setActive(boolean active, String reason) {
		MqmActiveState.active = active;
		MqmActiveState.reason = reason;
	}

	public static String getReason() {
		return reason;
	}
}
