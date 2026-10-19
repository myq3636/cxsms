package com.king.gmms.mqm;

public final class MqmActiveState {
	// M-5: 默认 false——仅协调器(MqmActiveCoordinator)明确置 active，消除"协调器启动前 standby 也自认 active"的竞态。
	private static volatile boolean active = false;
	private static volatile String reason = "uninitialized";
	// C-9 fencing：当前任期纪元号(单调递增)。active 干活时带上，被作用方拒收旧纪元。-1 表示未持有有效任期。
	private static volatile long fence = -1L;

	private MqmActiveState() {
	}

	public static boolean isActive() {
		return active;
	}

	public static void setActive(boolean active, String reason) {
		MqmActiveState.active = active;
		MqmActiveState.reason = reason;
	}

	/** C-9：携带纪元号置 active/standby。active=true 时 fence 为本任期纪元；standby 时传 -1。 */
	public static void setActive(boolean active, String reason, long fence) {
		MqmActiveState.active = active;
		MqmActiveState.reason = reason;
		MqmActiveState.fence = active ? fence : -1L;
	}

	public static long getFence() {
		return fence;
	}

	public static String getReason() {
		return reason;
	}
}
