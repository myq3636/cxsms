package com.king.gmms.processor;

import com.king.message.gmms.GmmsMessage;

/**
 * CSM 分段存入 Redis 合并缓冲的结果（{@link RedisCsmReassembler#storeSegment}）。
 *
 * <ul>
 *   <li>{@code COMPLETE} —— 本段使该长短信集齐，{@link #getAssembled()} 为组装后的整条消息
 *       （组装失败时为 {@code null}，由调用方按兜底策略处理）。</li>
 *   <li>{@code INCOMPLETE} —— 本段已持久化到 Redis hash，但尚未集齐（调用方可 ACK 该入站分段）。</li>
 *   <li>{@code DONE} —— 本段所在的组已经完成（外发或过期 DR），这是一次重投，调用方只需 ACK。</li>
 *   <li>{@code BUSY} —— 已集齐，另一个消费者正在组装外发，调用方不应 ACK，稍后重投时再判断。</li>
 *   <li>{@code FAILED} —— 存储失败（Redis 异常），调用方不应 ACK，留 PEL 由 auto-claim 重试。</li>
 * </ul>
 */
public class CsmStoreResult {

	public enum State { COMPLETE, INCOMPLETE, DONE, BUSY, FAILED }

	private final State state;
	private final GmmsMessage assembled;

	private CsmStoreResult(State state, GmmsMessage assembled) {
		this.state = state;
		this.assembled = assembled;
	}

	public static CsmStoreResult complete(GmmsMessage assembled) {
		return new CsmStoreResult(State.COMPLETE, assembled);
	}

	public static CsmStoreResult incomplete() {
		return new CsmStoreResult(State.INCOMPLETE, null);
	}

	public static CsmStoreResult done() {
		return new CsmStoreResult(State.DONE, null);
	}

	public static CsmStoreResult busy() {
		return new CsmStoreResult(State.BUSY, null);
	}

	public static CsmStoreResult failed() {
		return new CsmStoreResult(State.FAILED, null);
	}

	public boolean isComplete()   { return state == State.COMPLETE; }
	public boolean isIncomplete() { return state == State.INCOMPLETE; }
	public boolean isDone()       { return state == State.DONE; }
	public boolean isBusy()       { return state == State.BUSY; }
	public boolean isFailed()     { return state == State.FAILED; }

	public GmmsMessage getAssembled() { return assembled; }
}
