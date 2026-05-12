package com.king.gmms.messagequeue;

/**
 * @deprecated Phase 2 refactoring: Memory queues are being phased out in favor of Redis Streams.
 */
@Deprecated
public interface InternalMessageQueue extends OperatorMessageQueue{

//	public void stopMessageQueue();
//	public void setListener(QueueTimeoutInterface listener);
//	public Object getMsgFirst();
//	public Object getRspFirst();
}
