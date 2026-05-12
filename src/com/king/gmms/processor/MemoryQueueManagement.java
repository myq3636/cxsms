package com.king.gmms.processor;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.king.gmms.util.Queue;

public class MemoryQueueManagement {
	
	private Map<UUID, Queue> messageQueueMap = null;
	private static MemoryQueueManagement instance = new MemoryQueueManagement();
	
	private MemoryQueueManagement(){
		messageQueueMap = new ConcurrentHashMap<UUID, Queue>();
	}
	
	public static MemoryQueueManagement getInstance(){
		return instance;
	}
	
	public boolean register(UUID uuid,Queue queue){
		Queue oldQueue = messageQueueMap.put(uuid, queue);
		if(oldQueue == null){
			return true;
		}else{
			return false;
		}
	}
	
	public boolean cancel(UUID uuid){
		Queue queue = messageQueueMap.remove(uuid);
		if(queue == null){
			return false;
		}else{
			return true;
		}
	}
	
	public boolean hasOverloadQueue(){
		boolean result = false;
		
		Collection<Queue> queueSet = messageQueueMap.values();
		
		for(Queue queue:queueSet){
			if(queue != null && queue.isFull()){
				result = true;
				break;
			}
		}
		
		return result;
	}

}
