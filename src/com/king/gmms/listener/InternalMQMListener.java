package com.king.gmms.listener;

import java.io.IOException;
import java.net.Socket;

import com.king.gmms.connectionpool.session.InternalMQMSession;

public class InternalMQMListener extends AbstractInternalListener{
	private static InternalMQMListener instance = new InternalMQMListener();;
	
	private InternalMQMListener(){
		super();
	}
	public static synchronized InternalMQMListener getInstance(){
		if(instance == null){
			instance = new InternalMQMListener();
		}
		return instance;
	}
	/**
	 * create session
	 */
	protected void createSession(Socket clientSocket) throws IOException {
		if(clientSocket != null) {
            new InternalMQMSession(clientSocket);
        }
	}
}
