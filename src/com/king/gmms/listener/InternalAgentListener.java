package com.king.gmms.listener;

import java.io.IOException;
import java.net.Socket;

import com.king.gmms.connectionpool.session.InternalAgentSession;

public class InternalAgentListener extends AbstractInternalListener{
	private static InternalAgentListener instance = new InternalAgentListener();;
	
	private InternalAgentListener(){
		super();
	}
	public static synchronized InternalAgentListener getInstance(){
		if(instance == null){
			instance = new InternalAgentListener();
		}
		return instance;
	}
	/**
	 * create session
	 */
	protected void createSession(Socket clientSocket) throws IOException {
		if(clientSocket != null) {
            new InternalAgentSession(clientSocket);
        }
	}
}
