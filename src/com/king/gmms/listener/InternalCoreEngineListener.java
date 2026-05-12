package com.king.gmms.listener;

import java.io.IOException;
import java.net.Socket;

import com.king.gmms.connectionpool.session.InternalCoreEngineSession;

public class InternalCoreEngineListener extends AbstractInternalListener{

	private static InternalCoreEngineListener listener = new InternalCoreEngineListener();
	
	
	private InternalCoreEngineListener(){
		super();
	}
	
	public static synchronized InternalCoreEngineListener getInstance(){
		return listener;
	}
	
	protected void createSession(Socket clientSocket) throws IOException {
		if(clientSocket != null){
			new InternalCoreEngineSession(clientSocket);
		}
	}
	

}
