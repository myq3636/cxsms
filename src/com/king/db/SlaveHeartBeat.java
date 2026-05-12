package com.king.db;


import java.sql.ResultSet;

import com.king.db.DBHAConstants;

public class SlaveHeartBeat extends DBHeartBeat {
	private static int connectionCount = 0;
	private SlaveHeartBeat(){
			init(DBHAConstants.SLAVE_KEY);
	}
	protected void getConnection() throws Exception{
		if(connection==null){
			connection=this.dataControl.getSlaveConnection("gmms");
			stmt = connection.createStatement(
					ResultSet.TYPE_SCROLL_SENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			connectionCount++;
			log.trace("start getConnection with connectionCount {}...",connectionCount);
		}
	}
	static class SingletonHolder {   
	  static SlaveHeartBeat instance = new SlaveHeartBeat();
	}   
	  
	public static SlaveHeartBeat getInstance() {   
	   return SingletonHolder.instance;   
	}
}
