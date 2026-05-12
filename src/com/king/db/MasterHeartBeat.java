package com.king.db;


import java.sql.ResultSet;

import com.king.db.DBHAConstants;

public class MasterHeartBeat extends DBHeartBeat {
	private static int connectionCount = 0;
	private MasterHeartBeat(){
			init(DBHAConstants.MASTER_KEY);
	}
	protected void getConnection() throws Exception{
		if(connection==null){
			connection=this.dataControl.getMasterConnection("gmms");
			stmt = connection.createStatement(
					ResultSet.TYPE_SCROLL_SENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			connectionCount++;
			log.trace("start getConnection with connectionCount {}...",connectionCount);
		}
	}
	static class SingletonHolder {   
	  static MasterHeartBeat instance = new MasterHeartBeat();
	}   
	  
	public static MasterHeartBeat getInstance() {   
	   return SingletonHolder.instance;   
	}
}
