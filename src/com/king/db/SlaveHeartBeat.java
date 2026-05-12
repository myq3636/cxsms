package com.king.db;


import java.sql.ResultSet;

import org.hibernate.CacheMode;
import org.hibernate.Session;

import com.king.db.DBHAConstants;

public class SlaveHeartBeat extends DBHeartBeat {
	private static int connectionCount = 0;
	private SlaveHeartBeat(){
			init(DBHAConstants.SLAVE_KEY);
	}
	protected void getConnection() throws Exception{
		Session s = (Session)session.get();
		if(s == null){
			log.trace("init slave db session...");
			if(factory==null){
				factory = this.dataControl.getSlaveSessionFactory("gmms");
			}
			s = factory.openSession();
			s.setCacheMode(CacheMode.NORMAL);
			session.set(s);
		}
		if(s.isDirty()){
            s.flush();
		}
		if(connection==null){
			connection=s.connection();
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
