package com.king.gmms.mqm;

import java.util.*;

import com.king.framework.SystemLogger;
import com.king.gmms.mqm.task.TaskTimer;

public class TaskHolder {
	private static SystemLogger log = SystemLogger.getSystemLogger(TaskHolder.class);
	private static Map<String, ArrayList<TaskTimer>> tasks = new HashMap<String, ArrayList<TaskTimer>>();
	private static ArrayList<String> tables = new ArrayList<String>();
	private static boolean initialized = false;
	private static TaskHolder taskHolder = new TaskHolder();
	private static TaskConfiguration taskConfiguration = null;

	private TaskHolder() {
	}

	/**
	 * get instance
	 * 
	 * @return taskConfiguration
	 */
	public static TaskHolder init() {
		if (!initialized) {
			taskConfiguration = TaskConfiguration.getInstance();
			createTasks();
			initialized = true;
		}
		return taskHolder;
	}

	public static ArrayList<TaskTimer> getTasks(String table) {
		return tasks.get(table);
	}

	public static ArrayList<String> getTables() {
		return tables;
	}

	/**
	 * create task according to the configure
	 * 
	 * @throws ConfigurationException
	 */
	private static void createTasks() {
		String strTableNames = getProperty("MQM.TaskExecutor");
		if (strTableNames == null || "".equals(strTableNames)) {
			log.error("No Task configured for MQM.");
			return;
		} else {
			strTableNames = strTableNames.trim();
		}
		StringTokenizer tableNames = new StringTokenizer(strTableNames, ",");

		String tableName = "";
		String strTaskNames = "";
		String taskName = "";
		StringTokenizer taskNames = null;
		TaskTimer task = null;
		ArrayList<TaskTimer> timers = null;
		while (tableNames.hasMoreTokens()) {
			tableName = tableNames.nextToken().trim();
			if(log.isInfoEnabled()){
				log.info("Create Task for table: {}", tableName);
			}
			strTaskNames = getProperty(tableName + ".Task");
			if (strTaskNames == null || "".equals(strTaskNames)) {
				log.warn("No task configured for table: {}", tableName);
				continue;
			} else {
				tables.add(tableName);
				strTaskNames = strTaskNames.trim();
			}
			taskNames = new StringTokenizer(strTaskNames, ",");
			while (taskNames.hasMoreTokens()) {
				taskName = taskNames.nextToken().trim();
				if(log.isInfoEnabled()){
					log.info("Create Task: {} for table: {}", taskName, tableName);
				}
				task = TaskFactory.createTask(taskName, null);
				if (task != null) {
					task.init(tableName);
				}
				if (tasks.get(tableName) == null) {
					timers = new ArrayList<TaskTimer>();
					timers.add(task);
					tasks.put(tableName, timers);
				} else {
					tasks.get(tableName).add(task);
				}
			}
		}
	}

	public static String getProperty(String propertyName) {
		return taskConfiguration.getProperty(propertyName);
	}

	public static void main(String[] args) {
		TaskHolder.init();
		System.out.println("table size: " + tasks.size());
		System.out.println("table size: " + tasks.get("SMQ").size());
		System.out.println("table size: " + tasks.size());
		System.out.println("table size: " + tasks.size());
		System.out.println("table size: " + tasks.size());
		System.out.println("table size: " + tasks.size());
		System.out.println("table size: " + tasks.size());
	}
}
