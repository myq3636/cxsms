package com.king.gmms.processor;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import com.king.framework.SystemLogger;
import com.king.message.gmms.GmmsMessage;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class CsmValueInfoMark {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CsmValueInfoMark.class);
	private Set<CsmValueInfo> valueSet = new TreeSet<CsmValueInfo>();

	//Flag is used to indicate whether all the concatenated messages return DR 
	private boolean deliverReport = false; 
	
	public boolean isDeliverReport() {
		return deliverReport;
	}
	public void setDeliverReport(boolean deliverReport) {
		this.deliverReport = deliverReport;
	}
	
	public CsmValueInfoMark() {

	}

	public boolean hasReceivedAllCsm(final GmmsMessage message) {
		boolean ret = false;

		// check totalsegments
		int sarTotalSegments = message.getSarTotalSeqments();
		int currentSegments = valueSet.size();
		log.trace(message, "sarTotalSegments={}, currentTotalSegments={}",
				sarTotalSegments,currentSegments);
		if (currentSegments != sarTotalSegments) {
			return ret;
		}

		// get all seqNum, already sorted
		int[] seqNumArray = new int[currentSegments];
		int i = 0;
		for (Iterator<CsmValueInfo> iter = valueSet.iterator(); iter.hasNext();) {
			seqNumArray[i] = iter.next().getSarSegmentSeqNum();
			i++;
		}

		// check whether sequence is continuous, since there's no duplicated
		// seqNum, just need to check head and tail
		if (seqNumArray[0] == 1
				&& seqNumArray[sarTotalSegments - 1] == sarTotalSegments) {
			ret = true;
		}

		if (ret) {
			 if(log.isTraceEnabled()){
				 log.trace(message, "Received all csm segments.");
			 }
		}
		return ret;
	}

	public void add(CsmValueInfo valueInfo) {
		valueSet.add(valueInfo);
	}

	public Set<CsmValueInfo> getValueSet() {
		return valueSet;
	}

}
