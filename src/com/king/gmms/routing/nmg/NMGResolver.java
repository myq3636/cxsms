/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.routing.nmg;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.udp.nmg.Pdu;
import com.king.gmms.protocol.udp.nmg.UdpByteBuffer;
import com.king.message.gmms.GmmsMessage;

/**
 * An implementation that sends request to NMG UDP server and receive response.
 * 
 * @author bensonchen
 * @version 1.0.0
 */
public class NMGResolver {

	private static SystemLogger log = SystemLogger.getSystemLogger(NMGResolver.class);

	private final int timeoutValue = 10 * 1000;

	/**
	 * ByteBuffer capacity to receive UDP data
	 */
	private static final short DEFAULT_UDPSIZE = 1024;

	private Selector selector;
	private InetSocketAddress nmgAddress;

	private long checkTime;

	/**
	 * store GmmsMessage and time
	 */
	private class SelectElement {

		GmmsMessage message;
		long time;

		public SelectElement(GmmsMessage msg) {
			message = msg;
			time = System.currentTimeMillis();
		}

		public GmmsMessage getMsg() {
			return message;
		}

		public long getTime() {
			return time;
		}
	}

	/**
	 * Creates a NMGResolver that will query the specified host
	 * 
	 * @exception UnknownHostException
	 *                Failure occurred while finding the host
	 */
	public NMGResolver(String nmgAddr, int port) throws UnknownHostException {

		try {
			InetAddress addr = InetAddress.getByName(nmgAddr);
			nmgAddress = new InetSocketAddress(addr, port);
			selector = Selector.open();
		} catch (Exception ex) {
			log.warn(ex, ex);
		}
		checkTime = System.currentTimeMillis();

	}

	/**
	 * get ack of number_application and number_query
	 * 
	 * @param msgs
	 * @param resps
	 * @throws Exception
	 */
	public void getAsyResults(List<GmmsMessage> msgs, List<Pdu> resps)
			throws Exception {

		int eventsCount = selector.select(50);

		if (eventsCount > 0) {
			if(log.isTraceEnabled()){
				log.trace("call NMGResolver getAsyResults, eventsCount={}",
					eventsCount);
			}

			Set<SelectionKey> selected = selector.selectedKeys();
			Iterator<SelectionKey> iterator = selected.iterator();

			// may raise java.io.IOException: A message sent on a datagram
			// socket was larger than the internal message buffer or some other
			// network limit,
			// or the buffer used to receive a datagram into was smaller than
			// the datagram itself
			ByteBuffer byteBuffer = ByteBuffer.allocate(DEFAULT_UDPSIZE);

			while (iterator.hasNext()) {
				SelectionKey key = (SelectionKey) iterator.next();
				iterator.remove();
				if (key.isValid() && key.isReadable()) {

					try {
						DatagramChannel channel = (DatagramChannel) key
								.channel();
						channel.read(byteBuffer);
						byteBuffer.flip();

						SelectElement msg = (SelectElement) key.attachment();
						if (msg != null) {
							msgs.add(msg.getMsg());

							Pdu pdu = parsePDU(byteBuffer);
							resps.add(pdu);
						}

					} catch (Exception ex) {
						log.warn("NMG server communication exception:{}", ex);
					}
					cancelledKey(key);
				}
				byteBuffer.clear();
			}
		}

		// TODO
		long now = System.currentTimeMillis();
		if (!selector.keys().isEmpty()) {
			long delta = now - this.checkTime;
			if (delta > this.timeoutValue * 2 / 3) {
				handleSocketTimeout(resps, msgs, now);
				this.checkTime = now;
			}
		} else {
			this.checkTime = now;
		}
	}

	private void handleSocketTimeout(List<Pdu> resps, List<GmmsMessage> msgs,
			long now) {
		Set<SelectionKey> keys = this.selector.keys();
		if (keys == null) {
			return;
		}

		for (Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext();) {
			SelectionKey key = (SelectionKey) iter.next();
			try {
				if (key.interestOps() == 1) {
					SelectElement ele = (SelectElement) key.attachment();
					long time = ele.getTime();
					if ((now - time) > this.timeoutValue) {
						// time out
						if(log.isInfoEnabled()){
							log.info(ele.getMsg(), "NMG operation timeout.");
						}
						msgs.add(ele.getMsg());
						resps.add(null);
						cancelledKey(key);
					}
				}
			} catch (CancelledKeyException ckx) {
				cancelledKey(key);
			}
		}
	}

	public void cancelledKey(SelectionKey key) {

		key.cancel();
		key.attach(null);

		try {
			DatagramChannel channel = (DatagramChannel) key.channel();
			channel.socket().close();
			key.channel().close();
		} catch (IOException e) {
			log.warn("key channel close error {}", e);
		} catch (Exception e) {
			log.warn("socke error {}", e);
		}

	}

	/**
	 * send number_application and number_query to NMG
	 * 
	 * @param pdu
	 * @param msg
	 * @throws Exception
	 */
	public void asySend(Pdu pdu, GmmsMessage msg) throws Exception {
		if(log.isDebugEnabled()){
			log.debug(msg, pdu.toString());
		}
		boolean done = false;
		DatagramChannel channel = DatagramChannel.open();
		channel.configureBlocking(false);

		try {
			channel.connect(nmgAddress);
			byte[] pduBuffer = pdu.toByteBuffer().getBuffer();
			channel.write(ByteBuffer.wrap(pduBuffer));
			SelectElement ele = new SelectElement(msg);
			channel.register(selector, SelectionKey.OP_READ, ele);
			done = true;
		} catch (Exception e) {
			log.warn(e, e);
			throw e;
		} finally {
			if (!done) {
				channel.close();
			}
		}
	}

	/**
	 * parse ack from number_application and number_query
	 * 
	 * @param buffer
	 * @return
	 */
	public Pdu parsePDU(ByteBuffer buffer) {
		try {
			return Pdu.createPdu(new UdpByteBuffer(buffer.array()),
					Pdu.VERSION_2_0);
		} catch (Exception e) {
			log.warn("NMGResolver exception while create PDU, buffer is {} {}",
					buffer.array().toString(), e);
		}
		return null;
	}

}
