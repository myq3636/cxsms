package com.king.gmms.domain;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.util.LogRateLimiter;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * DR error codes (docs/DR错误码设计.md): conf/drErrorCode.cfg is the table of internal codes (3 digits, each with
 * its receipt state and GmmsStatus), conf/drErrorMapping.cfg maps a channel's codes to them ([shortName] blocks:
 * DRNetworkErr, DRErr, SubmitErr; [DEFAULT] for channels without a block) and them to a customer's codes
 * (CustomerErr, DRErrDefault, DRErrTLV). The Client only keeps what the channel sent (GmmsMessage.vendorErrorCode,
 * see {@link #drRaw} / {@link #submitRaw}); Core maps it, Server maps the result for the customer.
 * Loaded on first use, again by RELOAD_DRERROR; a file with an error keeps the tables in use.
 */
public final class DrErrorConfig {
	private static final SystemLogger log = SystemLogger.getSystemLogger(DrErrorConfig.class);
	private static final DrErrorConfig INSTANCE = new DrErrorConfig();

	public static final String CODE_FILE = "drErrorCode.cfg";
	public static final String MAPPING_FILE = "drErrorMapping.cfg";
	public static final String DEFAULT_BLOCK = "DEFAULT";
	/** code table used when conf/drErrorCode.cfg does not exist */
	static final String BUILT_IN_CODES =
			"000|DELIVRD|10000|Delivered|已送达\n"
			+ "001|ACCEPTD|10001|Accepted|已接受\n"
			+ "002|ENROUTE|10105|Enroute|投递中\n"
			+ "101|UNDELIV|10402|Unknown subscriber|号码不存在\n"
			+ "102|UNDELIV|10402|Invalid number format|号码格式错误\n"
			+ "103|UNDELIV|10402|Subscriber barred|用户停机或被禁\n"
			+ "104|UNDELIV|10405|Absent subscriber|用户关机或不在服务区\n"
			+ "200|UNDELIV|10400|Network error|网络或运营商错误\n"
			+ "201|UNDELIV|10405|Network failure|网络故障\n"
			+ "202|REJECTD|10500|Rejected by operator|运营商拒收\n"
			+ "209|DELETED|10300|Deleted|已删除\n"
			+ "301|REJECTD|10503|Content blocked|内容含违禁词\n"
			+ "302|REJECTD|10404|Sender not registered|发送方未报备\n"
			+ "400|REJECTD|10500|Rejected|被拒绝\n"
			+ "401|REJECTD|10502|Recipient in blacklist|号码在黑名单\n"
			+ "404|REJECTD|10800|No credit|欠费\n"
			+ "501|EXPIRED|10200|Expired|有效期内未送达\n"
			+ "601|UNDELIV|10400|Submit failed|通道提交失败\n"
			+ "999|UNKNOWN|10900|Unknown|未知\n";
	static final String[] REQUIRED = { "000", "001", "002", "200", "209", "400", "501", "999" };
	static final String[] STATES = { "DELIVRD", "ACCEPTD", "ENROUTE", "UNDELIV", "EXPIRED", "REJECTD", "DELETED", "UNKNOWN" };

	/** one internal code */
	public static final class Code {
		public final String code;
		public final String state;
		public final int status;
		Code(String code, String state, int status) {
			this.code = code;
			this.state = state;
			this.status = status;
		}
	}

	/** a numeric range of err values or internal codes */
	static final class Range {
		final String state;
		final long low, high;
		final String code;
		Range(String state, long low, long high, String code) {
			this.state = state;
			this.low = low;
			this.high = high;
			this.code = code;
		}
	}

	static final class ChannelRules {
		final Map<String, String> networkErr = new HashMap<String, String>();
		final Map<String, String> stateErr = new HashMap<String, String>();
		final List<Range> stateErrRanges = new ArrayList<Range>();
		final Map<String, String> errOnly = new HashMap<String, String>();
		final List<Range> errRanges = new ArrayList<Range>();
		final Map<String, String> stateOnly = new HashMap<String, String>();
		final Map<Long, String> submit = new HashMap<Long, String>();
		String submitAny;
		boolean isEmpty() {
			return networkErr.isEmpty() && stateErr.isEmpty() && stateErrRanges.isEmpty() && errOnly.isEmpty()
					&& errRanges.isEmpty() && stateOnly.isEmpty() && submit.isEmpty() && submitAny == null;
		}
	}

	static final class CustomerRules {
		final Map<String, String> codes = new HashMap<String, String>();
		final List<Range> ranges = new ArrayList<Range>();
		String mode = "legacy";
		boolean tlv = false;
	}

	static final class Tables {
		final Map<String, Code> codes = new LinkedHashMap<String, Code>();
		final Map<String, ChannelRules> channels = new HashMap<String, ChannelRules>();
		final Map<String, CustomerRules> customers = new HashMap<String, CustomerRules>();
	}

	private volatile Tables tables;

	private DrErrorConfig() {
	}

	public static DrErrorConfig getInstance() {
		DrErrorConfig c = INSTANCE;
		if (c.tables == null) {
			synchronized (c) {
				if (c.tables == null) {
					if (!c.reload()) {
						c.tables = builtIn();
					}
				}
			}
		}
		return c;
	}

	private static Tables builtIn() {
		Tables t = new Tables();
		try {
			parseCodes(BUILT_IN_CODES, t);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return t;
	}

	private static String confDir() {
		String home = System.getProperty("a2p_home", "/usr/local/a2p/");
		return home.endsWith("/") ? home + "conf/" : home + "/conf/";
	}

	/** loads both files; false (tables in use kept) when one has an error */
	public synchronized boolean reload() {
		File codeFile = new File(confDir() + CODE_FILE);
		File mappingFile = new File(confDir() + MAPPING_FILE);
		try {
			Tables t = new Tables();
			parseCodes(codeFile.exists() ? new String(Files.readAllBytes(codeFile.toPath()), StandardCharsets.UTF_8) : BUILT_IN_CODES, t);
			if (mappingFile.exists()) {
				List<A2PCustomerConfig> blocks = A2PCustomerConfig.parse(mappingFile, "UTF-8");
				if (blocks == null) {
					throw new IllegalArgumentException(MAPPING_FILE + " cannot be read");
				}
				for (A2PCustomerConfig b : blocks) {
					parseBlock(b, t);
				}
			}
			tables = t;
			log.info("[DR-ERR] loaded {} codes ({}), {} channel and {} customer blocks ({})", t.codes.size(),
					codeFile.exists() ? codeFile.getPath() : "built-in", t.channels.size(), t.customers.size(),
					mappingFile.exists() ? mappingFile.getPath() : "no mapping file");
			return true;
		} catch (Exception e) {
			log.error("[DR-ERR] DR error code files not loaded, the tables in use are kept: {}", e.getMessage());
			return false;
		}
	}

	static void parseCodes(String text, Tables t) {
		int lineNo = 0;
		for (String line : text.split("\n")) {
			lineNo++;
			line = line.trim();
			if (line.isEmpty() || line.startsWith("#")) {
				continue;
			}
			String[] c = line.split("\\|");
			if (c.length < 3) {
				throw new IllegalArgumentException(CODE_FILE + " line " + lineNo + ": code|state|status expected");
			}
			String code = c[0].trim(), state = c[1].trim().toUpperCase();
			if (!code.matches("\\d{3}")) {
				throw new IllegalArgumentException(CODE_FILE + " line " + lineNo + ": code " + code + " is not 3 digits");
			}
			if (t.codes.containsKey(code)) {
				throw new IllegalArgumentException(CODE_FILE + " line " + lineNo + ": code " + code + " twice");
			}
			boolean known = false;
			for (String s : STATES) known |= s.equals(state);
			if (!known) {
				throw new IllegalArgumentException(CODE_FILE + " line " + lineNo + ": state " + state + " unknown");
			}
			int status;
			try {
				status = Integer.parseInt(c[2].trim());
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(CODE_FILE + " line " + lineNo + ": status " + c[2].trim() + " is not a number");
			}
			if (GmmsStatus.getStatus(status) == null || GmmsStatus.getStatus(status).getCode() != status) {
				throw new IllegalArgumentException(CODE_FILE + " line " + lineNo + ": status " + status + " unknown");
			}
			t.codes.put(code, new Code(code, state, status));
		}
		for (String r : REQUIRED) {
			if (!t.codes.containsKey(r)) {
				throw new IllegalArgumentException(CODE_FILE + ": code " + r + " is required");
			}
		}
	}

	private static String attr(Group g, String name) {
		AttrPair a = g.getAttr(name);
		if (a == null || a.getStringValue() == null) {
			return null;
		}
		String v = a.getStringValue().trim();
		return v.isEmpty() ? null : v;
	}

	private static String knownCode(Tables t, String block, String key, Group g) {
		String code = attr(g, "Code");
		if (code == null || !t.codes.containsKey(code)) {
			throw new IllegalArgumentException(MAPPING_FILE + " [" + block + "] " + key + ": Code " + code + " is not in " + CODE_FILE);
		}
		return code;
	}

	private static List<Group> groups(A2PCustomerConfig b, String key) {
		if (b.getString(key) == null) {
			return Collections.emptyList();
		}
		A2PCustomerMultiValue v = b.parseMultiValue(key);
		if (v == null) {
			throw new IllegalArgumentException(MAPPING_FILE + " [" + b.getShortName() + "] " + key + " cannot be parsed");
		}
		return v.getAllGroups();
	}

	private static void put(Map<String, String> m, String k, String code, String block, String key) {
		if (m.put(k, code) != null) {
			throw new IllegalArgumentException(MAPPING_FILE + " [" + block + "] " + key + ": " + k + " twice");
		}
	}

	/** low~high, both numeric; null when value is not a range */
	private static long[] range(String value) {
		int i = value.indexOf('~');
		if (i < 0) {
			return null;
		}
		long lo = Long.parseLong(value.substring(0, i).trim()), hi = Long.parseLong(value.substring(i + 1).trim());
		if (lo > hi) {
			throw new IllegalArgumentException("range " + value + " is empty");
		}
		return new long[] { lo, hi };
	}

	private static void addRange(List<Range> l, Range r, String block, String key) {
		for (Range o : l) {
			boolean sameState = o.state == null ? r.state == null : o.state.equals(r.state);
			if (sameState && r.low <= o.high && o.low <= r.high) {
				throw new IllegalArgumentException(MAPPING_FILE + " [" + block + "] " + key + ": ranges " + o.low + "~" + o.high
						+ " and " + r.low + "~" + r.high + " overlap");
			}
		}
		l.add(r);
		// the narrower range first
		Collections.sort(l, (a, b) -> Long.compare(a.high - a.low, b.high - b.low));
	}

	static void parseBlock(A2PCustomerConfig b, Tables t) {
		String block = b.getShortName();
		ChannelRules ch = new ChannelRules();
		for (Group g : groups(b, "DRNetworkErr")) {
			String type = attr(g, "Type"), value = attr(g, "Value");
			if (type == null || value == null) {
				throw new IllegalArgumentException(MAPPING_FILE + " [" + block + "] DRNetworkErr: Type and Value needed");
			}
			put(ch.networkErr, networkKey(type, value), knownCode(t, block, "DRNetworkErr", g), block, "DRNetworkErr");
		}
		for (Group g : groups(b, "DRErr")) {
			String state = attr(g, "Stat"), err = attr(g, "Err"), code = knownCode(t, block, "DRErr", g);
			state = state == null ? null : normalizeState(state);
			if (err == null) {
				if (state == null) {
					throw new IllegalArgumentException(MAPPING_FILE + " [" + block + "] DRErr: Stat or Err needed");
				}
				put(ch.stateOnly, state, code, block, "DRErr");
			} else if (range(err) != null) {
				long[] r = range(err);
				addRange(state == null ? ch.errRanges : ch.stateErrRanges, new Range(state, r[0], r[1], code), block, "DRErr");
			} else if (state == null) {
				put(ch.errOnly, normalizeErr(err), code, block, "DRErr");
			} else {
				put(ch.stateErr, state + "|" + normalizeErr(err), code, block, "DRErr");
			}
		}
		for (Group g : groups(b, "SubmitErr")) {
			String status = attr(g, "Status"), code = knownCode(t, block, "SubmitErr", g);
			if ("*".equals(status)) {
				ch.submitAny = code;
			} else if (status == null || ch.submit.put(commandStatus(status), code) != null) {
				throw new IllegalArgumentException(MAPPING_FILE + " [" + block + "] SubmitErr: Status " + status + " missing or twice");
			}
		}
		if (!ch.isEmpty()) {
			t.channels.put(block, ch);
		}
		boolean customer = b.getString("CustomerErr") != null || b.getString("DRErrDefault") != null || b.getString("DRErrTLV") != null;
		if (customer) {
			CustomerRules cu = new CustomerRules();
			for (Group g : groups(b, "CustomerErr")) {
				String code = attr(g, "Code"), err = attr(g, "Err");
				if (code == null || err == null) {
					throw new IllegalArgumentException(MAPPING_FILE + " [" + block + "] CustomerErr: Code and Err needed");
				}
				long[] r = range(code);
				if (r != null) {
					addRange(cu.ranges, new Range(null, r[0], r[1], err), block, "CustomerErr");
				} else {
					knownCode(t, block, "CustomerErr", g);
					put(cu.codes, code, err, block, "CustomerErr");
				}
				if (err.length() > 3) {
					log.warn("[DR-ERR] {} CustomerErr Err={} is longer than 3: an SMPP receipt carries its first 3", block, err);
				}
			}
			String mode = b.getString("DRErrDefault", "legacy").trim().toLowerCase();
			if (!"legacy".equals(mode) && !"passthrough".equals(mode) && !"vendor".equals(mode) && !"000".equals(mode)) {
				throw new IllegalArgumentException(MAPPING_FILE + " [" + block + "] DRErrDefault " + mode + ": legacy, passthrough, vendor or 000");
			}
			cu.mode = mode;
			cu.tlv = "true".equalsIgnoreCase(b.getString("DRErrTLV", "false").trim());
			t.customers.put(block, cu);
		}
	}

	// ---- values as the channel sent them ----

	static String normalizeState(String s) {
		String v = s.trim().toUpperCase();
		switch (v) {
			case "DELIVERED": return "DELIVRD";
			case "UNDELIVERABLE": case "UNDELIVERED": return "UNDELIV";
			case "REJECTED": return "REJECTD";
			case "ACCEPTED": return "ACCEPTD";
			case "EXPIRE": return "EXPIRED";
			default: return v;
		}
	}

	/** digits compare by value (1, 01, 001), anything else case-insensitive */
	static String normalizeErr(String e) {
		String v = e.trim();
		if (!v.isEmpty() && v.matches("\\d{1,18}")) {
			return String.valueOf(Long.parseLong(v));
		}
		return v.toUpperCase();
	}

	static String networkKey(String type, String value) {
		String v = value.trim().toUpperCase();
		if (v.startsWith("0X")) {
			v = v.substring(2);
		}
		long n = Long.parseLong(v, 16);
		return Integer.parseInt(type.trim()) + ":" + String.format("%04X", n);
	}

	static long commandStatus(String s) {
		String v = s.trim();
		return v.toLowerCase().startsWith("0x") ? Long.parseLong(v.substring(2), 16) : Long.parseLong(v);
	}

	/** what the Client keeps of a vendor DR: state, err and network_error_code (type:value hex), any may be null */
	public static String drRaw(String state, String err, String networkErr) {
		StringBuilder b = new StringBuilder();
		if (state != null && !state.trim().isEmpty()) b.append("stat=").append(state.trim());
		if (err != null && !err.trim().isEmpty()) b.append(b.length() > 0 ? ";" : "").append("err=").append(err.trim());
		if (networkErr != null && !networkErr.isEmpty()) b.append(b.length() > 0 ? ";" : "").append("nec=").append(networkErr);
		return b.length() == 0 ? null : b.toString();
	}

	/** what the Client keeps of a failed submit: the command_status */
	public static String submitRaw(int commandStatus) {
		return "submit=" + commandStatus;
	}

	private static String part(String raw, String name) {
		if (raw == null) {
			return null;
		}
		for (String p : raw.split(";")) {
			int i = p.indexOf('=');
			if (i > 0 && p.substring(0, i).trim().equals(name)) {
				return p.substring(i + 1).trim();
			}
		}
		return null;
	}

	// ---- codes ----

	public Code code(String code) {
		return code == null ? null : tables.codes.get(code);
	}

	/** the first code with exactly this status, null when none */
	public String exactCode(int status) {
		for (Code c : tables.codes.values()) {
			if (c.status == status) return c.code;
		}
		return null;
	}

	/** the code of a status for the customer (a DR without one of its own): exactCode, else one with its hundred */
	public String defaultCode(int status) {
		String exact = exactCode(status);
		if (exact != null) return exact;
		for (Code c : tables.codes.values()) {
			if (c.status / 100 == status / 100) return c.code;
		}
		return "999";
	}

	private static String stateOf(int status) {
		GmmsStatus s = GmmsStatus.getStatus(status);
		if (status == GmmsStatus.DELIVERED.getCode()) return "DELIVRD";
		if (status == GmmsStatus.ACCEPT.getCode()) return "ACCEPTD";
		if (status == GmmsStatus.ENROUTE.getCode()) return "ENROUTE";
		if (status / 100 == 102) return "EXPIRED";
		if (status / 100 == 103) return "DELETED";
		if (status / 100 == 104) return "UNDELIV";
		if (status / 100 == 105 || status / 100 == 107 || status / 100 == 108) return "REJECTD";
		return s == null ? "UNKNOWN" : "UNKNOWN";
	}

	/** the code for a vendor DR by the channel's rules, null when no rule matched */
	String match(ChannelRules r, String state, String err, String nec) {
		if (r == null) {
			return null;
		}
		boolean delivered = "DELIVRD".equals(state);
		if (nec != null && !delivered && r.networkErr.containsKey(nec)) {
			return r.networkErr.get(nec);
		}
		Long n = err != null && err.matches("\\d{1,18}") ? Long.valueOf(err) : null;
		if (state != null && err != null && r.stateErr.containsKey(state + "|" + err)) {
			return r.stateErr.get(state + "|" + err);
		}
		if (state != null && n != null) {
			for (Range x : r.stateErrRanges) if (state.equals(x.state) && n >= x.low && n <= x.high) return x.code;
		}
		if (!delivered && err != null && r.errOnly.containsKey(err)) {
			return r.errOnly.get(err);
		}
		if (!delivered && n != null) {
			for (Range x : r.errRanges) if (n >= x.low && n <= x.high) return x.code;
		}
		if (state != null && r.stateOnly.containsKey(state)) {
			return r.stateOnly.get(state);
		}
		return null;
	}

	private static String channelName(int ssid) {
		A2PCustomerInfo c = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(ssid);
		return c == null ? null : c.getShortName();
	}

	/**
	 * Core, a vendor DR matched to its MT: its internal code from the channel's rules (then [DEFAULT]); a rule
	 * sets the status of that code, no rule keeps the status and takes its default code.
	 */
	public void applyVendorDr(GmmsMessage m, String raw) {
		Tables t = tables;
		m.setVendorErrorCode(raw);
		String state = part(raw, "stat");
		state = state == null ? stateOf(m.getStatusCode()) : normalizeState(state);
		String rawErr = part(raw, "err");
		String err = rawErr == null || rawErr.isEmpty() ? null : normalizeErr(rawErr);
		String nec = part(raw, "nec");
		if (nec != null) {
			int i = nec.indexOf(':');
			try {
				nec = i > 0 ? networkKey(nec.substring(0, i), nec.substring(i + 1)) : null;
			} catch (Exception e) {
				nec = null;
			}
		}
		A2PCustomerInfo ci = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(m.getRSsID());
		String channel = ci == null ? null : ci.getShortName();
		// the channel's SMSOptionDRNotPaidCode (cm.cfg) first, compared as sent: no credit
		if (ci != null && !ci.getSmsOptionDRNoCreditCode().isEmpty()) {
			List<String> notPaid = ci.getSmsOptionDRNoCreditCode();
			String necHex = null;
			if (nec != null) {
				int i = nec.indexOf(':');
				necHex = String.format("%02X", Integer.parseInt(nec.substring(0, i))) + nec.substring(i + 1);
			}
			if ((rawErr != null && notPaid.contains(rawErr)) || (necHex != null && (notPaid.contains(necHex) || notPaid.contains(necHex.toLowerCase())))) {
				m.setStatus(GmmsStatus.REJECTED_BYNC);
				m.setDrErrorCode(exactCode(GmmsStatus.REJECTED_BYNC.getCode()));
				log.info(m, "[DR-ERR] rssid={} vendor={} code={} status={} (SMSOptionDRNotPaidCode)", m.getRSsID(), raw, m.getDrErrorCode(), m.getStatusCode());
				return;
			}
		}
		String code = match(channel == null ? null : t.channels.get(channel), state, err, nec);
		if (code == null) {
			code = match(t.channels.get(DEFAULT_BLOCK), state, err, nec);
		}
		if (code != null) {
			Code c = t.codes.get(code);
			m.setStatus(GmmsStatus.getStatus(c.status));
			m.setDrErrorCode(code);
		} else {
			// kept only when it is this status's own code: a DR sent again from RDQ restores its status by it
			m.setDrErrorCode(exactCode(m.getStatusCode()));
			boolean errGiven = (err != null && !"0".equals(err)) || nec != null;
			if (errGiven && !"DELIVRD".equals(state)) {
				long n = LogRateLimiter.allow("dr-err-unmapped:" + m.getRSsID() + ":" + raw, 600000L);
				if (n > 0) {
					log.warn(m, "[DR-ERR-UNMAPPED] rssid={} channel={} vendor={} count={}: no rule, code {} by the status",
							m.getRSsID(), channel, raw, n, m.getDrErrorCode());
				}
			}
		}
		log.info(m, "[DR-ERR] rssid={} vendor={} code={} status={}", m.getRSsID(), raw, m.getDrErrorCode(), m.getStatusCode());
	}

	/** Core, a failed submit: its internal code by SubmitErr of the channel (then [DEFAULT]); the status is kept */
	public void applySubmitResult(GmmsMessage m) {
		String raw = m.getVendorErrorCode();
		String s = part(raw, "submit");
		if (s == null) {
			return;
		}
		long status;
		try {
			status = Long.parseLong(s);
		} catch (NumberFormatException e) {
			return;
		}
		Tables t = tables;
		String channel = channelName(m.getRSsID());
		String code = null;
		for (ChannelRules r : new ChannelRules[] { channel == null ? null : t.channels.get(channel), t.channels.get(DEFAULT_BLOCK) }) {
			if (r == null) continue;
			code = r.submit.containsKey(status) ? r.submit.get(status) : r.submitAny;
			if (code != null) break;
		}
		if (code != null) {
			m.setDrErrorCode(code);
			log.info(m, "[DR-ERR] rssid={} vendor={} code={} (submit)", m.getRSsID(), raw, code);
		}
	}

	// ---- for the customer ----

	/** the err the customer gets for this DR; 3 digits for an SMPP receipt */
	public String customerErr(String customer, GmmsMessage m) {
		Tables t = tables;
		String code = m.getDrErrorCode();
		if (code == null || !t.codes.containsKey(code)) {
			// kept on the DR when it is this status's own code: a DR sent again from RDQ restores its status by it
			m.setDrErrorCode(exactCode(m.getStatusCode()));
			code = defaultCode(m.getStatusCode());
		}
		CustomerRules r = customer == null ? null : t.customers.get(customer);
		if (r != null) {
			String e = r.codes.get(code);
			if (e != null) return e;
			long n = Long.parseLong(code);
			for (Range x : r.ranges) if (n >= x.low && n <= x.high) return x.code;
		}
		String mode = r == null ? "legacy" : r.mode;
		switch (mode) {
			case "passthrough":
				return code;
			case "vendor": {
				String v = part(m.getVendorErrorCode(), "err");
				return v != null && v.matches("\\d{1,3}") ? String.format("%03d", Integer.parseInt(v)) : code;
			}
			case "000":
				return "000";
			default:
				return legacyErr(m.getStatusCode());
		}
	}

	public boolean customerTlv(String customer) {
		CustomerRules r = customer == null ? null : tables.customers.get(customer);
		return r != null && r.tlv;
	}

	/** the err of a receipt before the DR error codes, by the status (the table customers got with SMSOptionSmppMapErrCod4DR) */
	public static String legacyErr(int status) {
		switch (status) {
			case 10000: return "000";
			case 10105: return "000";
			case 10200: return "001";
			case 10300: return "002";
			case 10400: return "003";
			case 10401: return "011";
			case 10402: return "012";
			case 10403: return "013";
			case 10404: return "014";
			case 10405: return "016";
			case 10406: return "017";
			case 10500: return "005";
			case 10501: return "006";
			case 10502: return "020";
			case 10503: return "011";
			case 10800: return "008";
			case 10801: return "014";
			case 10802: return "014";
			case 10803: return "019";
			case 10804: return "019";
			default: return "004";
		}
	}
}
