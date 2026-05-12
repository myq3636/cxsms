package com.king.gmms;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */

public class ContentScan {
    private String oCharset;
    private String rPrefix;
    private String charset;

    public ContentScan(String oCharset, String rPrefix, String charset) {
        this.oCharset = oCharset;
        this.rPrefix = rPrefix;
        this.charset = charset;
    }

    public String getOCharset() {
        return this.oCharset;
    }

    public String getRPrefix() {
        return this.rPrefix;
    }

    public String getTransferedCharset() {
        return this.charset;
    }

}
