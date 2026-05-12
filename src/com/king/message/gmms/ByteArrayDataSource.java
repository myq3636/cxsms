package com.king.message.gmms;

import javax.activation.DataSource;
import java.io.*;

public class ByteArrayDataSource implements DataSource {
    InputStream in = null;
    OutputStream out = null;
    String boundary = null;
    String contentType = null;


    public ByteArrayDataSource(byte [] bytes) {
        in = new ByteArrayInputStream(bytes);
        out = new ByteArrayOutputStream();
        String content = new String(bytes);
        int startB = content.indexOf("--") + 2;
        boundary = content.substring(startB, content.indexOf("\n", startB));
        this.contentType = "Multipart/related; \n Boundary=\"" + boundary + "\"; \n type=\"multipart/related\"";
    }

    public ByteArrayDataSource(byte [] bytes, boolean removeSlashR) {
        in = new ByteArrayInputStream(bytes);
        out = new ByteArrayOutputStream();
        String content = new String(bytes);
        int startB = content.indexOf("--") + 2;
        if(removeSlashR){
            boundary = content.substring(startB, content.indexOf("\r\n", startB));
        } else {
            boundary = content.substring(startB, content.indexOf("\n", startB));
        }
        this.contentType = "multipart/related; boundary=\"" + boundary + "\"";
    }

    public ByteArrayDataSource(byte [] bytes, String contentType) {
        in = new ByteArrayInputStream(bytes);
        out = new ByteArrayOutputStream();
        this.contentType = contentType;
    }

    /**
     * getInputStream
     *
     * @return InputStream
     */
    public InputStream getInputStream() {
        return in;
    }

    /**
     * getOutputStream
     *
     * @return OutputStream
     */
    public OutputStream getOutputStream() {
        return out;
    }

    /**
     * getContentType
     *
     * @return String
     */
    public String getContentType() {
        return this.contentType;
    }

    /**
     * getName
     *
     * @return String
     */
    public String getName() {
        return "";
    }
}
