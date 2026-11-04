
package com.king.framework;

import java.util.regex.Pattern;

public interface A2PService {
    boolean startService();

    boolean stopService();

    /** Stop taking new work, without waiting: the first step when the process stops. */
    default void stopReading() {
    }
}
