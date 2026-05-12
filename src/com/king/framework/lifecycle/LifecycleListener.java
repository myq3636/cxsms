package com.king.framework.lifecycle;
import com.king.framework.lifecycle.event.Event;
/**
 * Lifecycle Listener is the class process lifecycle event.
 *
 * To support lifecycle, the class shoule implement this interface.
 *
 * @author not attributable
 * @version 1.0
 */
public interface LifecycleListener {
    public static final int STOP_PRIORITY_0=0;
    public static final int STOP_PRIORITY_1=1;
    public static final int STOP_PRIORITY_2=2;
    public static final int STOP_PRIORITY_3=3;
    public static final int STOP_PRIORITY_4=4;
    public static final int STOP_PRIORITY_5=5;
    public static final int STOP_LEVELS=6;

    public static final int STOP_PRIORITY_HIGHEST=STOP_PRIORITY_0;
    public static final int STOP_PRIORITY_LOWEST=STOP_PRIORITY_5;
    // Event entry.
    public int OnEvent(Event event);

}
