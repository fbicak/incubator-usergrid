package org.usergrid.persistence.cassandra.util;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.utils.UUIDUtils;

/**
 * Keeps the TraceTag as a ThreadLocal
 * @author zznate
 */
public class TraceTagManager {
    private Logger logger = LoggerFactory.getLogger(TraceTagManager.class);

    private static ThreadLocal<TraceTag> localTraceTag = new ThreadLocal<TraceTag>();

    /**
     * Get the tag from a ThreadLocal. Will return null if no tag is attached.
     * @return
     */
    public TraceTag acquire() {
        return localTraceTag.get();
    }

    public TimedOpTag timerInstance() {
        return TimedOpTag.instance(acquire());
    }

    public void addTimer(TimedOpTag timedOpTag) {
        if ( acquire() != null ) {
            acquire().add(timedOpTag);
        } else {
            logger.info("Added TimedOpTag {} but no Trace in progress", timedOpTag);
        }
    }

    public boolean isActive() {
        return acquire() != null;
    }

    /**
     * Attache the tag to the current Thread
     * @param traceTag
     */
    public void attach(TraceTag traceTag) {
        // TODO throw illegal state exception if we have one already
        localTraceTag.set(traceTag);
        logger.debug("Attached TraceTag {} to thread", traceTag);
    }

    /**
     * Detach the tag from the current thread
     *
     */
    public TraceTag detach() {
        TraceTag traceTag = localTraceTag.get();
        Preconditions.checkState(traceTag != null,"Attempt to detach on no active trace");
        localTraceTag.remove();



        logger.debug("Detached TraceTag {} from thread", traceTag);
        return traceTag;
    }

    /**
     * Create a TraceTag
     * @return
     */
    public TraceTag create(String tagName) {
        return TraceTag.getInstance(UUIDUtils.newTimeUUID(), tagName);
    }
}
