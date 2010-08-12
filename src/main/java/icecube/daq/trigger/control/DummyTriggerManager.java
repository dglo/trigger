/*
 * class: TriggerManager
 *
 * Version $Id: TriggerManager.java,v 1.24 2006/05/09 19:55:29 toale Exp $
 *
 * Date: October 25 2004
 *
 * (c) 2004 IceCube Collaboration
 */

package icecube.daq.trigger.control;

import icecube.daq.oldpayload.impl.MasterPayloadFactory;
import icecube.daq.oldpayload.impl.TriggerRequestPayloadFactory;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.splicer.Spliceable;
import icecube.daq.splicer.SpliceableFactory;
import icecube.daq.splicer.Splicer;
import icecube.daq.splicer.SplicerChangedEvent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class provides the analysis framework for the inice trigger
 *
 * @version $Id: TriggerManager.java,v 1.24 2006/05/09 19:55:29 toale Exp $
 * @author pat
 */
public class DummyTriggerManager
        extends DummyTriggerHandler
        implements ITriggerManager
{

    /**
     * Log object for this class
     */
    private static final Log log = LogFactory.getLog(DummyTriggerManager.class);

    /**
     * The factory used to produce IHitPayloads for this object to use.
     */
    private SpliceableFactory inputFactory;

    /**
     * splicer associated with this manager
     */
    private Splicer splicer;

    /**
     * marks the begining position of each new spliced buffer, modulo the decrement due to shifting
     */
    private int start;

    /**
     * Constructor
     * @param inputFactory SpliceableFactory used by Splicer
     * @param sourceId SourceId of this TriggerManager
     * @param outputFactory factory used to build triggers
     */
    public DummyTriggerManager(SpliceableFactory inputFactory,
                               ISourceID sourceId,
                               TriggerRequestPayloadFactory outputFactory)
    {
        super(sourceId, outputFactory);
        this.inputFactory = inputFactory;
        init();
    }

    private void init() {
        start = 0;
    }

    /**
     * Reset the handler for a new run.
     */
    public void reset() {
        super.reset();
        init();
    }

    /**
     * method which is called by splicer to process primitives
     * assumes that all splicedObjects are IHitPayload's
     * @param splicedObjects list of primitives from splicer
     * @param decrement amount by which buffer was shifted since last call
     */
    public synchronized void execute(List splicedObjects, int decrement) {

        // Loop over the new objects in the splicer
        int numberOfObjectsInSplicer = splicedObjects.size();
        int lastInputListSize = numberOfObjectsInSplicer - (start - decrement);

        if (lastInputListSize > 0) {
            for (int index = start-decrement; numberOfObjectsInSplicer != index; index++) {

                ILoadablePayload payload =
                    (ILoadablePayload) splicedObjects.get(index);

                process(payload);
            }

            // Update splicer
            updateSplicer();

        }

        start = numberOfObjectsInSplicer;

    }

    /**
     * setter for splicer
     * @param splicer splicer associated with this object
     */
    public void setSplicer(Splicer splicer) {
        this.splicer = splicer;
        this.splicer.addSplicerListener(this);
    }

    /**
     * update splicer to earliest time of interest
     */
    public void updateSplicer() {
        Spliceable update = (Spliceable) getEarliestPayloadOfInterest();
        if (null != update) {
            splicer.truncate(update);
        }
    }

    /**
     * Called when the {@link icecube.daq.splicer.Splicer Splicer} enters the disposed state.
     *
     * @param event the event encapsulating this state change.
     */
    public void disposed(SplicerChangedEvent event) {
        if (log.isInfoEnabled()) {
            log.info("Received Splicer DISPOSED");
        }
        getPayloadOutput().destroyProcessor();
    }

    /**
     * Called when the {@link icecube.daq.splicer.Splicer Splicer} enters the failed state.
     *
     * @param event the event encapsulating this state change.
     */
    public void failed(SplicerChangedEvent event) {
        if (log.isErrorEnabled()) {
            log.error("Received Splicer FAILED");
        }
        getPayloadOutput().destroyProcessor();
    }

    /**
     * Called when the {@link icecube.daq.splicer.Splicer Splicer} enters the starting state.
     *
     * @param event the event encapsulating this state change.
     */
    public void starting(SplicerChangedEvent event) {
        // do nothing
    }

    /**
     * Called when the {@link icecube.daq.splicer.Splicer Splicer} enters the started state.
     *
     * @param event the event encapsulating this state change.
     */
    public void started(SplicerChangedEvent event) {
        // do nothing
    }

    /**
     * Called when the {@link icecube.daq.splicer.Splicer Splicer} enters the stopped state.
     *
     * @param event the event encapsulating this state change.
     */
    public void stopped(SplicerChangedEvent event) {
        if (log.isInfoEnabled()) {
            log.info("Received Splicer STOPPED");
        }
        getPayloadOutput().forcedStopProcessing();
    }

    /**
     * Called when the {@link icecube.daq.splicer.Splicer Splicer} enters the stopping state.
     *
     * @param event the event encapsulating this state change.
     */
    public void stopping(SplicerChangedEvent event) {
        // do nothing
    }

    /**
     * Called when the {@link icecube.daq.splicer.Splicer Splicer} has truncated its "rope". This method is called
     * whenever the "rope" is cut, for example to make a clean start from the frayed beginning of a "rope", and not jsut
     * the the {@link Splicer#truncate(Spliceable)} method is invoked. This enables the client to be notified as to
     * which Spliceable are nover going to be accessed again by the Splicer.
     *
     * @param event the event encapsulating this truncation.
     */
    public void truncated(SplicerChangedEvent event) {

        if (event.getSpliceable() == Splicer.LAST_POSSIBLE_SPLICEABLE) {
            flush();
        }

        Iterator iter = event.getAllSpliceables().iterator();
        while (iter.hasNext()) {
            ILoadablePayload payload = (ILoadablePayload) iter.next();
            payload.recycle();
        }

    }

    public Splicer getSplicer() {
        return splicer;
    }

    public IUTCTime getEarliestTime() {
        return null;
    }

    public IUTCTime getLatestTime() {
        return null;
    }

    public long getAverageProcessingTime() {
        return 0;
    }

    public long getMinProcessingTime() {
        return 0;
    }

    public long getMaxProcessingTime() {
        return 0;
    }

    public int getProcessingCount() {
        return 0;
    }

}
