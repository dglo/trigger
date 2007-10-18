/*
 * class: CoincidenceTriggerBag
 *
 * Version $Id: ConditionalTriggerBag.java 2155 2007-10-18 18:24:29Z dglo $
 *
 * Date: September 2 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.trigger.control;

import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.IUTCTime;
import icecube.daq.trigger.ITriggerRequestPayload;
import icecube.daq.trigger.algorithm.CoincidenceTrigger;

import java.util.*;
import java.util.zip.DataFormatException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is to provide some methods in GlobalTrigBag to CoincidenceTrigger algorithms.
 * This bag is handled by CoincidenceTrigger.
 * (cf. GlobalTrigBag is handled by GlobalTrigHandler.)
 *
 * @version $Id: ConditionalTriggerBag.java 2155 2007-10-18 18:24:29Z dglo $
 * @author shseo
 */
public class ConditionalTriggerBag
        extends GlobalTriggerBag
{
    /**
     * Log object for this class
     */
    private static final Log log = LogFactory.getLog(ConditionalTriggerBag.class);
    private CoincidenceTrigger mtCoincidenceTriggerAlgorithm;
    private List mListConfiguredTriggerIDs;
    private boolean mbContainAllTriggerIDsRequired;
    private String msCoincidenceTriggerAlgorithmName;

    private Vector payloadListInConditionalBag = new Vector();

    private boolean flushing;

    /**
     * UID for newly merged triggers
     */
    private int triggerUID;

    private boolean mbNeedUpdate;
    private DummyPayload mtUpdater;
    private List mListUnqualifiedTriggers = new ArrayList();
    /**
     * Create an instance of this class.
     * Default constructor is declared, but private, to stop accidental
     * creation of an instance of the class.
     */
    public ConditionalTriggerBag()
    {
        super();
    }

    public ConditionalTriggerBag(int iTrigType, int iTrigconfigId, ISourceID tSourceId)
    {
        super(iTrigType, iTrigconfigId, tSourceId);
    }
    /**
     * Overrided mothod in super class.
     * This method cotains specific conditional trigger algorithm.
     *
     * @param newPayload
     */
    public void add(ILoadablePayload newPayload)
    {
        try {
            newPayload.loadPayload();
        } catch (Exception e) {
            log.error("Error loading newPayload", e);
            return;
        }

        //--accept only configured triggers.
        if(!mtCoincidenceTriggerAlgorithm.isConfiguredTrigger((ITriggerRequestPayload)newPayload)){
            return;
        }

        //--add to internal list
        if (payloadListInConditionalBag.isEmpty()) {

            if (log.isDebugEnabled()) {
                log.debug("Adding trigger to empty bag");
            }

            payloadListInConditionalBag.add(newPayload);

        } else {

            if (log.isDebugEnabled())
            {
                log.debug("Adding newPayload to a full bag");
            }

            List mergeListForConditionalBag = null;

            //--loop over existing triggers to check timeOverlap w/ currentTrigger.
            Iterator iter = payloadListInConditionalBag.iterator();
            boolean addedNewPayload = false;
            while (iter.hasNext())
            {
                ITriggerRequestPayload existingPayload = (ITriggerRequestPayload) iter.next();

                //--check if CoincidenceTrigger
                if (mtCoincidenceTriggerAlgorithm.isCoincidentTrigger(existingPayload,
                                                             (ITriggerRequestPayload) newPayload))
                {
                    if (log.isDebugEnabled()) {
                        log.debug("Two payloads are coincident");
                    }
                    if(mergeListForConditionalBag == null)
                    {
                        mergeListForConditionalBag = new ArrayList();
                    }
                    if(!mergeListForConditionalBag.contains(existingPayload))
                    {
                        mergeListForConditionalBag.add(existingPayload);
                    }
                    if(!addedNewPayload)
                    {
                        mergeListForConditionalBag.add(newPayload);
                        addedNewPayload = true;
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Two payloads are not coincident");
                    }
                }
            }

            //--merge if neccessary, else add newPayload to list
            if (mergeListForConditionalBag != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Lets merge " + mergeListForConditionalBag.size() + " payloads for "
                            + msCoincidenceTriggerAlgorithmName);
                }
                for(int i=0; i<mergeListForConditionalBag.size(); i++)
                {
                    ITriggerRequestPayload trigger = (ITriggerRequestPayload) mergeListForConditionalBag.get(i);
                    log.debug("Trigger in mergeList: FirstTime = " + trigger.getFirstTimeUTC());
                    log.debug("Trigger in mergeList: LastTime = " + trigger.getLastTimeUTC());
                }

                Collections.sort(mergeListForConditionalBag);
                //-- performed prevention of multiple wrapping in this stage: only single wrap !!!
                getGlobalTrigEventWrapper().wrapMergingEvent(mergeListForConditionalBag,
                        mtCoincidenceTriggerAlgorithm.getTriggerType(),
                        mtCoincidenceTriggerAlgorithm.getTriggerConfigId());

                //-- remove individual triggers from triggerList and add new merged trigger
                payloadListInConditionalBag.removeAll(mergeListForConditionalBag);
                payloadListInConditionalBag.add(getGlobalTrigEventWrapper().getGlobalTrigEventPayload_merged());

            } else {
                if (log.isDebugEnabled()) {
                    log.debug("No need to merge");
                }

                payloadListInConditionalBag.add(newPayload);
            }

            Collections.sort(payloadListInConditionalBag);
        }

        if (log.isDebugEnabled()) {
            log.debug("Selected CoincidenceTriggerList has " + payloadListInConditionalBag.size() + " payloads.");
            log.debug("   TimeGate at " + getTimeGate());
        }

    }
    /**
     * This method checks a selected coincidenceTrigger (which was merged already)
     * contains all triggerIDs required by a specipic coincidenceTriggerAlgorithm.
     *
     * @return
     */
    private boolean containAllTriggerIDsRequired(ITriggerRequestPayload tTrigger)
    {
        if(tTrigger.getSourceID().getSourceID() != GlobalTrigEventWrapper.GLOBAL_TRIGGER_SOURCE_ID.getSourceID()){
            return false;
        }

        Iterator iterTriggers;
        try {
            iterTriggers = tTrigger.getPayloads().iterator();
        } catch (IOException e) {
            log.error("Couldn't get payloads", e);
            return false;
        } catch (DataFormatException e) {
            log.error("Couldn't get payloads", e);
            return false;
        }

        //--find triggerIDs
        List listTriggerIDs = new ArrayList();
        while(iterTriggers.hasNext())
        {
            ITriggerRequestPayload tPayload = (ITriggerRequestPayload) iterTriggers.next();
            try {
                ((ILoadablePayload) tPayload).loadPayload();
            } catch (IOException e) {
                log.error("Couldn't load payload", e);
                continue;
            } catch (DataFormatException e) {
                log.error("Couldn't load payload", e);
                continue;
            }

            Integer tTriggerId = new Integer(mtCoincidenceTriggerAlgorithm.getTriggerId(tPayload));
            if(!listTriggerIDs.contains(tTriggerId))
            {
                listTriggerIDs.add(tTriggerId);
            }
        }

        //--compare w/ getTriggerIDs from triggerAlgorithm
        return listTriggerIDs.containsAll(mListConfiguredTriggerIDs);
    }
    /**
     * This is to remove any unqualified triggers before checking hasNext().
     * unqualified trigger == not-contain all trigger IDs required && lastTime < timeGate
     */
    private void removeUnqualifiedTriggers()
    {
        int iTriggersInBag = payloadListInConditionalBag.size();
        boolean bRemoved = false;
        if(iTriggersInBag > 0){
            for(int i=0; i < iTriggersInBag; i++){
                if(bRemoved){
                    i--;
                    bRemoved = false;
                }else{
                    ITriggerRequestPayload tPayload = (ITriggerRequestPayload) payloadListInConditionalBag.get(i);
                    IUTCTime lastTime = tPayload.getLastTimeUTC();
                    if((!containAllTriggerIDsRequired(tPayload) && getTimeGate().compareTo(lastTime) > 0)
                    || (!containAllTriggerIDsRequired(tPayload) && flushing)){
                        payloadListInConditionalBag.remove(i);
                        setUpdateInfo(new DummyPayload(tPayload.getFirstTimeUTC()));
                        iTriggersInBag = payloadListInConditionalBag.size();
                        bRemoved = true;
                    }
                }
            }

        }
    }

    public void flush() {
        flushing = true;
    }

    /**
     * this method checks whether triggers in the payload list are releasable or not
     * by comparing against timeGate.
     * @return
     */
    public boolean hasNext() {
        //--remove invalid triggers before processing further.
        initUpdateInfo();
        removeUnqualifiedTriggers();
        //--iterate over triggerList and check against timeGate
        Iterator iter = payloadListInConditionalBag.iterator();

        while (iter.hasNext())
        {
            ITriggerRequestPayload trigger = (ITriggerRequestPayload) iter.next();

            if (flushing || 0 < getTimeGate().compareTo(trigger.getLastTimeUTC())) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method returns releasable trigger.
     *
     * @return
     */
    public ITriggerRequestPayload next() {
        //-- iterate over triggerList and check against timeGate
        Iterator iter = payloadListInConditionalBag.iterator();
        while (iter.hasNext())
        {
            ITriggerRequestPayload trigger = (ITriggerRequestPayload) iter.next();
            double timeDiff = getTimeGate().timeDiff_ns(trigger.getLastTimeUTC());
            if ( flushing || 0 < getTimeGate().compareTo(trigger.getLastTimeUTC()) )
            {
                iter.remove();
                if (log.isDebugEnabled()) {
                    log.debug("Releasing trigger with timeDiff = " + timeDiff);
                }
                //--GTEventNumber should be assigned here.
                triggerUID++;
                getGlobalTrigEventWrapper().wrapFinalEvent(trigger, triggerUID);
                trigger = (ITriggerRequestPayload) getGlobalTrigEventWrapper().getGlobalTrigEventPayload_final();

                return trigger;
            }
        }
        return null;
    }

    /**
     * This is to flush splicer when return = true
     * @return
     */
    public boolean needUpdate()
    {
        return mbNeedUpdate;
    }

    public DummyPayload getUpdater()
    {
        return mtUpdater;
    }

    private void initUpdateInfo(){
        mbNeedUpdate = false;
        mtUpdater = null;
    }
    private void setUpdateInfo(DummyPayload trigger)
    {
        mbNeedUpdate = true;
        mtUpdater = trigger;
    }
    /**
     * This method sets which CoincidenceTriggerAlgorithm being used in this ConditionalTriggerBag.
     * That information is absolutely necessary here.
     *
     * @param tCoincidenceTrigger
     */
    public void setConditionalTriggerAlgorithm(CoincidenceTrigger tCoincidenceTrigger)
    {
        mtCoincidenceTriggerAlgorithm = tCoincidenceTrigger;
        mListConfiguredTriggerIDs = mtCoincidenceTriggerAlgorithm.getConfiguredTriggerIDs();

        msCoincidenceTriggerAlgorithmName = mtCoincidenceTriggerAlgorithm.getClass().getName();
        log.info("A specific ConditionalTriggerAlgorithm was set in ConditionalTriggerBag: "
                    + msCoincidenceTriggerAlgorithmName);
    }

    private List getListUnqualifiedTriggers()
    {
        return mListUnqualifiedTriggers;
    }

    public Vector getVectorPayloadsInConditonalTriggerBag()
    {
        return payloadListInConditionalBag;
    }
    private void setVectorPayloadsInConditonalTriggerBag(Vector vecPayloads)
    {
        payloadListInConditionalBag.removeAllElements();
        payloadListInConditionalBag.add(vecPayloads);
    }
}
