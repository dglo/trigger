/*
 * class: GlobalTrigBag
 *
 * Version $Id: GlobalTrigBag.java, shseo
 *
 * Date: August 1 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.trigger.control;

//import icecube.daq.iniceTrig.framework.ITriggerBag;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.PayloadInterfaceRegistry;
import icecube.daq.payload.splicer.Payload;
import icecube.daq.payload.splicer.PayloadFactory;
import icecube.daq.payload.impl.UTCTime8B;
import icecube.daq.payload.impl.SourceID4B;
import icecube.daq.trigger.impl.TriggerRequestPayload;
import icecube.daq.trigger.impl.TriggerRequestPayloadFactory;
import icecube.daq.trigger.IHitPayload;
import icecube.daq.trigger.ITriggerRequestPayload;
import icecube.daq.trigger.monitor.PayloadBagMonitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * This class receives TriggerRequestPayloads from each active GlobalTrigAlgorithm
 * , merges if they overlap and produces globalTrigEventPayload.
 *
 * @version $Id: GlobalTriggerBag.java,v 1.12 2006/03/16 19:08:25 shseo Exp $
 * @author shseo
 */
public class GlobalTriggerBag
        implements ITriggerBag
{
    /**
     * Log object for this class
     */
    private static final Log log = LogFactory.getLog(GlobalTriggerBag.class);

    /**
     * internal list of triggers
     */
    public Vector payloadList = new Vector();

    /**
     * set of overlapping triggers to merge
     */
    protected static List mergeList = new ArrayList();

    private final TriggerRequestPayloadFactory DEFAULT_TRIGGER_FACTORY = new TriggerRequestPayloadFactory();
    /**
     * The factory used to create triggers
     */
    private TriggerRequestPayloadFactory triggerFactory = new TriggerRequestPayloadFactory();

    /**
     * triggers that occur earlier than this time are free to be released
     */
    protected IUTCTime timeGate = new UTCTime8B(-1);

    protected GlobalTrigEventWrapper mtGlobalTrigEventWrapper;

    /**
     * UID for newly merged triggers
     */
    private int triggerUID;

    /**
     * type for newly merged triggers
     */
    private int triggerType;

    /**
     * config ID for newly merged triggers
     */
    private int triggerConfigID;

    /**
     * source ID for newly merged triggers
     */
    private ISourceID triggerSourceID;

    /**
     * flag to indicate we are flushing
     */
    private boolean flushing = false;

    private int miTimeGateWindow;

    /**
     * Payload monitor object.
     */
    private PayloadBagMonitor monitor;

    /**
     * Create an instance of this class.
     * Default constructor is declared.
     * creation of an instance of the class.
     */
    public GlobalTriggerBag()
    {
        this(-1, -1, new SourceID4B(6000));
    }
    /**
      * constructor
      *
      * @param type trigger type to use for merged triggers
      * @param configID trigger config ID to use for merged triggers
      * @param sourceID trigger source ID to use for merged triggers
      */
    public GlobalTriggerBag(int type, int configID, ISourceID sourceID)
    {
        triggerUID = 0;
        triggerType = type;
        triggerConfigID = configID;
        triggerSourceID = sourceID;

        mtGlobalTrigEventWrapper = new GlobalTrigEventWrapper();

        //--todo: need to rework...?
        setTimeGap_option(1);//No_TimeGap
        this.init();

        monitor = new PayloadBagMonitor();

    }
    /**
     * In this method overlap is checked, merge if so, and then produces GlobalTrigEventPayload.
     *
     * @param currentPayload
     */
    public void add(ILoadablePayload currentPayload) {
         try {
             currentPayload.loadPayload();
         } catch (Exception e) {
             log.error("Error loading currentPayload", e);
         }

        // show this input to the monitor
        monitor.recordInput(currentPayload);

         //--add to internal list
         if (payloadList.isEmpty()) {

             if (log.isDebugEnabled()) {
                 log.debug("Adding trigger to empty bag");
             }

             payloadList.add(currentPayload);

         } else {
             if (log.isDebugEnabled()) {
                 log.debug("Adding currentPayload to a full bag");
             }

            //--prepare mergeList as empty.
            mergeList.clear();

            //--loop over existing triggers to check timeOverlap w/ currentTrigger.
            Iterator iter = payloadList.iterator();
            while (iter.hasNext()) {
                IPayload previousPayload = (IPayload) iter.next();

                //--check for timeOverlap
                if (overlap(previousPayload, currentPayload)) {
                    if (log.isDebugEnabled()) {
                        log.debug("Payload overlaps with another");
                    }
                    if(!mergeList.contains(previousPayload))
                    {
                        mergeList.add(previousPayload);
                    }
                    mergeList.add(currentPayload);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Payload does not overlap");
                    }
                }
            }

            //--merge if neccessary, else add new currentPayload to list
            if (!mergeList.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("Lets merge " + mergeList.size() + " payloads");
                }
                Collections.sort(mergeList);
                mtGlobalTrigEventWrapper.wrapMergingEvent(mergeList);
                // remove individual triggers from triggerList and add new merged trigger
                payloadList.removeAll(mergeList);
                payloadList.add(mtGlobalTrigEventWrapper.getGlobalTrigEventPayload_merged());

            } else {
                if (log.isDebugEnabled()) {
                    log.debug("No need to merge");
                }

                payloadList.add(currentPayload);
            }

            Collections.sort(payloadList);
        }

            if (log.isDebugEnabled()) {
                log.debug("TriggerList has " + payloadList.size() + " payloads");
                log.debug("   TimeGate at " + timeGate.getUTCTimeAsLong());
            }
     }

    public void flush() {
        flushing = true;
    }
    public void init() {
        flushing = false;
    }
    public boolean hasNext() {
        // iterate over triggerList and check against timeGate
        Iterator iter = payloadList.iterator();
        while (iter.hasNext()) {
            ITriggerRequestPayload trigger = (ITriggerRequestPayload) iter.next();
            if ( (flushing) ||
                 (0 < timeGate.compareTo(trigger.getLastTimeUTC())) ) {
                return true;
            }
        }
        return false;
    }

    public TriggerRequestPayload next() {

        // iterate over triggerList and check against timeGate
        Iterator iter = payloadList.iterator();
        while (iter.hasNext()) {
            TriggerRequestPayload trigger = (TriggerRequestPayload) iter.next();
            double timeDiff = timeGate.timeDiff_ns(trigger.getLastTimeUTC());
            if ( (flushing) ||
                 (0 < timeGate.compareTo(trigger.getLastTimeUTC())) ) {
                iter.remove();
                if (log.isDebugEnabled()) {
                    log.debug("Releasing trigger with timeDiff = " + timeDiff);
                }
                //GTEventNumber should be assigned here.
                triggerUID++;
                mtGlobalTrigEventWrapper.wrapFinalEvent(trigger, triggerUID);
                trigger = (TriggerRequestPayload) mtGlobalTrigEventWrapper.getGlobalTrigEventPayload_final();

                // show this output to the monitor
                monitor.recordOutput(trigger);

                return trigger;
            }
        }

        return null;
    }
    /**
     * checks the top-level trigger for sub-triggers
     *
     * @param trigger top-level trigger
     * @return vector of sub-triggers
     */
    private static List getSubTriggers(IPayload trigger) {

        Vector subTriggers = new Vector();
        LinkedList stack = new LinkedList();
        stack.add(trigger);

        while (!stack.isEmpty()) {

            // check trigger type to see if this is a merged trigger
            TriggerRequestPayload next = (TriggerRequestPayload) stack.removeFirst();
            if (0 > next.getTriggerType()) {
                // if it is, get the subPayloads and add them to the stack
                try {
                    stack.addAll(next.getPayloads());
                } catch (Exception e) {
                    log.error("Error accessing sub-payloads", e);
                }
            } else {
                // if it is not, add it to the list of subTriggers
                if (log.isDebugEnabled()) {
                    log.debug("  SubTrigger from " + next.getSourceID().getSourceID()
                              + " has type " + next.getTriggerType());
                }
                subTriggers.add(next);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("  Returning " + subTriggers.size() + " subTriggers");
        }

        return subTriggers;

    }

    public IUTCTime getTimeGate() {
        return timeGate;
    }

    /**
     * Check for overlap between two payloads
     *
     * @param payload1 first payload
     * @param payload2 second payload
     * @return true if they overlap, false otherwise
     */
    protected static boolean overlap(IPayload payload1, IPayload payload2)
    {
        // set times for first payload based on its type
        int type1 = payload1.getPayloadInterfaceType();
        IUTCTime startOfPayload1;
        IUTCTime endOfPayload1;

        if (type1 == PayloadInterfaceRegistry.I_HIT_PAYLOAD) {
            startOfPayload1 = ((IHitPayload) payload1).getHitTimeUTC();
            endOfPayload1   = startOfPayload1;
        } else if (type1 == PayloadInterfaceRegistry.I_TRIGGER_REQUEST_PAYLOAD) {
            startOfPayload1 = ((ITriggerRequestPayload) payload1).getFirstTimeUTC();
            endOfPayload1   = ((ITriggerRequestPayload) payload1).getLastTimeUTC();
        } else {
            log.error("Unexpected payload type passed to TriggerBag");
            return false;
        }

        // set times for second payload based on its type
        int type2 = payload2.getPayloadInterfaceType();
        IUTCTime startOfPayload2;
        IUTCTime endOfPayload2;

        if (type2 == PayloadInterfaceRegistry.I_HIT_PAYLOAD) {
            startOfPayload2 = ((IHitPayload) payload2).getHitTimeUTC();
            endOfPayload2   = startOfPayload2;
        } else if (type2 == PayloadInterfaceRegistry.I_TRIGGER_REQUEST_PAYLOAD) {
            startOfPayload2 = ((ITriggerRequestPayload) payload2).getFirstTimeUTC();
            endOfPayload2   = ((ITriggerRequestPayload) payload2).getLastTimeUTC();
        } else {
            log.error("Unexpected payload type passed to TriggerBag");
            return false;
        }

        if (log.isDebugEnabled()) {
            log.debug("Payload1: FirstTime = " + startOfPayload1.getUTCTimeAsLong()
                      + " LastTime = " + endOfPayload1.getUTCTimeAsLong());
            log.debug("Payload2: FirstTime = " + startOfPayload2.getUTCTimeAsLong()
                      + " LastTime = " + endOfPayload2.getUTCTimeAsLong());
        }

        if ( (0 < startOfPayload1.compareTo(endOfPayload2)) ||
             (0 < startOfPayload2.compareTo(endOfPayload1)) ) {
            if (log.isDebugEnabled()) {
                log.debug("  NO OVERLAP");
            }
            return false;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("  OVERLAP!!!");
            }
            return true;
        }

    }
    /**
     * This method sets timeGate which will be used for releasing GT events.
     * The timeGate should be comparedTo all of lastReadoutTime in payloadList.
     * If timeGate > lstReadoutTime then release the payload.
     *
     * @param time
     */
    public void setTimeGate(IUTCTime time)
    {
        if (log.isDebugEnabled()) {
            log.debug("Updating timeGate to " + time.getUTCTimeAsLong());
        }
        //System.out.println("------------------------------------------------------");
        //System.out.println("in BAG: MAX-TIME-GATE-WINDOW = " + miTimeGateWindow);
        timeGate = time.getOffsetUTCTime((double) miTimeGateWindow);
        //System.out.println("print timeGate = " + timeGate.getUTCTimeAsLong());
        //System.out.println("------------------------------------------------------");
    }

    /**
     * This method should be called in a class where configuration mbean is obtained.
     * todo. this mehtod fits better in Config.java class...?
     * @param iConfiguredMaxTimeWindow
     */
    public void setMaxTimeGateWindow(int iConfiguredMaxTimeWindow)
    {
        //--set timeGateWindow as negative always.
        //miTimeGateWindow = -Math.abs(iConfiguredMaxTimeWindow);
        miTimeGateWindow = iConfiguredMaxTimeWindow;
        //System.out.println("set MAX-TIME-GATE-WINDOW at GT Bag= " + miTimeGateWindow);
    }

    public int size()
    {
        return payloadList.size();
    }

    public void setPayloadFactory(PayloadFactory payloadFactory) {

        triggerFactory = (TriggerRequestPayloadFactory) payloadFactory;
        mtGlobalTrigEventWrapper.setPayloadFactory(triggerFactory);
    }

    /**
     * Get the monitor object.
     *
     * @return a PayloadBagMonitor
     */
    public PayloadBagMonitor getMonitor() {
        return monitor;
    }

    /**
     * Set the monitor object.
     *
     * @param monitor
     */
    public void setMonitor(PayloadBagMonitor monitor) {
        this.monitor = monitor;
    }

    public void setTimeGap_option(int iTimeGap_option)
    {
        mtGlobalTrigEventWrapper.setTimeGap_option(iTimeGap_option);
    }

}
