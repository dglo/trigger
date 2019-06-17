/*
 * class: MultiplicityStringTrigger
 *
 * Version $Id: MultiplicityStringTrigger.java,v 1.2 2007/02/06 20:26:29 vav111 Exp $
 *
 * Date: February 6 2006
 *
 * (c) 2007 IceCube Collaboration
 */

package icecube.daq.trigger.algorithm;

import icecube.daq.payload.PayloadInterfaceRegistry;
import icecube.daq.payload.IHitPayload;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.IUTCTime;
import icecube.daq.trigger.control.DummyPayload;
import icecube.daq.trigger.exceptions.IllegalParameterValueException;
import icecube.daq.trigger.exceptions.TimeOutOfOrderException;
import icecube.daq.trigger.exceptions.TriggerException;
import icecube.daq.trigger.exceptions.UnknownParameterException;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * This class implements a string based trigger.  It will veto a number of doms from the top
 * of the detector.  Then it will search for a given number of hits out of a given number of doms.
 * The trigger assumes all incoming hits are on the same string.
 *
 * @version $Id: MultiplicityStringTrigger.java,v 1.2 2007/02/06 20:26:29 vav111 Exp $
 * @author vince
 */

public class MultiplicityStringTrigger extends AbstractTrigger {

    /**
     * Log object for this class
     */

    private static final Logger LOG =
        Logger.getLogger(MultiplicityStringTrigger.class);

    private static int nextTriggerNumber;
    private int triggerNumber;

    /**
     * Trigger Parameters
     */

    /**
     * Parameters used by SimpleMajorityTrigger
     */


    private int threshold;
    private int timeWindow;
    private int numberOfHitsProcessed = 0;

    /**
     * list of hits currently within slidingTimeWindow
     */

    private SlidingTimeWindow slidingTimeWindow = new SlidingTimeWindow();

    /**
     * list of hits in current trigger
     */

    private LinkedList hitsWithinTriggerWindow = new LinkedList();

    /**
     * number of hits in hitsWithinTriggerWindow
     */

    private int numberOfHitsInTriggerWindow;

    private boolean configThreshold = false;
    private boolean configTimeWindow = false;

    /**
     * Parameters used by StringTrigger algorithm
     */

    private int numberOfVetoTopDoms;
    private int maxLength;
    private int string;

    private boolean configNumberOfVetoTopDoms = false;
    private boolean configMaxLength = false;
    private boolean configString = false;

    public MultiplicityStringTrigger()
    {
        triggerNumber = ++nextTriggerNumber;
    }

    /**
     *
     * Methods of ITriggerConfig
     *
     */

    /**
     * Is the trigger configured?
     *
     * @return true if it is
     */

    @Override
    public boolean isConfigured()
    {
        return (configThreshold&&configTimeWindow&&configNumberOfVetoTopDoms&&configMaxLength&&configString);
    }

    /**
     * Add a trigger parameter.
     *
     * @param name parameter name
     * @param value parameter value
     *
     * @throws UnknownParameterException if the parameter is unknown
     * @throws IllegalParameterValueException if the parameter value is bad
     */
    @Override
    public void addParameter(String name, String value)
        throws UnknownParameterException, IllegalParameterValueException
    {
        if (name.compareTo("numberOfVetoTopDoms") == 0) {
           if(Integer.parseInt(value)>=0) {
               numberOfVetoTopDoms = Integer.parseInt(value);
               configNumberOfVetoTopDoms = true;
            } else {
                throw new IllegalParameterValueException("Illegal number of exlcuded top doms value: " + value);
            }
        }
        else if (name.compareTo("maxLength")==0) {
            if(Integer.parseInt(value)>0) {
                maxLength = Integer.parseInt(value);
                configMaxLength = true;
            } else
                throw new IllegalParameterValueException("Illegal max length value:" +value);
        }
        else if (name.compareTo("threshold") == 0) {
            if(Integer.parseInt(value)>=0) {
                threshold = Integer.parseInt(value);
                configThreshold = true;
            }
            else {
                throw new IllegalParameterValueException("Illegal Threshold value: " + Integer.parseInt(value));
            }
        }
        else if(name.compareTo("string")==0) {
	    // Can't have this check on SPTS
	    // if(Integer.parseInt(value)>0&&Integer.parseInt(value)<=80) {
                string = Integer.parseInt(value);
                configString = true;
	    //            }
	    //else {
	    //    throw new IllegalParameterValueException("Illegal String value: " + Integer.parseInt(value));
	    //}
        }
        else if (name.compareTo("timeWindow") == 0) {
            if(Integer.parseInt(value)>=0) {
                timeWindow = Integer.parseInt(value);
                configTimeWindow = true;
            }
            else {
                throw new IllegalParameterValueException("Illegal timeWindow value: " + Integer.parseInt(value));
            }
        }
        else {
            throw new UnknownParameterException("Unknown parameter: " + name);
        }
        super.addParameter(name, value);
    }

    @Override
    public void setTriggerName(String triggerName)
    {
        super.triggerName = triggerName + triggerNumber;
        if (LOG.isInfoEnabled()) {
            LOG.info("TriggerName set to " + super.triggerName);
        }
    }

    /**
     *
     * Methods of ITriggerControl
     *
     */

    /**
     * Run the trigger algorithm on a payload.
     * *Same as a SimpleMajorityTrigger*
     * @param payload payload to process
     *
     * @throws icecube.daq.trigger.exceptions.TriggerException
     *          if the algorithm doesn't like this payload
     */

    @Override
    public void runTrigger(IPayload payload)
            throws TriggerException
        {

        // check that this is a hit
        int interfaceType = payload.getPayloadInterfaceType();
        if ((interfaceType != PayloadInterfaceRegistry.I_HIT_PAYLOAD) &&
            (interfaceType != PayloadInterfaceRegistry.I_HIT_DATA_PAYLOAD)) {
            throw new TriggerException("Expecting an IHitPayload");
        }
        IHitPayload hit = (IHitPayload) payload;

        // make sure spe bit is on for this hit (must have 0x02)
        int type = AbstractTrigger.getHitType(hit);
        if (type != AbstractTrigger.SPE_HIT) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Hit type is " + hit.getTriggerType() + ", returning.");
            }
            return;
        }

	// make sure this hit is on the proper string
	int hitString = getTriggerHandler().getDOMRegistry().getStringMajor(hit.getDOMID().longValue());
	if (hitString != string) {
		if(LOG.isDebugEnabled())
			LOG.debug("This hit is not on the proper string.");
		return;
	}


        IUTCTime hitTimeUTC = hit.getHitTimeUTC();

        /*
         * Initialization for first hit
         */
        if (numberOfHitsProcessed == 0) {

            // initialize slidingTimeWindow
            slidingTimeWindow.add(hit);
            // initialize triggerWindow
            //addHitToTriggerWindow(hit);
            // initialize earliest time of interest
            setEarliestPayloadOfInterest(hit);

            if (LOG.isDebugEnabled()) {
                LOG.debug("This is the first hit, initializing...");
                LOG.debug("slidingTimeWindowStart set to " + slidingTimeWindow.startTime());
            }

        }
        /*
         * Add another hit
         */
        else {

            if (LOG.isDebugEnabled()) {
                LOG.debug("Processing hit at time " + hitTimeUTC);
            }

            /*
             * Check for out-of-time hits
             * hitTime < slidingTimeWindowStart
             */
            if (hitTimeUTC == null) {
                throw new TriggerException("hitTimeUTC was null");
            } else if (slidingTimeWindow.startTime() == null) {
                LOG.error("SlidingTimeWindow startTime is null!!!");
                for (int i=0; i<slidingTimeWindow.size(); i++) {
                    IHitPayload h = (IHitPayload) slidingTimeWindow.hits.get(i);
                    if (h == null) {
                        LOG.error("  Hit " + i + " is null");
                    } else {
                        if (h.getPayloadTimeUTC() == null) {
                            LOG.error("  Hit " + i + " has a null time");
                        } else {
                            LOG.error("  Hit " + i + " has time = " + h.getPayloadTimeUTC());
                        }
                    }
                }
            }
            if (hitTimeUTC.compareTo(slidingTimeWindow.startTime()) < 0) {
                throw new TimeOutOfOrderException("Hit comes before start of sliding time window: Window is at "
                                                                                 + slidingTimeWindow.startTime() + " Hit is at "
                                                                                 + hitTimeUTC + " DOMId = "
                                                                                 + hit.getDOMID());
            }

            /*
             * Hit falls within the slidingTimeWindow
             */
            if (slidingTimeWindow.inTimeWindow(hitTimeUTC)) {
                slidingTimeWindow.add(hit);

                // If onTrigger, add hit to list
                if (onTrigger) {
                    addHitToTriggerWindow(hit);
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Hit falls within slidingTimeWindow numberOfHitsInSlidingTimeWindow now equals "
                              + slidingTimeWindow.size());
                }

            }
            /*
             * Hits falls outside the slidingTimeWindow
             *   First see if we have a trigger, then slide the window
             */
            else {

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Hit falls outside slidingTimeWindow, numberOfHitsInSlidingTimeWindow is "
                              + slidingTimeWindow.size());
                }

                // Do we have a trigger?
                if (slidingTimeWindow.aboveThreshold()) {

                    // onTrigger?
                    if (onTrigger) {

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Trigger is already on. Changing triggerWindowStop to "
                                      + getTriggerWindowStop());
                        }

                    } else {

                        // We now have the start of a new trigger
                        hitsWithinTriggerWindow.addAll(slidingTimeWindow.copy());
                        numberOfHitsInTriggerWindow = hitsWithinTriggerWindow.size();

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Trigger is now on, numberOfHitsInTriggerWindow = "
                                      + numberOfHitsInTriggerWindow + " triggerWindowStart = "
                                      + getTriggerWindowStart() + " triggerWindowStop = "
                                      + getTriggerWindowStop());
                        }

                    }

                    // turn trigger on
                    onTrigger = true;

                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Now slide the window...");
                }

                // Now advance the slidingTimeWindow until the hit is inside or there is only 1 hit left in window
                while ( (!slidingTimeWindow.inTimeWindow(hitTimeUTC)) &&
                        (slidingTimeWindow.size() > 1) ) {

                    IHitPayload oldHit = slidingTimeWindow.slide();

                    // if this hit is not part of the trigger, update the earliest time of interest
                    if ( (hitsWithinTriggerWindow == null) ||
                         ((hitsWithinTriggerWindow != null) && (!hitsWithinTriggerWindow.contains(oldHit))) ) {
                        IPayload oldHitPlus = new DummyPayload(oldHit.getHitTimeUTC().getOffsetUTCTime(1));
                        setEarliestPayloadOfInterest(oldHitPlus);
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("numberOfHitsInSlidingTimeWindow is now " + slidingTimeWindow.size()
                                  + " slidingTimeWindowStart is now " + slidingTimeWindow.startTime());
                    }

                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Done sliding");
                }

                /*
                 * Does it fall in new slidingTimeWindow?
                 *  if not, it defines the start of a new slidingTimeWindow
                 */
                if (!slidingTimeWindow.inTimeWindow(hitTimeUTC)) {

                    IHitPayload oldHit = slidingTimeWindow.slide();

                    if ( (hitsWithinTriggerWindow == null) ||
                         ((hitsWithinTriggerWindow != null) && (!hitsWithinTriggerWindow.contains(oldHit))) ) {
                        IPayload oldHitPlus = new DummyPayload(oldHit.getHitTimeUTC().getOffsetUTCTime(1));
                        setEarliestPayloadOfInterest(oldHitPlus);
                    }

                    slidingTimeWindow.add(hit);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Hit still outside slidingTimeWindow, start a new one "
                                  + "numberOfHitsInSlidingTimeWindow = " + slidingTimeWindow.size()
                                  + " slidingTimeWindowStart = " + slidingTimeWindow.startTime());
                    }

                }
                // if so, add it to window
                else {
                    slidingTimeWindow.add(hit);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Hit is now in slidingTimeWindow numberOfHitsInSlidingTimeWindow = "
                                  + slidingTimeWindow.size());
                    }

                }

                /*
                 * If onTrigger, check for finished trigger
                 */
                if (onTrigger) {

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Trigger is on, numberOfHitsInSlidingTimeWindow = "
                                  + slidingTimeWindow.size());
                    }

                    if ( ( (!slidingTimeWindow.aboveThreshold()) &&
                           (!slidingTimeWindow.overlaps(hitsWithinTriggerWindow)) ) ||
                         ( (slidingTimeWindow.size() == 1) && (threshold == 1) ) ) {

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Pass trigger to analyzeTopology and reset");
                        }

                        // pass trigger and reset
                        for (int i=0; i<hitsWithinTriggerWindow.size(); i++) {
                            IHitPayload h = (IHitPayload) hitsWithinTriggerWindow.get(i);
                            if (slidingTimeWindow.contains(h)) {
                                LOG.error("Hit at time " + h.getPayloadTimeUTC()
                                          + " is part of new trigger but is still in SlidingTimeWindow");
                            }
                        }
                        analyzeString(hitsWithinTriggerWindow);

                        onTrigger = false;
                        hitsWithinTriggerWindow.clear();
                        numberOfHitsInTriggerWindow = 0;

                    } else {
                        addHitToTriggerWindow(hit);

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Still on trigger, numberOfHitsInTriggerWindow = "
                                      + numberOfHitsInTriggerWindow);
                        }


                    }

                }

            }

        }

        numberOfHitsProcessed++;

    }

    /**
     * This method implements the string trigger criteria with a veto on all hits in a veto region.
     * @param hitsWithinTriggerWindow
     */

    public void analyzeString(LinkedList hitsWithinTriggerWindow)
    {

        if(LOG.isDebugEnabled()) {
            LOG.debug("Counting hits which meet string trigger criteria");
        }

        Iterator iter = hitsWithinTriggerWindow.iterator();

	// Veto events that have an intime hit in the veto region
        while(iter.hasNext()) {
            IHitPayload hit = (IHitPayload) iter.next();
            int hitPosition = getTriggerHandler().getDOMRegistry().getStringMinor(hit.getDOMID().longValue());
            if (hitPosition <= numberOfVetoTopDoms) {
                if (LOG.isDebugEnabled())
                    LOG.debug("The event contains a hit in the veto region, vetoing event.");
                return;
            }
        }

        Iterator iter2 = hitsWithinTriggerWindow.iterator();

        while(iter2.hasNext()) {
            IHitPayload topHit = (IHitPayload) iter2.next();
            int topPosition = getTriggerHandler().getDOMRegistry().getStringMinor(topHit.getDOMID().longValue());
            int numberOfHits = 0;
            Iterator iter3 = hitsWithinTriggerWindow.iterator();

            while(iter3.hasNext()) {
                IHitPayload hit = (IHitPayload) iter3.next();
                int hitPosition = getTriggerHandler().getDOMRegistry().getStringMinor(hit.getDOMID().longValue());
                if(hitPosition>=topPosition && hitPosition<(topPosition+maxLength))
                    numberOfHits++;
            }

            if(LOG.isDebugEnabled()) {
                LOG.debug("The number of hits between positions " + topPosition + " and " + (topPosition+maxLength) + " is " + numberOfHits);
            }

            if(numberOfHits>=threshold) {

                if(LOG.isDebugEnabled()) {
                    LOG.debug("The number of hits greater than the threshold.");
                    LOG.debug("Reporting trigger");
                }
                formTrigger(hitsWithinTriggerWindow, null, null);
                return;

            } else {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("The number of hits is less than the threshold");
                    LOG.debug("Trying next hit at topPostion");
                }
            }
        }
    }

    /**
     * Implementation of an exlcusion instead of a veto.  An exlcusion will only ignore hits in the top Dom
     * layer instead of vetoing the event.  The remaining hits below the excluded dom region must still meet
     * the remaining trigger criteria (n of m doms hits).
     */

//    public void analyzeString(LinkedList hitsWithinTriggerWindow) {
//
//        if(LOG.isDebugEnabled()) {
//            LOG.debug("Counting hits which meet string trigger criteria");
//        }
//
//        Iterator iter = hitsWithinTriggerWindow.iterator();
//
//        while(iter.hasNext()) {
//            IHitPayload topHit = (IHitPayload) iter.next();
//            int topPosition = getTriggerHandler().getDOMRegistry().getStringMinor(topHit.getDOMID().longValue());
//            int numberOfHits = 0;
//            if(topPosition>numberOfVetoTopDoms) {
//                Iterator iter2 = hitsWithinTriggerWindow.iterator();
//                while(iter2.hasNext()) {
//                    IHitPayload hit = (IHitPayload) iter2.next();
//                    int hitPosition = getTriggerHandler().getDOMRegistry().getStringMinor(hit.getDOMID().longValue());
//                    if(hitPosition>=topPosition && hitPosition<(topPosition+maxLength))
//                        numberOfHits++;
//                }
//
//                if(LOG.isDebugEnabled()) {
//                    LOG.debug("The number of hits between positions " + topPosition + " and " + (topPosition+maxLength) + " is " + numberOfHits);
//                }
//
//                if(numberOfHits>=threshold) {
//
//                    if(LOG.isDebugEnabled()) {
//                        LOG.debug("The number of hits greater than the threshold.");
//                        LOG.debug("Reporting trigger");
//                    }
//                    formTrigger(hitsWithinTriggerWindow, null, null);
//                    return;
//
//                } else {
//                    if(LOG.isDebugEnabled()) {
//                        LOG.debug("The number of hits is less than the threshold");
//                        LOG.debug("Trying next hit at topPostion");
//                    }
//                }
//            }
//        }
//    }

    /**
     * Flush the trigger. Basically indicates that there will be no further payloads to process
     * and no further calls to runTrigger.
     */

    @Override
    public void flush()
    {
        // see if we're above threshold, if so form a trigger
        if (onTrigger) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Last Trigger is on, numberOfHitsInTriggerWindow = "
                          + numberOfHitsInTriggerWindow);
            }

            // pass last trigger
            analyzeString(hitsWithinTriggerWindow);
        }
        //see if TimeWindow has enough hits to form a trigger before an out of bounds hit
        else if(slidingTimeWindow.aboveThreshold()) {

            hitsWithinTriggerWindow.addAll(slidingTimeWindow.copy());
            numberOfHitsInTriggerWindow = hitsWithinTriggerWindow.size();

            if (LOG.isDebugEnabled()) {
                LOG.debug(" Last Trigger is now on, numberOfHitsInTriggerWindow = "
                + numberOfHitsInTriggerWindow + " triggerWindowStart = "
                + getTriggerWindowStart() + " triggerWindowStop = "
                + getTriggerWindowStop());
            }

            // pass last trigger
            analyzeString(hitsWithinTriggerWindow);
        }
        reset();
    }
    /*
     *Get Parameter Methods
     */

    public int getThreshold()
    {
        return threshold;
    }

    public int getTimeWindow()
    {
        return timeWindow;
    }

    public int getNumberOfVetoTopDoms()
    {
        return numberOfVetoTopDoms;
    }

    public int getMaxLength()
    {
        return maxLength;
    }

    public int getString()
    {
        return string;
    }

    /**
     * getter for triggerWindowStart
     * @return start of trigger window
     */

    private IUTCTime getTriggerWindowStart()
    {
        return ((IHitPayload) hitsWithinTriggerWindow.getFirst()).getHitTimeUTC();
    }

    /**
     * getter for triggerWindowStop
     * @return stop of trigger window
     */

    private IUTCTime getTriggerWindowStop()
    {
        return ((IHitPayload) hitsWithinTriggerWindow.getLast()).getHitTimeUTC();
    }

    public int getNumberOfHitsWithinSlidingTimeWindow()
    {
        return slidingTimeWindow.size();
    }

    public int getNumberOfHitsWithinTriggerWindow()
    {
        return hitsWithinTriggerWindow.size();
    }

    /*
     *Set Parameter Methods
     */

    public void setThreshold(int Threshold)
    {
        this.threshold = Threshold;
    }

    public void setTimeWindow(int timeWindow)
    {
        this.timeWindow = timeWindow;
    }

    public void setNumberOfVetoTopDoms(int VetoTopDoms)
    {
        this.numberOfVetoTopDoms = VetoTopDoms;
    }

    public void setMaxLength(int MaxLength)
    {
        this.maxLength = MaxLength;
    }

    public void setString(int String)
    {
        this.string = String;
    }

    /**
     * add a hit to the trigger time window
     * @param triggerPrimitive hit to add
     */

    private void addHitToTriggerWindow(IHitPayload triggerPrimitive)
    {
        hitsWithinTriggerWindow.addLast(triggerPrimitive);
        numberOfHitsInTriggerWindow = hitsWithinTriggerWindow.size();
    }

    /**
     * Reset SimpleMajorityTrigger parameters
     */
    private void reset()
    {
        slidingTimeWindow = new SlidingTimeWindow();
        numberOfHitsProcessed = 0;
        hitsWithinTriggerWindow.clear();
        numberOfHitsInTriggerWindow = 0;
    }

    /**
     * Reset the algorithm to its initial condition.
     */
    @Override
    public void resetAlgorithm()
    {
        reset();

        super.resetAlgorithm();
    }

    private final class SlidingTimeWindow {

        private LinkedList hits;

        private SlidingTimeWindow()
        {
            hits = new LinkedList();
        }

        private void add(IHitPayload hit)
        {
            hits.addLast(hit);
        }

        private int size()
        {
            return hits.size();
        }

        private IHitPayload slide()
        {
            return (IHitPayload) hits.removeFirst();
        }

        private IUTCTime startTime()
        {
            return ((IHitPayload) hits.getFirst()).getHitTimeUTC();
        }

        private IUTCTime endTime()
        {
            return startTime().getOffsetUTCTime(timeWindow * 10L);
        }

        private boolean inTimeWindow(IUTCTime hitTime)
        {
            return (hitTime.compareTo(startTime()) >= 0) &&
                   (hitTime.compareTo(endTime()) <= 0);
        }

        private LinkedList copy()
        {
            return (LinkedList) hits.clone();
        }

        private boolean contains(Object object)
        {
            return hits.contains(object);
        }

        private boolean overlaps(List list)
        {
            Iterator iter = list.iterator();
            while (iter.hasNext()) {
                if (contains(iter.next())) {
                    return true;
                }
            }
            return false;
        }

        private boolean aboveThreshold()
        {
            return (hits.size() >= threshold);
        }
    }

    /**
     * Get the monitoring name.
     *
     * @return the name used for monitoring this trigger
     */
    @Override
    public String getMonitoringName()
    {
        return "STRING";
    }

    /**
     * Does this algorithm include all relevant hits in each request
     * so that it can be used to calculate multiplicity?
     *
     * @return <tt>true</tt> if this algorithm can supply a valid multiplicity
     */
    @Override
    public boolean hasValidMultiplicity()
    {
        return true;
    }
}
