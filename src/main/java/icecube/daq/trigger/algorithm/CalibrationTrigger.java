/*
 * class: CalibrationTrigger
 *
 * Version $Id: CalibrationTrigger.java 17776 2020-03-24 18:32:31Z dglo $
 *
 * Date: August 27 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.trigger.algorithm;

import icecube.daq.payload.IDOMID;
import icecube.daq.payload.IHitPayload;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.impl.DOMID;
import icecube.daq.payload.impl.SourceID;
import icecube.daq.trigger.control.DummyPayload;
import icecube.daq.trigger.exceptions.ConfigException;
import icecube.daq.trigger.exceptions.IllegalParameterValueException;
import icecube.daq.trigger.exceptions.TriggerException;
import icecube.daq.trigger.exceptions.UnknownParameterException;
import icecube.daq.util.DOMInfo;
import icecube.daq.util.IDOMRegistry;

import org.apache.log4j.Logger;

/**
 * This class is an implementation of a calibration trigger. It checks each hit
 * to see if it is the correct type (flasher hit for instance), and if so it
 * forms a TriggerRequest.
 *
 * This trigger is an example of an 'instantaneous trigger' since it is capable
 * of making a decision based only on the current hit.
 *
 * @version $Id: CalibrationTrigger.java 17776 2020-03-24 18:32:31Z dglo $
 * @author pat
 */
public class CalibrationTrigger
    extends AbstractTrigger
{
    /** Log object for this class */
    private static final Logger LOG =
        Logger.getLogger(CalibrationTrigger.class);

    /** I3Live monitoring name for this algorithm */
    private static final String MONITORING_NAME = "CALIBRATION";

    /** Numeric type for this algorithm */
    public static final int TRIGGER_TYPE = 1;

    private static int nextTriggerNumber;
    private int triggerNumber;

    /**
     * Type of hit to trigger on.
     */
    private int hitType;

    private boolean configHitType = false;

    /**
     * Constructor.
     */
    public CalibrationTrigger()
    {
        triggerNumber = ++nextTriggerNumber;
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
        if (name.compareTo("hitType") == 0) {
            hitType = Integer.parseInt(value);
            configHitType = true;
        } else if (name.compareTo("triggerPrescale") == 0) {
            triggerPrescale = Integer.parseInt(value);
        } else if (name.compareTo("domSet") == 0) {
            domSetId = Integer.parseInt(value);
            try {
                configHitFilter(domSetId);
            } catch (ConfigException ce) {
                throw new IllegalParameterValueException("Bad DomSet #" +
                                                         domSetId, ce);
            }
        } else {
            throw new UnknownParameterException("Unknown parameter: " + name);
        }
        super.addParameter(name, value);
    }

    /**
     * Flush the trigger. Basically indicates that there will be no further
     * payloads to process.
     */
    @Override
    public void flush()
    {
        // nothing to be done here since this trigger does not buffer anything.
    }

    public int getHitType()
    {
        return hitType;
    }

    /**
     * Get the monitoring name.
     *
     * @return the name used for monitoring this trigger
     */
    @Override
    public String getMonitoringName()
    {
        return MONITORING_NAME;
    }

    /**
     * Get the trigger type.
     *
     * @return trigger type
     */
    @Override
    public int getTriggerType()
    {
        return TRIGGER_TYPE;
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

    /**
     * Is the trigger configured?
     *
     * @return true if it is
     */
    @Override
    public boolean isConfigured()
    {
        return configHitType;
    }

    /**
     * Run the trigger algorithm on a payload.
     *
     * @param payload payload to process
     *
     * @throws icecube.daq.trigger.exceptions.TriggerException
     *          if the algorithm doesn't like this payload
     */
    @Override
    public void runTrigger(IPayload payload)
        throws TriggerException
    {

        if (!(payload instanceof IHitPayload)) {
            throw new TriggerException("Expecting an IHitPayload");
        }
        IHitPayload hit = (IHitPayload) payload;

        if (getHitType(hit) == hitType && hitFilter.useHit(hit)) {
            // this is the correct type, report trigger
            IDOMID domId;
            ISourceID srcId;
            if (hit.hasChannelID()) {
                IDOMRegistry registry = getTriggerHandler().getDOMRegistry();
                DOMInfo dom = getDOMFromHit(registry, hit);
                if (dom == null) {
                    LOG.error("Cannot find DOM for " + hit);
                    return;
                }
                domId = new DOMID(dom.getNumericMainboardId());
                srcId = new SourceID(dom.computeSourceId(hit.getChannelID()));
            } else {
                domId = hit.getDOMID();
                srcId = hit.getSourceID();
            }

            formTrigger(hit, domId, srcId);
        } else {
            // this is not, update earliest time of interest
            IPayload earliest =
                new DummyPayload(hit.getPayloadTimeUTC().getOffsetUTCTime(1));
            setEarliestPayloadOfInterest(earliest);
        }
    }

    public void setHitType(int hitType)
    {
        this.hitType = hitType;
    }

    @Override
    public void setTriggerName(String triggerName)
    {
        super.triggerName = triggerName + triggerNumber;
        if (LOG.isInfoEnabled()) {
            LOG.info("TriggerName set to " + super.triggerName);
        }
    }
}
