/*
 * class: PhysicsMinBiasTrigger
 *
 * Version $Id: PhysicsMinBiasTrigger.java,v 1.20 2006/09/14 20:35:13 toale Exp $
 *
 * Date: August 27 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.trigger.algorithm;

import icecube.daq.payload.IHitPayload;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.impl.UTCTime;
import icecube.daq.trigger.control.DummyPayload;
import icecube.daq.trigger.exceptions.ConfigException;
import icecube.daq.trigger.exceptions.IllegalParameterValueException;
import icecube.daq.trigger.exceptions.TriggerException;
import icecube.daq.trigger.exceptions.UnknownParameterException;

import org.apache.log4j.Logger;

/**
 * This class implements a minimum bias trigger with a deadtime. It simply
 * counts hits that are not in a deadtime window and applies a prescale for
 * determining when a trigger should be formed.
 *
 * @version $Id: PhysicsMinBiasTrigger.java,v 1.20 2006/09/14 20:35:13 toale Exp $
 * @author pat
 */
public class PhysicsMinBiasTrigger
    extends AbstractTrigger
{
    /** Log object for this class */
    private static final Logger LOG =
        Logger.getLogger(PhysicsMinBiasTrigger.class);

    /**
     * I3Live monitoring name for this algorithm
     *
     * NOTE: PnF calls both MinBias and PhysicsMinBias "MIN_BIAS"
     */
    private static final String MONITORING_NAME = "MIN_BIAS";

    /** Numeric type for this algorithm */
    public static final int TRIGGER_TYPE = 13;

    private static int nextTriggerNumber;
    private int triggerNumber;

    private int prescale;
    private int deadtime;
    private int numberProcessed = 0;

    private boolean configPrescale = false;
    private boolean configDeadtime = false;

    private IUTCTime deadtimeWindow = new UTCTime(0);

    public PhysicsMinBiasTrigger()
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
        if (name.compareTo("prescale") == 0) {
            prescale = Integer.parseInt(value);
            configPrescale = true;
        } else if (name.compareTo("deadtime") == 0) {
            deadtime = Integer.parseInt(value);
            configDeadtime = true;
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
        return (configPrescale && configDeadtime);
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
        if (prescale == -1) {
            throw new TriggerException("Prescale has not been set!");
        }

        if (!(payload instanceof IHitPayload)) {
            throw new TriggerException("Expecting an IHitPayload, not " +
                                       payload.getClass().getName());
        }
        IHitPayload hit = (IHitPayload) payload;

        boolean usableHit = getHitType(hit) == SPE_HIT &&
            hitFilter.useHit(hit);

        IUTCTime hitTime = hit.getPayloadTimeUTC();
        if (hitTime == null) {
            throw new TriggerException("Hit time was null");
        }

        boolean formedTrigger = false;
        if (usableHit && hitTime.compareTo(deadtimeWindow) > 0) {
            // this hit comes after the end of the deadtime window,
            // count it
            numberProcessed++;
            deadtimeWindow = hitTime.getOffsetUTCTime(deadtime * 10L);
            if (numberProcessed % prescale == 0) {
                // report this as a trigger and update the deadtime window
                formTrigger(hit, null, null);
                formedTrigger = true;
            }
        }

        if (!formedTrigger) {
            // just update earliest time of interest
            IPayload earliest =
                new DummyPayload(hitTime.getOffsetUTCTime(1));
            setEarliestPayloadOfInterest(earliest);
        }
    }

    /**
     * Flush the trigger. Basically indicates that there will be no further
     * payloads to process.
     */
    @Override
    public void flush()
    {
        // nothing to do here
    }

    public int getPrescale()
    {
        return prescale;
    }

    public void setPrescale(int prescale)
    {
        this.prescale = prescale;
    }

    public int getDeadtime()
    {
        return deadtime;
    }

    public void setDeadtime(int deadtime)
    {
        this.deadtime = deadtime;
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
