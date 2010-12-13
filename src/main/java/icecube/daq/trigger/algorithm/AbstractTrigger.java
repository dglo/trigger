/*
 * class: AbstractTrigger
 *
 * Version $Id: AbstractTrigger.java 4891 2010-02-16 21:09:34Z dglo $
 *
 * Date: August 19 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.trigger.algorithm;

import icecube.daq.oldpayload.impl.TriggerRequestPayload;
import icecube.daq.oldpayload.impl.TriggerRequestPayloadFactory;
import icecube.daq.payload.IDOMID;
import icecube.daq.payload.IHitPayload;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.IReadoutRequest;
import icecube.daq.payload.IReadoutRequestElement;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.trigger.config.TriggerParameter;
import icecube.daq.trigger.config.TriggerReadout;
import icecube.daq.trigger.control.DummyPayload;
import icecube.daq.trigger.control.HitFilter;
import icecube.daq.trigger.control.ITriggerHandler;
import icecube.daq.trigger.exceptions.IllegalParameterValueException;
import icecube.daq.trigger.exceptions.TriggerException;
import icecube.daq.trigger.exceptions.UnknownParameterException;
import icecube.daq.trigger.monitor.TriggerMonitor;
import icecube.icebucket.monitor.ScalarFlowMonitor;
import icecube.icebucket.monitor.simple.ScalarFlowMonitorImpl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is an abstract Trigger. It implments nearly all of the methods of the
 * ITriggerConfig, ITriggerControl, and ITriggerMonitor interfaces. All specific trigger
 * classes derive from this class.
 *
 * @version $Id: AbstractTrigger.java 4891 2010-02-16 21:09:34Z dglo $
 * @author pat
 */
public abstract class AbstractTrigger
    implements ITrigger
{

    /**
     * Log object for this class
     */
    private static final Log log = LogFactory.getLog(AbstractTrigger.class);

    /**
     * hit type for test pattern, based on engineering format version 2+
     */
    public static final int TEST_HIT  = 0x00;
    /**
     * hit type for cpu hit, based on engineering format version 2+
     */
    public static final int CPU_HIT   = 0x01;
    /**
     * hit type for spe hit, based on engineering format version 2+
     */
    public static final int SPE_HIT   = 0x02;
    /**
     * hit type for flasher hit, based on engineering format version 2+
     */
    public static final int FLASH_HIT = 0x03;

    /**
     * status flags, based on engineering format version 2
     */
    public static final int FB_RUN    = 0x10;
    public static final int LC_DOWN   = 0x20;
    public static final int LC_UP     = 0x40;

    protected int triggerType;
    protected int triggerConfigId;
    protected ISourceID sourceId;
    protected String triggerName;
    private List readouts = new ArrayList();
    private List parameters = new ArrayList();

    private IPayload earliestPayloadOfIterest = null;
    private ITriggerHandler triggerHandler = null;
    protected TriggerRequestPayloadFactory triggerFactory = new TriggerRequestPayloadFactory();
    protected boolean onTrigger;
    protected int triggerCounter = 0;
    private int sentTriggerCounter = 0;
    private int printMod = 1000;

    protected int triggerPrescale = 0;
    protected int domSetId = -1;

    protected HitFilter hitFilter;

    private ScalarFlowMonitor countMonitor;
    private ScalarFlowMonitor byteMonitor;
    private TriggerMonitor triggerMonitor;

    /**
     * Default constructor
     */
    public AbstractTrigger() {
        countMonitor = new ScalarFlowMonitorImpl();
        byteMonitor  = new ScalarFlowMonitorImpl();
        triggerMonitor = new TriggerMonitor(countMonitor, byteMonitor);

        // setup default hitFilter
        hitFilter = new HitFilter();
    }

    /*
     *
     * Methods of ITriggerConfig
     *
     */

    /**
     * Get trigger type.
     * @return triggerType
     */
    public int getTriggerType() {
        return triggerType;
    }

    /**
     * Set trigger type.
     * @param triggerType
     */
    public void setTriggerType(int triggerType) {
        this.triggerType = triggerType;
        if (log.isInfoEnabled()) {
            log.info("TriggerType = " + triggerType);
        }
    }

    /**
     * Get trigger configuration id.
     * @return triggerConfigId
     */
    public int getTriggerConfigId() {
        return triggerConfigId;
    }

    /**
     * Set trigger configuration id.
     * @param triggerConfigId
     */
    public void setTriggerConfigId(int triggerConfigId) {
        this.triggerConfigId = triggerConfigId;
        if (log.isInfoEnabled()) {
            log.info("TriggerConfigId = " + triggerConfigId);
        }
    }

    /**
     * Get source id.
     * @return sourceId
     */
    public ISourceID getSourceId() {
        return sourceId;
    }

    /**
     * Set source id.
     * @param sourceId
     */
    public void setSourceId(ISourceID sourceId) {
        this.sourceId = sourceId;
        if (log.isInfoEnabled()) {
            log.info("SourceId = " + sourceId.getSourceID());
        }
    }

    /**
     * Get trigger name.
     * @return triggerName
     */
    public String getTriggerName() {
        return triggerName;
    }

    /**
     * Set trigger name.
     * @param triggerName
     */
    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
        if (log.isInfoEnabled()) {
            log.info("TriggerName = " + triggerName);
        }
    }

    /**
     * Add a readout.
     * @param readout TriggerReadout object
     */
    public void addReadout(TriggerReadout readout) {
        readouts.add(readout);
        if (log.isInfoEnabled()) {
            log.info("Added Readout: " + readout.toString());
        }
    }

    /**
     * Get list of trigger readouts.
     *
     * @return readout list
     */
    public List getReadoutList() {
        return readouts;
    }

    /**
     * Add a parameter.
     *
     * @param parameter TriggerParameter object.
     *
     * @throws icecube.daq.trigger.exceptions.UnknownParameterException
     *
     */
    public void addParameter(TriggerParameter parameter) throws UnknownParameterException, IllegalParameterValueException {
        parameters.add(parameter);
        if (log.isInfoEnabled()) {
            log.info("Added Parameter: " + parameter.toString());
        }
    }

    /**
     * Get list of trigger parameters.
     *
     * @return parameter list
     */
    public List getParameterList() {
        return parameters;
    }

    /*
     *
     * Methods of ITriggerControl
     *
     */

    /**
     * Get the earliest payload still of interest to this trigger.
     * @return earliest payload of interest
     */
    public IPayload getEarliestPayloadOfInterest() {
        return earliestPayloadOfIterest;
    }

    /**
     * Set the trigger handler of this trigger.
     * @param triggerHandler trigger handler
     */
    public void setTriggerHandler(ITriggerHandler triggerHandler) {
        this.triggerHandler = triggerHandler;

	// pass DOMRegistry to hitFilter
	hitFilter.setDomRegistry(triggerHandler.getDOMRegistry());
    }

    public ITriggerHandler getTriggerHandler() {
	return triggerHandler;
    }

    public void setTriggerFactory(TriggerRequestPayloadFactory triggerFactory) {
        this.triggerFactory = triggerFactory;
    }

    /**
     * Run the trigger algorithm on a payload.
     * @param payload payload to process
     * @throws icecube.daq.trigger.exceptions.TriggerException if the algorithm doesn't like this payload
     */
    public abstract void runTrigger(IPayload payload) throws TriggerException;

    /**
     * Flush the trigger.
     * Basically indicates that there will be no further payloads to process.
     */
    public abstract void flush();

    /*
     *
     * Methods of ITriggerMonitor
     *
     */

    public int getTriggerCounter() {
        return sentTriggerCounter;
    }

    public boolean isOnTrigger() {
        return onTrigger;
    }

    public TriggerMonitor getTriggerMonitor() {
        return triggerMonitor;
    }

    /*
     *
     * AbstractTrigger methods
     *
     */

    /**
     * Report a new trigger to the trigger handler.
     * @param payload single payload forming a trigger
     */
    protected void reportTrigger(ILoadablePayload payload) {
        if (null == triggerHandler) {
            throw new Error("TriggerHandler was not set!");
        }
        triggerCounter++;
        if ((triggerPrescale == 0) || ((triggerCounter % triggerPrescale) == 0)) {
            triggerHandler.addToTriggerBag(payload);
            sentTriggerCounter++;
            countMonitor.measure(1);
            byteMonitor.measure(payload.getPayloadLength());
        } else {
            payload.recycle();
        }
    }

    protected void setEarliestPayloadOfInterest(IPayload payload) {
        this.earliestPayloadOfIterest = payload;
    }

    /**
     * Form a ReadoutRequestElement based on the trigger and the readout configuration.
     * @param firstTime earliest time of trigger
     * @param readoutConfig ReadoutConfiguration object
     * @param domId domId, null if readout type is not MODULE
     * @param stringId stringId, null if readout type is not MODULE or STRING
     * @return IReadoutRequestElement
     *
     */
    protected IReadoutRequestElement createReadoutElement(IUTCTime firstTime, IUTCTime lastTime,
                                                          TriggerReadout readoutConfig,
                                                          IDOMID domId, ISourceID stringId) {

        IUTCTime timeOffset;
        IUTCTime timeMinus;
        IUTCTime timePlus;

        int type = readoutConfig.getType();
        switch (type) {
            case IReadoutRequestElement.READOUT_TYPE_GLOBAL:
                if (null != stringId) {
                    stringId = null;
                }
                if (null != domId) {
                    domId = null;
                }
                timeMinus = firstTime.getOffsetUTCTime(-readoutConfig.getMinus());
                timePlus = lastTime.getOffsetUTCTime(readoutConfig.getPlus());
                break;
            case IReadoutRequestElement.READOUT_TYPE_II_GLOBAL:
                if (null != stringId) {
                    stringId = null;
                }
                if (null != domId) {
                    domId = null;
                }
                if (sourceId.getSourceID() == SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID) {
                    timeOffset = firstTime.getOffsetUTCTime(readoutConfig.getOffset());
                    timeMinus = timeOffset.getOffsetUTCTime(-readoutConfig.getMinus());
                    timePlus = timeOffset.getOffsetUTCTime(readoutConfig.getPlus());
                } else {
                    timeMinus = firstTime.getOffsetUTCTime(-readoutConfig.getMinus());
                    timePlus = lastTime.getOffsetUTCTime(readoutConfig.getPlus());
                }
                break;
            case IReadoutRequestElement.READOUT_TYPE_II_STRING:
                // need stringId
                if (null == stringId) {
                    log.error("ReadoutType = " + type + " but StringId is NULL!");
                }
                if (null != domId) {
                    domId = null;
                }
                if (sourceId.getSourceID() == SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID) {
                    timeOffset = firstTime.getOffsetUTCTime(readoutConfig.getOffset());
                    timeMinus = timeOffset.getOffsetUTCTime(-readoutConfig.getMinus());
                    timePlus = timeOffset.getOffsetUTCTime(readoutConfig.getPlus());
                } else {
                    timeMinus = firstTime.getOffsetUTCTime(-readoutConfig.getMinus());
                    timePlus = lastTime.getOffsetUTCTime(readoutConfig.getPlus());
                }
                break;
            case IReadoutRequestElement.READOUT_TYPE_II_MODULE:
                // need stringId and domId
                if (null == stringId) {
                    log.error("ReadoutType = " + type + " but StringId is NULL!");
                }
                if (null == domId) {
                    log.error("ReadoutType = " + type + " but DomId is NULL!");
                }
                if (sourceId.getSourceID() == SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID) {
                    timeOffset = firstTime.getOffsetUTCTime(readoutConfig.getOffset());
                    timeMinus = timeOffset.getOffsetUTCTime(-readoutConfig.getMinus());
                    timePlus = timeOffset.getOffsetUTCTime(readoutConfig.getPlus());
                } else {
                    timeMinus = firstTime.getOffsetUTCTime(-readoutConfig.getMinus());
                    timePlus = lastTime.getOffsetUTCTime(readoutConfig.getPlus());
                }
                break;
            case IReadoutRequestElement.READOUT_TYPE_IT_GLOBAL:
                if (null != stringId) {
                    stringId = null;
                }
                if (null != domId) {
                    domId = null;
                }
                if (sourceId.getSourceID() == SourceIdRegistry.INICE_TRIGGER_SOURCE_ID) {
                    timeOffset = firstTime.getOffsetUTCTime(readoutConfig.getOffset());
                    timeMinus = timeOffset.getOffsetUTCTime(-readoutConfig.getMinus());
                    timePlus = timeOffset.getOffsetUTCTime(readoutConfig.getPlus());
                } else {
                    timeMinus = firstTime.getOffsetUTCTime(-readoutConfig.getMinus());
                    timePlus = lastTime.getOffsetUTCTime(readoutConfig.getPlus());
                }
                break;
            case IReadoutRequestElement.READOUT_TYPE_IT_MODULE:
                // need stringId and domId
                if (null == stringId) {
                    log.error("ReadoutType = " + type + " but StringId is NULL!");
                }
                if (null == domId) {
                    log.error("ReadoutType = " + type + " but DomId is NULL!");
                }
                if (sourceId.getSourceID() == SourceIdRegistry.INICE_TRIGGER_SOURCE_ID) {
                    timeOffset = firstTime.getOffsetUTCTime(readoutConfig.getOffset());
                    timeMinus = timeOffset.getOffsetUTCTime(-readoutConfig.getMinus());
                    timePlus = timeOffset.getOffsetUTCTime(readoutConfig.getPlus());
                } else {
                    timeMinus = firstTime.getOffsetUTCTime(-readoutConfig.getMinus());
                    timePlus = lastTime.getOffsetUTCTime(readoutConfig.getPlus());
                }
                break;
            default:
                log.error("Unknown ReadoutType: " + type + " -> Making it GLOBAL");
                type = IReadoutRequestElement.READOUT_TYPE_GLOBAL;
                timeMinus = firstTime.getOffsetUTCTime(-readoutConfig.getMinus());
                timePlus = lastTime.getOffsetUTCTime(readoutConfig.getPlus());
                break;
        }

        if (log.isDebugEnabled()) {
            log.debug("Creating readout: Type = " + type +
                      " FirstTime = " + timeMinus.longValue()/10.0 +
                      " LastTime = " + timePlus.longValue()/10.0);
        }

        return TriggerRequestPayloadFactory.createReadoutRequestElement(type, timeMinus, timePlus, domId, stringId);

    }

    /**
     * Form a TriggerRequestPayload.
     * @param hits hits that participated in trigger (must be time ordered)
     * @param dom DOM id, if relevent (otherwise null)
     * @param string sourceId of string, if relevent (otherwise null)
     */
    protected void formTrigger(List hits, IDOMID dom, ISourceID string) {

        if (null == triggerFactory) {
            throw new Error("TriggerFactory is not set!");
        }

        final int numberOfHits = hits.size();
        if (numberOfHits == 0) {
            throw new Error("Cannot form trigger from empty list of hits");
        }

        // get times (this assumes that the hits are time-ordered)
        IUTCTime firstTime = ((IHitPayload) hits.get(0)).getPayloadTimeUTC();
        IUTCTime lastTime = ((IHitPayload) hits.get(numberOfHits-1)).getPayloadTimeUTC();

        if (log.isDebugEnabled() && (triggerCounter % printMod == 0)) {
            log.debug("New Trigger " + triggerCounter + " from " + triggerName +
                      " includes " + numberOfHits + " hits:  First time = " +
                      firstTime + " Last time = " + lastTime);
        }

        // set earliest payload of interest to 1/10 ns after the last hit
        IPayload earliest
                = new DummyPayload(((IHitPayload) hits.get(numberOfHits-1)).getHitTimeUTC().getOffsetUTCTime(0.1));
        setEarliestPayloadOfInterest(earliest);

        // create readout requests
        Vector readoutElements = new Vector();
        Iterator readoutIter = readouts.iterator();
        while (readoutIter.hasNext()) {
            TriggerReadout readout = (TriggerReadout) readoutIter.next();
            readoutElements.add(createReadoutElement(firstTime, lastTime, readout, dom, string));
        }
        IReadoutRequest readoutRequest = TriggerRequestPayloadFactory.createReadoutRequest(sourceId,
                                                                                           triggerCounter,
                                                                                           readoutElements);

        // make payload
        TriggerRequestPayload triggerPayload
                = (TriggerRequestPayload) triggerFactory.createPayload(triggerCounter,
                                                                       triggerType,
                                                                       triggerConfigId,
                                                                       sourceId,
                                                                       firstTime,
                                                                       lastTime,
                                                                       new Vector(hits),
                                                                       readoutRequest);

        // report it
        reportTrigger(triggerPayload);

    }
    /**
     * Form a TriggerRequestPayload.
     */
    protected void formTrigger(IUTCTime time) {

        if (null == triggerFactory) {
            throw new Error("TriggerFactory is not set!");
        }

        if (log.isInfoEnabled() && (triggerCounter % printMod == 0)) {
            log.info("New Trigger " + triggerCounter + " from " + triggerName);
        }

        // create readout requests
        Vector readoutElements = new Vector();
        Iterator readoutIter = readouts.iterator();
        while (readoutIter.hasNext()) {
            TriggerReadout readout = (TriggerReadout) readoutIter.next();
            readoutElements.add(createReadoutElement(time, time, readout, null, null));
        }
        IReadoutRequest readoutRequest = TriggerRequestPayloadFactory.createReadoutRequest(sourceId,
                                                                                           triggerCounter,
                                                                                           readoutElements);

        // make payload
        TriggerRequestPayload triggerPayload
                = (TriggerRequestPayload) triggerFactory.createPayload(triggerCounter,
                                                                       triggerType,
                                                                       triggerConfigId,
                                                                       sourceId,
                                                                       time,
                                                                       time,
                                                                       new Vector(),
                                                                       readoutRequest);

        // report it
        reportTrigger(triggerPayload);

    }

    protected void formTrigger(IHitPayload hit, IDOMID dom, ISourceID string) {
        List hitList = new ArrayList(1);
        hitList.add(hit);
        formTrigger(hitList, dom, string);
    }

    /**
     * Dump the trigger configuration.
     * @return string dump of trigger
     */
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append(triggerName + ":\n");
        buffer.append("\tTriggerType     = " + triggerType + "\n");
        buffer.append("\tTriggerConfigId = " + triggerConfigId + "\n");
        buffer.append("\tSourceId        = " + sourceId + "\n");
        if (!parameters.isEmpty()) {
            buffer.append("\tParameters:\n");
            Iterator iter = parameters.iterator();
            while (iter.hasNext()) {
                buffer.append("\t\t" + ((TriggerParameter) iter.next()).toString() + "\n");
            }
        }
        if (!readouts.isEmpty()) {
            buffer.append("\tReadouts:\n");
            Iterator iter = readouts.iterator();
            while (iter.hasNext()) {
                buffer.append("\t\t" + ((TriggerReadout) iter.next()).toString() + "\n");
            }
        }
        return buffer.toString();
    }

    /**
     * method for retrieving the hit type from the trigger mode byte
     * @param hit hit
     * @return hit type (integer in range 1 to 4)
     * <table>
     * <tr><th>Hit Type ID</th><th>Description</th></tr>
     * <tr><td>0</td><td>Test pattern trigger (engineering fmt)</td></tr>
     * <tr><td>1</td><td>Forced trigger</td></tr>
     * <tr><td>2</td><td>SPE / MPE trigger</td></td></tr>
     * <tr><td>3</td><td>Flasher trigger</td></td></tr>
     * <tr><td>4</td><td>IceTop minimum bias trigger</td></tr>
     * </table>
     * <br>NOTE: Only the low 4 bits of the trigger type are returned.
     */
    public static int getHitType(IHitPayload hit) {

        return hit.getTriggerType() & 0xf;

    }

    /**
     * method for retrieving the LC tag from the trigger mode byte
     * @param hit hit
     * @return LC Tag (integer in range 0 to 63)
     */
    public static int getLcTag(IHitPayload hit) {

        // right shift 2 bits
        int lcTag = hit.getTriggerType() >> 2;
        // clear all bits except 5..0
        return (lcTag & 0x3f);

    }

    public int getTriggerPrescale() {
        return triggerPrescale;
    }

    public void setTriggerPrescale(int triggerPrescale) {
        this.triggerPrescale = triggerPrescale;
    }

    public int getDomSetId() {
        return domSetId;
    }

    public void setDomSetId(int domSetId) {
        this.domSetId = domSetId;
        configHitFilter(domSetId);
    }

    protected void configHitFilter(int domSetId) {
        hitFilter = new HitFilter(domSetId);
    }

}
