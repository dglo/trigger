package icecube.daq.trigger.algorithm;

import icecube.daq.payload.IDOMID;
import icecube.daq.payload.IHitPayload;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.IReadoutRequest;
import icecube.daq.payload.IReadoutRequestElement;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.SourceIdRegistry;
import icecube.daq.payload.impl.ReadoutRequest;
import icecube.daq.payload.impl.ReadoutRequestElement;
import icecube.daq.payload.impl.TriggerRequest;
import icecube.daq.payload.impl.TriggerRequestFactory;
import icecube.daq.trigger.config.TriggerParameter;
import icecube.daq.trigger.config.TriggerReadout;
import icecube.daq.trigger.control.DummyPayload;
import icecube.daq.trigger.control.HitFilter;
import icecube.daq.trigger.control.ITriggerCollector;
import icecube.daq.trigger.control.ITriggerManager;
import icecube.daq.trigger.control.Interval;
import icecube.daq.trigger.control.PayloadSubscriber;
import icecube.daq.trigger.control.SubscribedList;
import icecube.daq.trigger.exceptions.ConfigException;
import icecube.daq.trigger.exceptions.IllegalParameterValueException;
import icecube.daq.trigger.exceptions.TriggerException;
import icecube.daq.trigger.exceptions.UnimplementedError;
import icecube.daq.trigger.exceptions.UnknownParameterException;
import icecube.daq.util.DOMInfo;
import icecube.daq.util.IDOMRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Base class for trigger algorithms.
 */
public abstract class AbstractTrigger
    implements AbstractTriggerMBean, ITriggerAlgorithm
{
    /** Log object for this class */
    private static final Logger LOG = Logger.getLogger(AbstractTrigger.class);

    /** Requests can be up to this number of DAQ ticks wide */
    private static final long REQUEST_WIDTH = 100000000;

    protected int triggerPrescale;
    protected int domSetId = -1;
    protected HitFilter hitFilter = new HitFilter();

    protected String triggerName;
    private int trigCfgId;
    private int srcId;
    private ArrayList<TriggerReadout> readouts =
        new ArrayList<TriggerReadout>();
    private ArrayList<TriggerParameter> params =
        new ArrayList<TriggerParameter>();

    private ITriggerManager mgr;
    protected TriggerRequestFactory triggerFactory;
    protected boolean onTrigger;
    protected int triggerCounter;
    private int sentTriggerCounter;
    private int printMod = 1000;

    private IPayload earliestPayloadOfInterest;

    private long releaseTime = Long.MIN_VALUE;

    private ArrayList<ITriggerRequestPayload> requests =
        new ArrayList<ITriggerRequestPayload>();
    private ITriggerCollector collector;

    private PayloadSubscriber subscriber;

    private long earliestMonitorTime = Long.MIN_VALUE;

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
        params.add(new TriggerParameter(name, value));
    }

    /**
     * Add a readout entry to the cached list.
     *
     * @param rdoutType readout type
     * @param offset offset value
     * @param minus minus
     * @param plus plus
     */
    @Override
    public void addReadout(int rdoutType, int offset, int minus, int plus)
    {
        readouts.add(new TriggerReadout(rdoutType, offset, minus, plus));
    }

    /**
     * Check the trigger type.
     *
     * @param val trigger type
     *
     * @throws ConfigException if the configured trigger type is wrong
     */
    @Override
    public void checkTriggerType(int val)
        throws ConfigException
    {
        if (getTriggerType() != val) {
            throw new ConfigException("Trigger type should be " +
                                      getTriggerType() + ", not " + val);
        }
    }

    @Override
    public int compareTo(ITriggerAlgorithm a)
    {
        int val = getTriggerName().compareTo(a.getTriggerName());
        if (val == 0) {
            val = getTriggerType() - a.getTriggerType();
            if (val == 0) {
                val = trigCfgId - a.getTriggerConfigId();
                if (val == 0) {
                    val = srcId - a.getSourceId();
                }
            }
        }

        return val;
    }

    protected void configHitFilter(int domSetId)
        throws ConfigException
    {
        hitFilter = new HitFilter(domSetId);
    }

    /**
     * Form a ReadoutRequestElement based on the trigger and the readout
     * configuration.
     * @param firstTime earliest time of trigger
     * @param roCfg ReadoutConfiguration object
     * @param domId domId, null if readout type is not MODULE
     * @param stringId stringId, null if readout type is not MODULE or STRING
     * @return IReadoutRequestElement
     *
     */
    protected IReadoutRequestElement createReadoutElement(IUTCTime firstTime,
                                                          IUTCTime lastTime,
                                                          TriggerReadout roCfg,
                                                          IDOMID domId,
                                                          ISourceID stringId)
    {
        IUTCTime timeOffset;
        IUTCTime timeMinus;
        IUTCTime timePlus;

        int type = roCfg.getType();
        switch (type) {
        case IReadoutRequestElement.READOUT_TYPE_GLOBAL:
            if (null != stringId) {
                stringId = null;
            }
            if (null != domId) {
                domId = null;
            }
            timeMinus = firstTime.getOffsetUTCTime(-roCfg.getMinus() * 10L);
            timePlus = lastTime.getOffsetUTCTime(roCfg.getPlus() * 10L);
            break;
        case IReadoutRequestElement.READOUT_TYPE_II_GLOBAL:
            if (null != stringId) {
                stringId = null;
            }
            if (null != domId) {
                domId = null;
            }
            if (srcId == SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID) {
                timeOffset =
                    firstTime.getOffsetUTCTime(roCfg.getOffset() * 10L);
                timeMinus =
                    timeOffset.getOffsetUTCTime(-roCfg.getMinus() * 10L);
                timePlus =
                    timeOffset.getOffsetUTCTime(roCfg.getPlus() * 10L);
            } else {
                timeMinus =
                    firstTime.getOffsetUTCTime(-roCfg.getMinus() * 10L);
                timePlus =
                    lastTime.getOffsetUTCTime(roCfg.getPlus() * 10L);
            }
            break;
        case IReadoutRequestElement.READOUT_TYPE_II_STRING:
            // need stringId
            if (null == stringId) {
                LOG.error("ReadoutType = " + type +
                          " but StringId is NULL!");
            }
            if (null != domId) {
                domId = null;
            }
            if (srcId == SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID) {
                timeOffset = firstTime.getOffsetUTCTime(roCfg.getOffset() * 10L);
                timeMinus = timeOffset.getOffsetUTCTime(-roCfg.getMinus() * 10L);
                timePlus = timeOffset.getOffsetUTCTime(roCfg.getPlus() * 10L);
            } else {
                timeMinus = firstTime.getOffsetUTCTime(-roCfg.getMinus() * 10L);
                timePlus = lastTime.getOffsetUTCTime(roCfg.getPlus() * 10L);
            }
            break;
        case IReadoutRequestElement.READOUT_TYPE_II_MODULE:
            // need stringId and domId
            if (null == stringId) {
                LOG.error("ReadoutType = " + type +
                          " but StringId is NULL!");
            }
            if (null == domId) {
                LOG.error("ReadoutType = " + type + " but DomId is NULL!");
            }
            if (srcId == SourceIdRegistry.ICETOP_TRIGGER_SOURCE_ID) {
                timeOffset = firstTime.getOffsetUTCTime(roCfg.getOffset() * 10L);
                timeMinus = timeOffset.getOffsetUTCTime(-roCfg.getMinus() * 10L);
                timePlus = timeOffset.getOffsetUTCTime(roCfg.getPlus() * 10L);
            } else {
                timeMinus = firstTime.getOffsetUTCTime(-roCfg.getMinus() * 10L);
                timePlus = lastTime.getOffsetUTCTime(roCfg.getPlus() * 10L);
            }
            break;
        case IReadoutRequestElement.READOUT_TYPE_IT_GLOBAL:
            if (null != stringId) {
                stringId = null;
            }
            if (null != domId) {
                domId = null;
            }
            if (srcId == SourceIdRegistry.INICE_TRIGGER_SOURCE_ID) {
                timeOffset = firstTime.getOffsetUTCTime(roCfg.getOffset() * 10L);
                timeMinus = timeOffset.getOffsetUTCTime(-roCfg.getMinus() * 10L);
                timePlus = timeOffset.getOffsetUTCTime(roCfg.getPlus() * 10L);
            } else {
                timeMinus = firstTime.getOffsetUTCTime(-roCfg.getMinus() * 10L);
                timePlus = lastTime.getOffsetUTCTime(roCfg.getPlus() * 10L);
            }
            break;
        case IReadoutRequestElement.READOUT_TYPE_IT_MODULE:
            // need stringId and domId
            if (null == stringId) {
                LOG.error("ReadoutType = " + type +
                          " but StringId is NULL!");
            }
            if (null == domId) {
                LOG.error("ReadoutType = " + type + " but DomId is NULL!");
            }
            if (srcId == SourceIdRegistry.INICE_TRIGGER_SOURCE_ID) {
                timeOffset = firstTime.getOffsetUTCTime(roCfg.getOffset() * 10L);
                timeMinus = timeOffset.getOffsetUTCTime(-roCfg.getMinus() * 10L);
                timePlus = timeOffset.getOffsetUTCTime(roCfg.getPlus() * 10L);
            } else {
                timeMinus = firstTime.getOffsetUTCTime(-roCfg.getMinus() * 10L);
                timePlus = lastTime.getOffsetUTCTime(roCfg.getPlus() * 10L);
            }
            break;
        default:
            LOG.error("Unknown ReadoutType: " + type +
                      " -> Making it GLOBAL");
            type = IReadoutRequestElement.READOUT_TYPE_GLOBAL;
            timeMinus = firstTime.getOffsetUTCTime(-roCfg.getMinus() * 10L);
            timePlus = lastTime.getOffsetUTCTime(roCfg.getPlus() * 10L);
            break;
        }

        int rreSrcId;
        if (stringId == null) {
            rreSrcId = -1;
        } else {
            rreSrcId = stringId.getSourceID();
        }

        long rreDomId;
        if (domId == null) {
            rreDomId = -1;
        } else {
            rreDomId = domId.longValue();
        }

        return new ReadoutRequestElement(type, rreSrcId,
                                         timeMinus.longValue(),
                                         timePlus.longValue(),
                                         rreDomId);
    }

    /**
     * Flush the algorithm.
     */
    @Override
    public abstract void flush();

    protected void formTrigger(IUTCTime time)
    {
        formTrigger(null, time, time, null, null);
    }

    protected void formTrigger(IHitPayload hit, IDOMID dom, ISourceID string)
    {
        List<IHitPayload> hitList = new ArrayList<IHitPayload>(1);
        hitList.add(hit);

        formTrigger(hitList, dom, string);
    }

    protected void formTrigger(List<IHitPayload> hits, IDOMID dom,
                               ISourceID string)
    {
        final int numberOfHits = hits.size();
        if (numberOfHits == 0) {
            throw new Error("Cannot form trigger from empty list of hits");
        }

        formTrigger(hits, hits.get(0).getPayloadTimeUTC(),
                    hits.get(numberOfHits - 1).getPayloadTimeUTC(), dom,
                    string);
    }

    protected void formTrigger(List<IHitPayload> hits)
    {
        final int num = hits.size();
        if (num == 0) {
            throw new Error("Cannot form trigger from empty list of hits");
        }

        formTrigger(hits, hits.get(0).getPayloadTimeUTC(),
                    hits.get(num - 1).getPayloadTimeUTC(), null, null);
    }

    private void formTrigger(Collection<IHitPayload> hits, IUTCTime firstTime,
                             IUTCTime lastTime, IDOMID dom, ISourceID string)
    {
        if (null == triggerFactory) {
            throw new Error("TriggerFactory is not set!");
        }

        // copy hits so they can be recycled
        ArrayList<IPayload> hitList = new ArrayList<IPayload>();
        if (hits != null) {
            if (hits.size() == 0) {
                throw new Error("Cannot form trigger from empty list of hits");
            }

            for (IHitPayload hit : hits) {
                IHitPayload copy = (IHitPayload) hit.deepCopy();
                if (copy.getUTCTime() < 0) {
                    LOG.error("Ignoring bad hit " + copy + " (from " + hit +
                              ")");
                    continue;
                }
                hitList.add(copy);
            }
        }

        // create readout requests
        ArrayList<IReadoutRequestElement> readoutElements =
            new ArrayList<IReadoutRequestElement>();
        Iterator readoutIter = readouts.iterator();
        while (readoutIter.hasNext()) {
            TriggerReadout readout = (TriggerReadout) readoutIter.next();
            readoutElements.add(createReadoutElement(firstTime, lastTime,
                                                     readout, dom, string));
        }

        final int uid = getNextUID();

        IReadoutRequest readoutRequest =
            new ReadoutRequest(firstTime.longValue(), uid, srcId,
                               readoutElements);

        // make payload
        TriggerRequest triggerPayload =
            (TriggerRequest) triggerFactory.createPayload(uid,
                                                          getTriggerType(),
                                                          trigCfgId, srcId,
                                                          firstTime.longValue(),
                                                          lastTime.longValue(),
                                                          readoutRequest,
                                                          hitList);

        // report it
        reportTrigger(triggerPayload);

        // set earliest payload of interest to 1/10 ns after the last hit
        IPayload dummy = new DummyPayload(lastTime.getOffsetUTCTime(1));
        setEarliestPayloadOfInterest(dummy);
    }

    static DOMInfo getDOMFromHit(IDOMRegistry registry, IHitPayload hit)
    {
        if (hit.hasChannelID()) {
            return registry.getDom(hit.getChannelID());
        }

        return registry.getDom(hit.getDOMID().longValue());
    }

    /**
     * Get the earliest payload of interest for this algorithm.
     *
     * @return earliest payload
     */
    @Override
    public IPayload getEarliestPayloadOfInterest()
    {
        return earliestPayloadOfInterest;
    }

    /**
     * Get the earliest event time of interest for this algorithm.
     *
     * @return earliest UTC time
     */
    @Override
    public long getEarliestTime()
    {
        final long val;
        synchronized (this) {
            if (earliestPayloadOfInterest == null) {
                return 0;
            }

            val = earliestPayloadOfInterest.getUTCTime();
        }

        if (earliestMonitorTime == Long.MIN_VALUE) {
            earliestMonitorTime = val;
        }

        if (val == Long.MAX_VALUE) {
            return earliestMonitorTime;
        }

        return val;
    }

    /**
     * Get this hit's type.
     *
     * @param hit hit to evaluate
     *
     * @return hit type
     */
    public static int getHitType(IHitPayload hit)
    {
        return hit.getTriggerType() & 0xf;
    }

    /**
     * Get the input queue size.
     *
     * @return input queue size
     */
    @Override
    public int getInputQueueSize()
    {
        if (subscriber == null) {
            return -1;
        }

        return subscriber.size();
    }

    /**
     * Get the next interval.
     *
     * @param interval interval being considered
     *
     * @return next interval
     */
    @Override
    public Interval getInterval(Interval interval)
    {
        if (interval.isEmpty()) {
            Interval rtnInterval;
            synchronized (requests) {
                if (requests.size() == 0) {
                    rtnInterval = interval;
                } else {
                    ITriggerRequestPayload req = requests.get(0);

                    rtnInterval =
                        new Interval(req.getFirstTimeUTC().longValue(),
                                     req.getLastTimeUTC().longValue());
                }
            }

            return rtnInterval;
        }

        final long earliest;
        synchronized (this) {
            if (earliestPayloadOfInterest == null) {
                earliest = 0L;
            } else {
                earliest = earliestPayloadOfInterest.getUTCTime();
            }
        }

        long start = interval.start;
        long end = interval.end;

        synchronized (requests) {
            for (ITriggerRequestPayload req : requests) {
                long firstTime = req.getFirstTimeUTC().longValue();
                long lastTime = req.getLastTimeUTC().longValue();

                // if this request precedes the interval, start a new interval
                if (start > lastTime) {
                    start = firstTime;
                    end = lastTime;
                    break;
                }

                // if this request is significantly past the interval,
                //  we're done
                if (end + REQUEST_WIDTH < lastTime) {
                    break;
                }

                // if this request is past the end of the interval, ignore it
                if (end < firstTime) {
                    continue;
                }

                // if necessary, widen the interval
                if (firstTime < start) {
                    start = firstTime;
                }
                if (lastTime > end) {
                    end = lastTime;
                }
            }

            if (srcId == SourceIdRegistry.GLOBAL_TRIGGER_SOURCE_ID) {
                if (requests.size() > 0) {
                    final ITriggerRequestPayload lastReq =
                        requests.get(requests.size() - 1);

                    final long finalTime =
                        lastReq.getLastTimeUTC().longValue();
                    // wait until the interval is significantly past
                    // the most recent request
                    if (end + REQUEST_WIDTH > finalTime) {
                        return null;
                    }
                }
            }

            // if we're past the earliest payload, give up
            if (end > earliest) {
                return null;
            }
        }

        // if this trigger is still interested in hits within the interval,
        //  return null to signal that the current interval is invalid
        if (earliestPayloadOfInterest == null ||
            (earliest != FlushRequest.FLUSH_TIME &&
             (start >= earliest || end >= earliest)))
        {
            return null;
        }

        if (start == interval.start && end == interval.end) {
            return interval;
        }

        return new Interval(start, end);
    }

    /**
     * Return the difference between the start of the first cached request
     * and the earliest payload of interest (in DAQ ticks).
     *
     * @return latency in DAQ ticks
     */
    @Override
    public long getLatency()
    {
        final long reqStart;
        synchronized (requests) {
            if (requests.size() == 0) {
                return 0L;
            }

            reqStart = requests.get(0).getFirstTimeUTC().longValue();
        }

        synchronized (this) {
            if (earliestPayloadOfInterest == null) {
                return 0L;
            }

            // latency is the difference between the start time of the oldest
            // request and the earliest payload of interest
            return earliestPayloadOfInterest.getUTCTime() - reqStart;
        }
    }

    /**
     * Get the monitoring name.
     *
     * @return the name used for monitoring this trigger
     */
    @Override
    public abstract String getMonitoringName();

    /**
     * Get the next request UID.
     * NOTE: this increments the trigger-wide UID counter
     *
     * @return next uid
     */
    public int getNextUID()
    {
        return triggerCounter++;
    }

    /**
     * Get number of cached requests.
     *
     * @return number of cached requests
     */
    @Override
    public int getNumberOfCachedRequests()
    {
        return requests.size();
    }

    /**
     * Get the time of the last released trigger.
     *
     * @return release time
     */
    @Override
    public long getReleaseTime()
    {
        return releaseTime;
    }

    /**
     * Get the number of trigger intervals sent to the collector.
     *
     * @return sent count
     */
    @Override
    public long getSentTriggerCount()
    {
        return sentTriggerCounter;
    }

    /**
     * Get the source ID.
     *
     * @return source ID
     */
    @Override
    public int getSourceId()
    {
        return srcId;
    }

    @Override
    public PayloadSubscriber getSubscriber()
    {
        return subscriber;
    }

    /**
     * Get the configuration ID.
     *
     * @return configuration ID
     */
    @Override
    public int getTriggerConfigId()
    {
        return trigCfgId;
    }

    /**
     * Get the ID of the most recent trigger request.
     *
     * @return counter value
     */
    @Override
    public int getTriggerCounter()
    {
        return triggerCounter;
    }

    /**
     * Get the trigger handler.
     *
     * @return trigger handler
     * @deprecated use getTriggerManager()
     */
    public ITriggerManager getTriggerHandler()
    {
        return mgr;
    }

    /**
     * Get the trigger manager.
     *
     * @return trigger manager
     */
    public ITriggerManager getTriggerManager()
    {
        return mgr;
    }

    /**
     * Get the map of monitored quantitied for this algorithm.
     *
     * @return map of monitored quantity names and values
     */
    @Override
    public Map<String, Object> getTriggerMonitorMap()
    {
        return null;
    }

    /**
     * Get trigger name.
     * @return triggerName
     */
    @Override
    public String getTriggerName()
    {
        return triggerName;
    }

    /**
     * Are there requests waiting to be processed?
     *
     * @return <tt>true</tt> if there are non-flush requests available
     */
    @Override
    public boolean hasCachedRequests()
    {
        synchronized (requests) {
            return requests.size() > 0 &&
                requests.get(0).getUID() != FlushRequest.UID;
        }
    }

    /**
     * Is there data available?
     *
     * @return <tt>true</tt> if there are more payloads available
     */
    @Override
    public boolean hasData()
    {
        if (subscriber == null) {
            return false;
        }

        return subscriber.hasData();
    }

    /**
     * Has this algorithm been fully configured?
     *
     * @return <tt>true</tt> if the algorithm has been fully configured
     */
    @Override
    public abstract boolean isConfigured();

    /**
     * Has this algorithm's input stream been stopped?
     *
     * @return <tt>true</tt> if the algorithm's input stream has stopped
     */
    @Override
    public boolean isStopped()
    {
        return subscriber.isStopped() && !hasCachedRequests();
    }

    /**
     * Recycle all unused requests still cached in the algorithms.
     */
    @Override
    public void recycleUnusedRequests()
    {
        int count = 0;
        for (ITriggerRequestPayload req : requests) {
            req.recycle();
            if (!(req instanceof FlushRequest)) {
                count++;
            }
        }
        requests.clear();

        if (count > 0) {
            LOG.error("Recycled " + count + " unused " + toString() +
                      " requests");
        }
    }

    /**
     * Add all requests in the interval to the list of released requests.
     *
     * @param interval time interval to check
     * @param released list of released requests
     *
     * @return number of released requests
     */
    public int release(Interval interval,
                       List<ITriggerRequestPayload> released)
    {
        int num = 0;
        synchronized (requests) {
            int i = 0;
            for (ITriggerRequestPayload req : requests) {
                if (interval.start > req.getFirstTimeUTC().longValue() ||
                    interval.start > req.getLastTimeUTC().longValue())
                {
                    // yikes, found a request preceding the interval!
                    LOG.error("Found request " + req +
                              " before start of interval " + interval +
                              " (startDiff " +
                              (interval.start -
                               req.getFirstTimeUTC().longValue()) +
                              ", endDiff " +
                              (interval.start -
                               req.getLastTimeUTC().longValue()) + ")");
                    break;
                }

                if (interval.end < req.getLastTimeUTC().longValue()) {
                    // if request is past the end of the interval, we're done
                    break;
                }

                i++;
            }

            // release all requests found in the interval
            if (i > 0) {
                List<ITriggerRequestPayload> sub =
                    requests.subList(0, i);

                // save the last released time
                ITriggerRequestPayload req = sub.get(i-1);
                if (req instanceof FlushRequest) {
                    releaseTime = req.getUTCTime();
                } else {
                    IUTCTime lastTime = req.getLastTimeUTC();
                    if (lastTime == null) {
                        LOG.error("Last time is not set for " + req);
                    } else {
                        releaseTime = lastTime.longValue();
                    }
                }

                // add released requests to the list and remove from the cache
                released.addAll(sub);
                num += sub.size();
                sub.clear();
            }
        }

        return num;
    }

    public void reportHit(IHitPayload hit)
    {
        throw new UnimplementedError();
    }

    /**
     * Report the trigger request.
     *
     * @param trigReq new request
     */
    public void reportTrigger(ITriggerRequestPayload trigReq)
    {
        // if prescaling, should we throw out this trigger request?
        if ((triggerPrescale != 0) &&
            ((triggerCounter % triggerPrescale) != 0))
        {
            trigReq.recycle();
        } else {
            synchronized (requests) {
                requests.add(trigReq);
                if (releaseTime != Long.MIN_VALUE &&
                    trigReq.getFirstTimeUTC().longValue() < releaseTime)
                {
                    LOG.error(triggerName + " added " + trigReq +
                              " preceding release time " + releaseTime);
                }
                sentTriggerCounter++;
            }

            collector.setChanged();
        }
    }

    /**
     * Reset the algorithm to its initial condition.
     */
    @Override
    public void resetAlgorithm()
    {
        resetUID();

        onTrigger = false;
        earliestPayloadOfInterest = null;
        releaseTime = Long.MIN_VALUE;
    }

    /**
     * Reset the UID to signal a run switch.
     */
    @Override
    public void resetUID()
    {
        triggerCounter = 0;
    }

    /**
     * Run trigger algorithm on the payload.
     *
     * @param payload payload to process
     *
     * @throws TriggerException if there was a problem running the algorithm
     */
    @Override
    public abstract void runTrigger(IPayload payload)
        throws TriggerException;

    /**
     * Clear out all remaining payloads.
     */
    @Override
    public void sendLast()
    {
        flush();

        FlushRequest flushReq = new FlushRequest();
        setEarliestPayloadOfInterest(flushReq);
        synchronized (requests) {
            requests.add(flushReq);
        }
        collector.setChanged();
    }

    /**
     * Set DOM set ID
     *
     * @param domSetId DOM set ID
     *
     * @throws ConfigException if the DOMSet ID was not valid
     */
    public void setDomSetId(int domSetId)
        throws ConfigException
    {
        this.domSetId = domSetId;
        configHitFilter(domSetId);
    }

    /**
     * Set earliest hit to keep.
     *
     * @param payload earliest payload
     */
    protected void setEarliestPayloadOfInterest(IPayload payload)
    {
        synchronized (this) {
            if (earliestPayloadOfInterest != null &&
                earliestPayloadOfInterest.getUTCTime() > payload.getUTCTime())
            {
                long diff = earliestPayloadOfInterest.getUTCTime() -
                    payload.getUTCTime();
                LOG.error("Earliest time went " + diff + " ticks backward");
            }

            earliestPayloadOfInterest = payload;
            mgr.setEarliestPayloadOfInterest(earliestPayloadOfInterest);
        }
    }

    /**
     * Set source ID.
     *
     * @param val source ID
     */
    @Override
    public void setSourceId(int val)
    {
        srcId = val;
    }

    /**
     * Set the list subscriber client (for monitoring the input queue).
     *
     * @param subscriber input queue subscriber
     */
    @Override
    public void setSubscriber(PayloadSubscriber subscriber)
    {
        if (this.subscriber != null) {
            throw new Error(triggerName +
                            " is already subscribed to an input queue");
        }

        this.subscriber = subscriber;
    }

    /**
     * Set request collector.
     *
     * @param collector trigger collector
     */
    @Override
    public void setTriggerCollector(ITriggerCollector collector)
    {
        this.collector = collector;
    }

    /**
     * Set configuration ID.
     *
     * @param val configuration ID
     */
    @Override
    public void setTriggerConfigId(int val)
    {
        trigCfgId = val;
    }

    /**
     * Set the factory used to create trigger requests.
     *
     * @param triggerFactory trigger factory
     */
    @Override
    public void setTriggerFactory(TriggerRequestFactory triggerFactory)
    {
        this.triggerFactory = triggerFactory;
    }

    /**
     * Set the trigger manager for this trigger.
     *
     * @param mgr trigger manager
     */
    @Override
    public void setTriggerManager(ITriggerManager mgr)
    {
        this.mgr = mgr;
    }

    /**
     * Set trigger name.
     *
     * @param triggerName trigger name
     */
    @Override
    public void setTriggerName(String triggerName)
    {
        this.triggerName = triggerName;
        if (LOG.isDebugEnabled()) {
            LOG.debug("TriggerName = " + triggerName);
        }
    }

    /**
     * Unset the list subscriber client (for monitoring the input queue).
     */
    @Override
    public void unsubscribe(SubscribedList list)
    {
        if (subscriber == null) {
            LOG.warn(triggerName + " is not subscribed to the input queue");
            return;
        }

        if (!list.unsubscribe(subscriber)) {
            LOG.warn(triggerName +
                     " was not fully unsubscribed from the input queue");
        }

        subscriber = null;
    }

    /**
     * Wrap the trigger in a global trigger jacket and report it.
     *
     * @param trigReq trigger request
     */
    public void wrapTrigger(ITriggerRequestPayload trigReq)
    {
        // rReq is the same for a single payload.
        IReadoutRequest rReq =
            trigReq.getReadoutRequest();

        List elems;
        if (rReq == null) {
            elems = null;
        } else {
            elems = rReq.getReadoutRequestElements();
        }

        final long rReqTime;
        if (rReq == null) {
            rReqTime = trigReq.getUTCTime();
        } else {
            rReqTime = rReq.getUTCTime();
            if (rReqTime != trigReq.getUTCTime()) {
                LOG.error(String.format("Readout request UTC Time %d does" +
                                        " not match trigger request time %d",
                                        rReqTime, trigReq.getUTCTime()));
            }
        }

        IUTCTime earliest;
        IUTCTime latest;

        if (rReq == null || elems == null || elems.size() == 0) {
            earliest = trigReq.getFirstTimeUTC();
            latest = trigReq.getLastTimeUTC();
        } else {
            earliest = null;
            latest = null;
            for (Object obj : elems) {
                IReadoutRequestElement rrElem = (IReadoutRequestElement) obj;

                if (earliest == null ||
                    earliest.longValue() >
                    rrElem.getFirstTimeUTC().longValue())
                {
                    earliest = rrElem.getFirstTimeUTC();
                }

                if (latest == null ||
                    latest.longValue() < rrElem.getLastTimeUTC().longValue())
                {
                    latest = rrElem.getLastTimeUTC();
                }
            }

            if (earliest == null || latest == null) {
                throw new Error("Couldn't find earliest/latest time for " +
                                rReq);
            }
        }

        List list = new ArrayList();
        list.add(trigReq.deepCopy());

        final int uid = getNextUID();

        IReadoutRequest newRdoutReq =
            new ReadoutRequest(rReqTime, uid, srcId, elems);

        ITriggerRequestPayload newReq =
            (ITriggerRequestPayload) triggerFactory.
            createPayload(uid, getTriggerType(), getTriggerConfigId(),
                          srcId, earliest.longValue(), latest.longValue(),
                          newRdoutReq, list);

        reportTrigger(newReq);

        setEarliestPayloadOfInterest(new DummyPayload(earliest));
    }

    /**
     * Debugging string.
     *
     * @return debugging string
     */
    @Override
    public String toString()
    {
        return triggerName + "[" + subscriber.size() + "=>" + requests.size() +
            "=>" + sentTriggerCounter + "]";
    }
}
