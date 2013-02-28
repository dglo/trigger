package icecube.daq.trigger.test;

import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.payload.IWriteablePayload;
import icecube.daq.payload.PayloadChecker;
import icecube.daq.splicer.SplicerException;
import icecube.daq.splicer.StrandTail;
import icecube.daq.trigger.algorithm.AbstractTrigger;
import icecube.daq.trigger.exceptions.TriggerException;
import icecube.daq.util.DOMRegistry;
import icecube.daq.util.DeployedDOM;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Triggers described in sps-icecube-amanda-008.xml
 */
public class SPS2012Triggers
    extends TriggerCollection
{
    private DOMRegistry registry;
    private boolean checkSequentialTimes;
    private int numHitsPerTrigger;
    private long timeBase;
    private long timeStep;

    public SPS2012Triggers(DOMRegistry registry)
        throws TriggerException
    {
        this(registry, true);
    }

    public SPS2012Triggers(DOMRegistry registry, boolean checkSequentialTimes)
        throws TriggerException
    {
        this.registry = registry;
        this.checkSequentialTimes = checkSequentialTimes;

        AbstractTrigger trig;

        trig = createTrigger(3, -1, GLOBAL_TRIGGER, "ThroughputTrigger");
        add(trig);

        trig = createTrigger(13, 106, ICETOP_TRIGGER, "PhysicsMinBiasTrigger");
        trig.addParameter("deadtime", "5000");
        trig.addParameter("prescale", "200");
        trig.addReadout(1, 0, 10000, 10000);
        trig.addReadout(2, 0, 4000, 6000);
        add(trig);

        trig = createTrigger(0, 102, ICETOP_TRIGGER, "SimpleMajorityTrigger");
        trig.addParameter("threshold", "6");
        trig.addParameter("timeWindow", "5000");
        trig.addReadout(0, 0, 10000, 10000);
        add(trig);

        trig = createTrigger(1, 1009, ICETOP_TRIGGER, "CalibrationTrigger");
        trig.addParameter("hitType", "4");
        trig.addReadout(0, 0, 1000, 1000);
        add(trig);

        trig = createTrigger(2, 101, ICETOP_TRIGGER, "MinBiasTrigger");
        trig.addParameter("prescale", "10000");
        trig.addReadout(0, 0, 10000, 10000);
        add(trig);

        trig = createTrigger(23, 23050, INICE_TRIGGER, "FixedRateTrigger");
        trig.addParameter("interval", "30000000000");
        trig.addReadout(0, 0, 5000000, 5000000);
        add(trig);

        trig = createTrigger(13, 106, INICE_TRIGGER, "PhysicsMinBiasTrigger");
        trig.addParameter("deadtime", "5000");
        trig.addParameter("prescale", "200");
        trig.addReadout(1, 0, 10000, 10000);
        trig.addReadout(2, 0, 4000, 6000);
        add(trig);

        trig = createTrigger(0, 1006, INICE_TRIGGER, "SimpleMajorityTrigger");
        trig.addParameter("threshold", "8");
        trig.addParameter("timeWindow", "5000");
        trig.addReadout(1, 0, 10000, 10000);
        trig.addReadout(2, 0, 4000, 6000);
        add(trig);

        trig = createTrigger(24, 24002, INICE_TRIGGER, "SlowMPTrigger");
        trig.addParameter("t_proximity", "2500");
        trig.addParameter("t_max", "500000");
        trig.addParameter("alpha_min", "140");
        trig.addParameter("dc_algo", "false");
        trig.addParameter("rel_v", "0.5");
        trig.addParameter("min_n_tuples", "5");
        trig.addParameter("max_event_length", "5000000");
        trig.addReadout(1, 0, 10000, 10000);
        trig.addReadout(2, 0, 4000, 6000);
        add(trig);

        trig = createTrigger(21, 21001, INICE_TRIGGER, "CylinderTrigger");
        trig.addParameter("multiplicity", "4");
        trig.addParameter("simpleMultiplicity", "8");
        trig.addParameter("radius", "175");
        trig.addParameter("height", "75");
        trig.addParameter("timeWindow", "1000");
        trig.addParameter("domSet", "2");
        trig.addReadout(1, 0, 10000, 10000);
        trig.addReadout(2, 0, 4000, 6000);
        add(trig);

        trig = createTrigger(14, 1007, INICE_TRIGGER, "ClusterTrigger");
        trig.addParameter("multiplicity", "5");
        trig.addParameter("coherenceLength", "7");
        trig.addParameter("timeWindow", "1500");
        trig.addParameter("domSet", "2");
        trig.addReadout(1, 0, 10000, 10000);
        trig.addReadout(2, 0, 4000, 6000);
        add(trig);

        trig = createTrigger(0, 1011, INICE_TRIGGER, "SimpleMajorityTrigger");
        trig.addParameter("threshold", "3");
        trig.addParameter("timeWindow", "2500");
        trig.addParameter("domSet", "6");
        trig.addReadout(1, 0, 10000, 10000);
        trig.addReadout(2, 0, 6000, 6000);
        add(trig);

        numHitsPerTrigger = 8;
        timeBase = 100000L;
        timeStep = 5000L / (long) (numHitsPerTrigger + 1);
    }

    public BaseValidator getAmandaValidator()
    {
        throw new Error("Unimplemented");
    }

    public int getExpectedNumberOfAmandaPayloads(int numObjs)
    {
        throw new Error("Unimplemented");
    }

    public int getExpectedNumberOfInIcePayloads(int numObjs)
    {
        return numObjs / numHitsPerTrigger;
    }

    public BaseValidator getInIceValidator()
    {
        return new InIceValidator();
    }

    public void sendAmandaData(WritableByteChannel[] x0, int i1)
        throws IOException
    {
        throw new Error("Unimplemented");
    }

    public void sendAmandaStops(WritableByteChannel[] x0)
        throws IOException
    {
        throw new Error("Unimplemented");
    }

    public void sendInIceData(WritableByteChannel[] tails, int numObjs)
        throws IOException
    {
        java.util.Iterator<String> domIter = registry.keys().iterator();

        for (int i = 0; i < numObjs; i++) {
            final long time;
            if (i == 0) {
                time = timeBase;
            } else {
                time = (timeBase * (((i - 1) / numHitsPerTrigger) + 1)) +
                    (timeStep * i);
            }

            DeployedDOM dom = registry.getDom(domIter.next());

            final int tailIndex = i % tails.length;
            sendHit(tails[tailIndex], time, tailIndex,
                    dom.getNumericMainboardId());
        }
    }

    public void sendInIceStops(WritableByteChannel[] tails)
        throws IOException
    {
        sendStops(tails);
    }

    class InIceValidator
        extends BaseValidator
    {
        private long timeSpan;

        private boolean jumpHack = true;

        private long nextStart;
        private long nextEnd;

        InIceValidator()
        {
            timeSpan = timeStep * (long) numHitsPerTrigger;

            nextStart = timeBase;
            nextEnd = nextStart + timeSpan;
        }

        public boolean validate(IWriteablePayload payload)
        {
            if (!(payload instanceof ITriggerRequestPayload)) {
                throw new Error("Unexpected payload " +
                                payload.getClass().getName());
            }

            //dumpPayloadBytes(payload);

            ITriggerRequestPayload tr = (ITriggerRequestPayload) payload;

            if (!PayloadChecker.validateTriggerRequest(tr, true)) {
                throw new Error("Trigger request is not valid");
            }

            long firstTime = getUTC(tr.getFirstTimeUTC());
            long lastTime = getUTC(tr.getLastTimeUTC());

            if (checkSequentialTimes) {
                if (firstTime != nextStart) {
                    throw new Error("Expected first trigger time " + nextStart +
                                    ", not " + firstTime);
                } else if (lastTime != nextEnd) {
                    throw new Error("Expected last trigger time " + nextEnd +
                                    ", not " + lastTime);
                }
            }
/*
System.err.println("First: EXP "+nextStart+" ACT "+firstTime+" DIFF "+(firstTime-nextStart));
System.err.println(" Last: EXP "+nextEnd+" ACT "+lastTime+" DIFF "+(lastTime-nextEnd));
*/

            nextStart = firstTime + timeBase + timeSpan;
            if (jumpHack) {
                nextStart += timeStep;
                jumpHack = false;
            }
            nextEnd = lastTime + timeBase + timeSpan;

            return true;
        }
    }
}