package icecube.daq.trigger.control;

import icecube.daq.payload.IPayload;
import icecube.daq.trigger.algorithm.ITriggerAlgorithm;

import org.apache.log4j.Logger;

/**
 * Trigger algorithm thread.
 */
public class TriggerThread
    implements Runnable
{
    /** Message logger. */
    private static final Logger LOG = Logger.getLogger(TriggerThread.class);

    private int id;
    private ITriggerAlgorithm algorithm;
    private Thread thread;
    private boolean stopping;
    private boolean stopped;
    private long numSent;

    public TriggerThread(int id, ITriggerAlgorithm algorithm)
    {
        if (algorithm == null) {
            throw new Error("Algorithm cannot be null");
        }

        this.id = id;
        this.algorithm = algorithm;
    }

    public boolean isStopped()
    {
        return stopped;
    }

    public void join()
    {
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException ie) {
                // ignore interrupts
            }
        }
    }

    public void start()
    {
        if (algorithm.getSubscriber() == null) {
            throw new Error(algorithm.toString() +
                            " is not subscribed to any lists");
        }

        thread = new Thread(this);
        thread.setName(algorithm.getTriggerName() + "-Thread");
        thread.start();
    }

    public void stop()
    {
        stopping = true;

        PayloadSubscriber sub = algorithm.getSubscriber();
        if (sub == null) {
            LOG.error(algorithm.toString() +
                      " is not subscribed to any lists");
        } else {
            sub.stop();
        }
    }

    @Override
    public void run()
    {
        while (true) {
            PayloadSubscriber sub = algorithm.getSubscriber();
            if (sub == null) {
                // if there's no subscriber, we're done
                break;
            }

            IPayload pay = sub.pop();
            if (pay == PayloadSubscriber.STOPPED_PAYLOAD) {
                if (!sub.isStopped()) {
                    // miscoded subscriber, STOPPED_PAYLOAD means stopped
                    LOG.error("Ignoring STOPPED_PAYLOAD for " +
                              algorithm.getTriggerName());
                }
                else
                {
                    // subscriber/trigger thread are stopped sequentially so
                    // spin momentarily waiting to receive stop notification
                    int spin = 0;
                    while (!stopping && spin++ < 10)
                    {
                        try{ Thread.sleep(100);} catch (InterruptedException e){}
                    }

                    if(stopping)
                    {
                        break;
                    }
                    else
                    {
                        throw  new Error("Unexpected stop sequence");
                    }
                }

            } else if (pay == TriggerManager.FLUSH_PAYLOAD) {
                algorithm.sendLast();
            } else {
                numSent++;
                try {
                    algorithm.runTrigger(pay);
                } catch (Throwable thr) {
                    LOG.error("Trigger " + algorithm + " failed for " + pay,
                              thr);
                }
            }
        }

        stopped = true;
    }

    @Override
    public String toString()
    {
        PayloadSubscriber sub = algorithm.getSubscriber();

        int qlen;
        if (sub == null) {
            qlen = -1;
        } else {
            qlen = sub.size();
        }

        return "TriggerThread#" + id +
            ":" + algorithm.getTriggerName() +
            "[" + sub + "]" +
            ",sent#" + numSent +
            ",q#" + qlen +
            (stopping ? ":stopping" : "") +
            (stopped ? ":stopped" : "");
    }
}
