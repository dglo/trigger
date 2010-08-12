/*
 * class: DummyPayload
 *
 * Version $Id: DummyPayload.java 4574 2009-08-28 21:32:32Z dglo $
 *
 * Date: October 7 2005
 *
 * (c) 2005 IceCube Collaboration
 */

package icecube.daq.trigger.control;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IPayload;
import icecube.daq.payload.IUTCTime;
import icecube.daq.splicer.Spliceable;

import java.nio.ByteBuffer;

/**
 * This class is a dummy payload that only has a UTC time associated with it.
 * Its main purpose is for truncating the Splicer.
 *
 * @version $Id: DummyPayload.java 4574 2009-08-28 21:32:32Z dglo $
 * @author pat
 */
public class DummyPayload implements Spliceable, IPayload
{

    private IUTCTime payloadTimeUTC;

    public DummyPayload(IUTCTime payloadTimeUTC) {
        this.payloadTimeUTC = payloadTimeUTC;
    }

    public ByteBuffer getPayloadBacking()
    {
        throw new Error("Unimplemented");
    }

    /**
     * returns the length in bytes of this payload
     */
    public int getPayloadLength() {
        return 0;
    }

    /**
     * returns the Payload type
     */
    public int getPayloadType() {
        return -1;
    }

    /**
     * returns the Payload interface type as defined in the PayloadInterfaceRegistry.
     *
     * @return int ... one of the defined types in icecube.daq.payload.PayloadInterfaceRegistry
     */
    public int getPayloadInterfaceType() {
        return -1;
    }

    /**
     * gets the UTC time tag of a payload
     */
    public IUTCTime getPayloadTimeUTC() {
        return payloadTimeUTC;
    }

    public int compareSpliceable(Spliceable spl) {
        return payloadTimeUTC.compareTo(((IPayload) spl).getPayloadTimeUTC());
    }

    /**
     * This method allows a deepCopy of itself.
     *
     * @return Object which is a copy of the object which implements this interface.
     */
    public Object deepCopy() {
        return new DummyPayload(this.payloadTimeUTC);
    }

    public void setCache(IByteBufferCache cache)
    {
        throw new Error("Unimplemented");
    }

    public String toString()
    {
        return "Dummy@" + payloadTimeUTC;
    }
}
