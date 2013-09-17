package icecube.daq.trigger.test;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.IReadoutRequest;
import icecube.daq.payload.IReadoutRequestElement;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.IUTCTime;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

public class MockReadoutRequest
    implements ILoadablePayload, IReadoutRequest
{
    private int uid;
    private int srcId;
    private List<IReadoutRequestElement> elemList;

    private ISourceID srcObj;

    public MockReadoutRequest()
    {
        this(-1, -1, null);
    }

    public MockReadoutRequest(IReadoutRequest rReq)
    {
        this(rReq.getUID(), rReq.getSourceID().getSourceID(),
             rReq.getReadoutRequestElements());
    }

    public MockReadoutRequest(int uid, int srcId)
    {
        this(uid, srcId, null);
    }

    public MockReadoutRequest(int uid, int srcId,
                              List<IReadoutRequestElement> elemList)
    {
        this.uid = uid;
        this.srcId = srcId;
        this.elemList = elemList;
    }

    public void addElement(IReadoutRequestElement elem)
    {
        if (elemList == null) {
            elemList = new ArrayList<IReadoutRequestElement>();
        }

        elemList.add(elem);
    }

    public void addElement(int type, int srcId, long firstTime, long lastTime,
                           long domId)
    {
        addElement(new MockReadoutRequestElement(type, firstTime, lastTime,
                                                 domId, srcId));
    }

    public Object deepCopy()
    {
        throw new Error("Unimplemented");
    }

    public int getEmbeddedLength()
    {
        throw new Error("Unimplemented");
    }

    public ByteBuffer getPayloadBacking()
    {
        throw new Error("Unimplemented");
    }

    public int getPayloadInterfaceType()
    {
        throw new Error("Unimplemented");
    }

    public int getPayloadLength()
    {
        throw new Error("Unimplemented");
    }

    public IUTCTime getPayloadTimeUTC()
    {
        throw new Error("Unimplemented");
    }

    public int getPayloadType()
    {
        throw new Error("Unimplemented");
    }

    public List getReadoutRequestElements()
    {
        if (elemList == null) {
            elemList = new ArrayList();
        }

        return elemList;
    }

    public ISourceID getSourceID()
    {
        if (srcObj == null && srcId != IReadoutRequestElement.NO_STRING) {
            srcObj = new MockSourceID(srcId);
        }

        return srcObj;
    }

    public int getUID()
    {
        return uid;
    }

    public long getUTCTime()
    {
        throw new Error("Unimplemented");
    }

    public int length()
    {
        throw new Error("Unimplemented");
    }

    public void loadPayload()
        throws IOException, DataFormatException
    {
        // do nothing
    }

    public int putBody(ByteBuffer buf, int offset)
    {
        throw new Error("Unimplemented");
    }

    public void recycle()
    {
        // do nothing
    }

    public void setCache(IByteBufferCache cache)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Set the source ID. Needed for backward compatiblility with the old
     * global request handler implementation.
     *
     * @param srcId new source ID
     */
    public void setSourceID(ISourceID srcId)
    {
        if (srcId == null) {
            throw new Error("Source ID cannot be null");
        }

        this.srcId = srcId.getSourceID();

        // clear cached ISourceID
        srcObj = null;
    }

    /**
     * Set the universal ID for global requests which will become events.
     *
     * @param uid new UID
     */
    public void setUID(int uid)
    {
        throw new Error("Unimplemented");
    }

    public String toString()
    {
        return "MockRdoutReq[" + uid +
            " src " + MockSourceID.toString(srcId) +
            " elem*" + elemList.size() + "]";
    }
}
