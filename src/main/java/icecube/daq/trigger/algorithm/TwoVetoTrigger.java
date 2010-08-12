/*
 * class: TwoVetoTrigger
 *
 * Version $Id: TwoVetoTrigger.java 4574 2009-08-28 21:32:32Z dglo $
 *
 * Date: January 25 2006
 *
 * (c) 2006 IceCube Collaboration
 */

package icecube.daq.trigger.algorithm;

import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.trigger.config.TriggerParameter;
import icecube.daq.trigger.exceptions.IllegalParameterValueException;
import icecube.daq.trigger.exceptions.UnknownParameterException;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is accept all incoming triggers but to veto two configured triggers.
 *
 * @version $Id: TwoVetoTrigger.java 4574 2009-08-28 21:32:32Z dglo $
 * @author shseo
 */
public class TwoVetoTrigger
        extends VetoTrigger
{
    /**
    * Log object for this class
    */
    private static final Log log = LogFactory.getLog(TwoVetoTrigger.class);

    private int miConfiguredTriggerType_1;
    private int miConfiguredTriggerConfigId_1;
    private int miConfiguredSourceId_1;

    private int miConfiguredTriggerType_2;
    private int miConfiguredTriggerConfigId_2;
    private int miConfiguredSourceId_2;

    private static final int miUnconfiguredTrigId = -1;
    private static final int miConfiguredTrigId_1 = 1;
    private static final int miConfiguredTrigId_2 = 2;

    private boolean mbConfigTriggerType_1;
    private boolean mbConfigTriggerConfigId_1;
    private boolean mbConfigSourceId_1;
    private boolean mbConfigTriggerType_2;
    private boolean mbConfigTriggerConfigId_2;
    private boolean mbConfigSourceId_2;
    private boolean configTriggerType_3;
    private boolean configTriggerConfigId_3;
    private boolean configSourceId_3;

    private final int NUMBER_OF_REQUIRED_CONFIG_PARAMETERS = 6;
    private int miCurrentNumberOfConfigurationParameters;

    /**
     * Create an instance of this class.
     * Default constructor is declared, but private, to stop accidental
     * creation of an instance of the class.
     */
    public TwoVetoTrigger()
    {
        super();
    }

    public List getConfiguredTriggerIDs()
    {
        List listConfiguredTriggerIDs = new ArrayList();
        listConfiguredTriggerIDs.add(new Integer(miConfiguredTrigId_1));
        listConfiguredTriggerIDs.add(new Integer(miConfiguredTrigId_2));

        return listConfiguredTriggerIDs;
    }
    /**
     * This method is to provide triggerId which will be used for coincidence triggering.
     *
     * @param tPayload
     * @return
     */
    public int getTriggerId(ITriggerRequestPayload tPayload)
    {
        int iTriggerId = miUnconfiguredTrigId; //Unconfigured triggerId

        int iTrigType = ((ITriggerRequestPayload) tPayload).getTriggerType();
        int iTrigConfigId = ((ITriggerRequestPayload) tPayload).getTriggerConfigID();

        int iSourceId = ((ITriggerRequestPayload) tPayload).getSourceID().getSourceID();

        if(iTrigType == miConfiguredTriggerType_1
             && iTrigConfigId == miConfiguredTriggerConfigId_1
                && iSourceId == miConfiguredSourceId_1)
        {
            iTriggerId = miConfiguredTrigId_1;

        }else if(iTrigType == miConfiguredTriggerType_2
                 && iTrigConfigId == miConfiguredTriggerConfigId_2
                     && iSourceId == miConfiguredSourceId_2)
        {
            iTriggerId = miConfiguredTrigId_2;
        }else
        {
            iTriggerId = miUnconfiguredTrigId;
        }

        return iTriggerId;
    }

    public boolean isConfiguredTrigger(ITriggerRequestPayload tPayload)
    {
        int iTrigId = getTriggerId(tPayload);
        if(iTrigId != miUnconfiguredTrigId)
        {
            return true;
        }
         return false;

    }

    public boolean isConfigured()
    {
        boolean bIsConfigured = mbConfigTriggerType_1 && mbConfigTriggerType_2
                                && mbConfigTriggerConfigId_1 && mbConfigTriggerConfigId_2
                                && mbConfigSourceId_1 && mbConfigSourceId_2;
        return bIsConfigured;
    }

    /**
     * This method performs configuration.
     *
     * @param parameter TriggerParameter object
     * @throws icecube.daq.trigger.exceptions.UnknownParameterException
     */
    public void addParameter(TriggerParameter parameter) throws UnknownParameterException, IllegalParameterValueException
    {
        miCurrentNumberOfConfigurationParameters++;

        String paramName = parameter.getName();
        String paramValue = parameter.getValue();

        if (paramName.compareTo("triggerType1") == 0) {
            miConfiguredTriggerType_1 = Integer.parseInt(paramValue);
            //mlistConfiguredTriggerType.add(new Integer(Integer.parseInt(paramValue)));
            mbConfigTriggerType_1 = true;
        } else if (paramName.compareTo("triggerConfigId1") == 0) {
            miConfiguredTriggerConfigId_1 = Integer.parseInt(paramValue);
            //mlistConfiguredTriggerConfigId.add(new Integer(Integer.parseInt(paramValue)));
            mbConfigTriggerConfigId_1 = true;
        } else if (paramName.compareTo("sourceId1") == 0) {
            miConfiguredSourceId_1 = Integer.parseInt(paramValue);
            //mlistConfiguredSourceId.add(new Integer(Integer.parseInt(paramValue)));
            mbConfigSourceId_1 = true;
        } else if (paramName.compareTo("triggerType2") == 0) {
            miConfiguredTriggerType_2 = Integer.parseInt(paramValue);
            //mlistConfiguredTriggerType.add(new Integer(Integer.parseInt(paramValue)));
            mbConfigTriggerType_2 = true;
        } else if (paramName.compareTo("triggerConfigId2") == 0) {
            miConfiguredTriggerConfigId_2 = Integer.parseInt(paramValue);
            //mlistConfiguredTriggerConfigId.add(new Integer(Integer.parseInt(paramValue)));
            mbConfigTriggerConfigId_2 = true;
        } else if (paramName.compareTo("sourceId2") == 0) {
            miConfiguredSourceId_2 = Integer.parseInt(paramValue);
            //mlistConfiguredSourceId.add(new Integer(Integer.parseInt(paramValue)));
            mbConfigSourceId_2 = true;
        } else {
            throw new UnknownParameterException("Unknown parameter: " + paramName);
        }

        super.addParameter(parameter);

        if(miCurrentNumberOfConfigurationParameters == NUMBER_OF_REQUIRED_CONFIG_PARAMETERS){
            if(!isConfigured())
            {
                log.error("TwoVetoTrigger was NOT properly configured!");
            }else{
                log.info("TwoVetoTrigger was properly configured!");
            }
        }

    }

}
