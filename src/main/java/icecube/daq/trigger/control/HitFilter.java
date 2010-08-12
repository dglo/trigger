package icecube.daq.trigger.control;

import icecube.daq.payload.IHitPayload;
import icecube.daq.trigger.config.DomSet;
import icecube.daq.trigger.config.DomSetFactory;
import icecube.daq.util.DOMRegistry;

/**
 * Created by IntelliJ IDEA.
 * User: pat
 * Date: Sep 13, 2006
 * Time: 10:59:36 AM
 *
 *
 * This class implements a hit filter based on domId's. By default all hits pass. If
 * a DomSet object is intalled, then only hits from those doms will pass.
 *
 */
public class HitFilter
{

    /**
     * DomSet to use
     */
    private DomSet domSet = null;

    private int domSetId = -1;

    /**
     * Default constructor
     */
    public HitFilter() {
        this(-1);
    }

    /**
     * Constructor that takes a DomSet
     * @param domSet set of doms to use
     */
    public HitFilter(DomSet domSet) {
        setDomSet(domSet);
    }

    public HitFilter(int domSetId) {
        this.domSetId = domSetId;
	setDomSet(DomSetFactory.getDomSet(domSetId));
    }

    public void setDomRegistry(DOMRegistry dr) {
	DomSetFactory.setDomRegistry(dr);
    }

    /**
     * Install a DomSet to use
     * @param domSet set of doms
     */
    public void setDomSet(DomSet domSet) {
        this.domSet = domSet;
    }

    /**
     * Check if hit is from a dom that should be used. By default all hits are used.
     * If a DomSet has been installed, then only hits from doms in the set are used.
     * @param hit hit to check
     * @return true if hit should be used, false otherwise
     */
    public boolean useHit(IHitPayload hit) {
        // if domSet is not initialized, use the hit
        if (null == domSet) {
            return true;
        }

        // if the dom is in the domSet, use the hit
        if (domSet.inSet(hit.getDOMID())) {
            return true;
        } else {
            return false;
        }

    }

}
