/*
 * Welcome to NetBeans...!!!
 */
package se.kth.news.core.leader;

import se.sics.kompics.KompicsEvent;

/**
 *
 * @author admin
 */
public class LeaderNominationResponse implements KompicsEvent {
    
    // This event is sent as a response to the leader nomination request
    public final boolean bNodeAgreement;

    public LeaderNominationResponse(boolean bResp) {
        this.bNodeAgreement = bResp;
    }        
}
