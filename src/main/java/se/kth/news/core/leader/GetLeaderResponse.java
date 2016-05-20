/*
 * Welcome to NetBeans...!!!
 */
package se.kth.news.core.leader;

import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.network.KAddress;

/**
 *
 * @author admin
 */
public class GetLeaderResponse implements KompicsEvent {
    
    public final KAddress m_addrLeader;
    
    public GetLeaderResponse(KAddress addrLeader) {
        
        m_addrLeader = addrLeader;
    }
}
