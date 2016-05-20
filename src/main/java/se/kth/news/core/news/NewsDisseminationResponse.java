/*
 * Welcome to NetBeans...!!!
 */
package se.kth.news.core.news;

import se.sics.kompics.KompicsEvent;

/**
 *
 * @author admin
 */
public class NewsDisseminationResponse implements KompicsEvent {
    
    public final boolean m_bNewsDisseminated;
    public final String m_strReasonForNotDisseminating;
    
    public NewsDisseminationResponse(boolean bDisseminated, String strReason) {
        
        m_bNewsDisseminated = bDisseminated;
        m_strReasonForNotDisseminating = strReason;
    }
}
