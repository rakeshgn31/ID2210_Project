/*
 * Welcome to NetBeans...!!!
 */
package se.kth.news.core.leader;

import se.kth.news.core.news.util.NewsView;
import se.sics.kompics.KompicsEvent;

/**
 *
 * @author admin
 */
public class LeaderNomineeRequest implements KompicsEvent {
    
    // This event is triggered to ask If the node can be the leader
    public final NewsView m_nomineeNewsView;

    public LeaderNomineeRequest(NewsView view) {
        this.m_nomineeNewsView = view;
    }        
}
