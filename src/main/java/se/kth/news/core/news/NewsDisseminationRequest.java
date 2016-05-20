/*
 * Welcome to NetBeans...!!!
 */
package se.kth.news.core.news;

import se.kth.news.play.NewsItem;
import se.sics.kompics.KompicsEvent;

/**
 *
 * @author admin
 */
public class NewsDisseminationRequest implements KompicsEvent {
    
    public final NewsItem m_newsToDisseminate;
    
    public NewsDisseminationRequest(NewsItem news) {
        
        this.m_newsToDisseminate = news;
    }
}
