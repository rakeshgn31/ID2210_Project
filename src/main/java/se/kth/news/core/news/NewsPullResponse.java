/*
 * Welcome to NetBeans...!!!
 */
package se.kth.news.core.news;

import java.util.ArrayList;
import se.sics.kompics.KompicsEvent;

/**
 *
 * @author admin
 */
public class NewsPullResponse implements KompicsEvent {
    
    public final ArrayList<String> arrNewsItems;
    
    public NewsPullResponse(ArrayList<String> arrNews) {
        
        arrNewsItems = arrNews;
    }
}
