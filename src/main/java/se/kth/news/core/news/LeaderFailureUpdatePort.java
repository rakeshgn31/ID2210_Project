/*
 * Welcome to NetBeans...!!!
 */
package se.kth.news.core.news;

import se.sics.kompics.PortType;

/**
 *
 * @author admin
 */
public class LeaderFailureUpdatePort extends PortType {
    {
        indication(LeaderFailureUpdate.class);
    }
}
