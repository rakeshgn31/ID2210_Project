/*
 * 2016 Royal Institute of Technology (KTH)
 *
 * LSelector is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.kth.news.core.news;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.leader.LeadeUpdatePort;
import se.kth.news.core.leader.LeaderUpdate;
import se.kth.news.core.news.util.NewsView;
import se.kth.news.play.NewsItem;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierSample;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.event.TGradientSample;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.other.Container;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdate;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NewsComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(NewsComp.class);
    private String logPrefix = " ";

    //*******************************CONNECTIONS********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<CroupierPort> croupierPort = requires(CroupierPort.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Positive<LeadeUpdatePort> leaderUpdatePort = requires(LeadeUpdatePort.class);
    Negative<OverlayViewUpdatePort> viewUpdatePort = provides(OverlayViewUpdatePort.class);
    
    // Update the Leader Selection component the failure of the leader to respond
    Negative<LeaderFailureUpdatePort> leaderFailureUpdatePort = provides(LeaderFailureUpdatePort.class);
    
    //*******************************EXTERNAL_STATE*****************************
    private KAddress m_selfAdr;
    private final Identifier m_gradientOId;
    private Comparator viewComparator;
    
    //*******************************INTERNAL_STATE*****************************
    private UUID m_nNewsFloodTimeoutID;
    private int m_nNewsFloodSeqCounter;
    private CroupierSample<NewsView> m_croupNeighborView;
    private ArrayList<String> m_arrReceivedNews;
    private NewsView m_localNewsView;

    // Task - 3.2 and above related
    // Leader is elected for the first time stop the news flooding
    private KAddress m_addrLeader;
    private UUID m_nDissTimeoutID, m_nNewsPullTimeoutID;
    private int m_nPeriodicNewsDissCtr;
    private String m_strPreviousDisseminatedNews;
    private TGradientSample<NewsView> m_gradSample;
    private boolean m_bStopFloodingNews, m_bLeaderAlive, m_bPreviousNewsDisseminated;
    
    // *************************************************************************
    //                      Actual code starts here
    // *************************************************************************
    public NewsComp(Init init) {
        
        m_selfAdr = init.selfAdr;
        logPrefix = "<nid:" + m_selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);

        m_gradientOId = init.gradientOId;
        viewComparator = init.viewComparator;
        
        m_croupNeighborView = null;
        m_nNewsFloodSeqCounter = 0;
        m_arrReceivedNews = new ArrayList<>();

        m_addrLeader = null;
        m_gradSample = null;
        m_nPeriodicNewsDissCtr = 0;
        m_bStopFloodingNews = m_bLeaderAlive = false;
        m_bPreviousNewsDisseminated = true; // when first news item for dissemination is 
        m_strPreviousDisseminatedNews = "";     // being generated it should be true 
        
        m_nNewsFloodTimeoutID = null;
        m_nDissTimeoutID = null;
        m_nNewsPullTimeoutID = null;
        
        subscribe(handleStart, control);
        subscribe(handleCroupierSample, croupierPort);
        subscribe(handleGradientSample, gradientPort);
        
        // Task - 1 related events
        subscribe(handleCroupierStabTimeout, timerPort);
        subscribe(handleNewsItem, networkPort);
        subscribe(handleNewsFloodTimeout, timerPort);        
        
        // Task 3.2
        subscribe(handleLeader, leaderUpdatePort);
        subscribe(handlePeriodicNewsDissemTimeout, timerPort);
        subscribe(handleNewsDisseminationRequest, networkPort);
        subscribe(handleNewsDisseminationResponse, networkPort);

        // Task 4.2
        subscribe(handleNewsSynchronizationTimeout, timerPort);
        subscribe(handleNewsPullRequest, networkPort);
        subscribe(handleNewsPullResponse, networkPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            updateLocalNewsView(0);
            
            // Schedule a timeout so that the topology shall stabilize initially
            ScheduleTimeout spt = new ScheduleTimeout(110000);
            CroupierStabilizationTimeout croupStabTO = new CroupierStabilizationTimeout(spt);
            spt.setTimeoutEvent(croupStabTO);
            trigger(spt, timerPort);            
        }
    };

    @Override
    public void tearDown() {
        if(m_nNewsFloodTimeoutID != null && m_nDissTimeoutID != null && m_nNewsPullTimeoutID != null) {
            
            trigger(new CancelPeriodicTimeout(m_nNewsFloodTimeoutID), timerPort);
            trigger(new CancelPeriodicTimeout(m_nDissTimeoutID), timerPort);
            trigger(new CancelPeriodicTimeout(m_nNewsPullTimeoutID), timerPort);
        }
    }
    
    // ***************************************************************************************
    //                              TASK 1 - NEWS FLOODING
    // ***************************************************************************************
    private void updateLocalNewsView(int nCount) {
        
        m_localNewsView = new NewsView(m_selfAdr.getId(), nCount);
        // LOG.debug("{}informing overlays of new view", logPrefix);
        trigger(new OverlayViewUpdate.Indication<>(m_gradientOId, false, m_localNewsView.copy()), viewUpdatePort);
    }

    Handler handleCroupierSample = new Handler<CroupierSample<NewsView>>() {
        @Override
        public void handle(CroupierSample<NewsView> castSample) {
            if (castSample.publicSample.isEmpty()) {
                return;
            }
            
            m_croupNeighborView = castSample;
        }
    };

    Handler handleCroupierStabTimeout = new Handler<CroupierStabilizationTimeout>() {
        @Override
        public void handle(CroupierStabilizationTimeout e) {
            
            // Schedule a timeout for the news flood
            SchedulePeriodicTimeout sptNewsFlood = new SchedulePeriodicTimeout(0, 40000);
            NewsFloodTimeout floodTO = new NewsFloodTimeout(sptNewsFlood);
            sptNewsFlood.setTimeoutEvent(floodTO);
            trigger(sptNewsFlood, timerPort);
            m_nNewsFloodTimeoutID = floodTO.getTimeoutId();
            
            // Schedule a timeout for the periodic news dissemination through the leader
            SchedulePeriodicTimeout sptPerdNewsDissemination = new SchedulePeriodicTimeout(0, 60000);
            PeriodicNewsDisseminationTimeout pndTO = new PeriodicNewsDisseminationTimeout(sptPerdNewsDissemination);
            sptPerdNewsDissemination.setTimeoutEvent(pndTO);
            trigger(sptPerdNewsDissemination, timerPort);
            m_nDissTimeoutID = pndTO.getTimeoutId();
            
            // Schedule a timeout for periodic news pull and synchronization
            SchedulePeriodicTimeout sptPerdNewsPull = new SchedulePeriodicTimeout(0, 80000);
            PeriodicNewsSynchronizationTimeout pnsTO = new PeriodicNewsSynchronizationTimeout(sptPerdNewsPull);
            sptPerdNewsPull.setTimeoutEvent(pnsTO);
            trigger(sptPerdNewsPull, timerPort);
            m_nNewsPullTimeoutID = pnsTO.getTimeoutId();
        }        
    };
    
    Handler handleNewsFloodTimeout = new Handler<NewsFloodTimeout>() {
        @Override
        public void handle(NewsFloodTimeout event) {
         
            if(m_addrLeader == null && !m_bStopFloodingNews && !m_bLeaderAlive) {
                
                // Get node ID from the assigned IP address ( x.x.x.2 to x.x.x.2+NUM_OF_NODES)
                int nNodeID = Integer.parseInt( m_selfAdr.getIp().toString().split("\\.")[3] );
                if(nNodeID % 30 == 0) {
                    generateNews();                
                }
            }
        } 
    }; 
    
    ClassMatchedHandler handleNewsItem = new 
            ClassMatchedHandler<NewsItem, KContentMsg<?, ?, NewsItem>>() {
        @Override
        public void handle(NewsItem newsItem, KContentMsg<?, ?, NewsItem> container) {
            
            String strNews = newsItem.getNewsString();
            int nTTL = newsItem.getTTLValue();
            // Check if the news is not already received
            if( !m_arrReceivedNews.contains(strNews) ) {
                m_arrReceivedNews.add(strNews);
                LOG.info(m_selfAdr.getIp().toString() + " received news item : " + strNews);
                updateLocalNewsView(m_arrReceivedNews.size());    // Update local news view to the topology
                nTTL--;
                if(nTTL > 0) {
                    NewsItem news = new NewsItem(nTTL, strNews);
                    floodNewsToNeighbors(container.getHeader().getSource(), news);
                }
            }
        }    
    };
 
    private void generateNews() {

        CroupierSample<NewsView> tempView = m_croupNeighborView;
        if(tempView != null) {
            if( !tempView.publicSample.isEmpty() ) {
                
                // Generate the new News Item
                m_nNewsFloodSeqCounter++;
                String strNews = m_selfAdr.getIp().toString() + "_" + m_nNewsFloodSeqCounter 
                                                            + "_" + "Hai...I have the file";
                NewsItem news = new NewsItem(2, strNews);
                m_arrReceivedNews.add(strNews);                   // Add news to its own received set                
                
                // Distribute to its neighbors
                Iterator<Identifier> iter = tempView.publicSample.keySet().iterator();
                while(iter.hasNext()) {
                    KAddress neighbor = tempView.publicSample.get(iter.next()).getSource();
                    KHeader header = new BasicHeader(m_selfAdr, neighbor, Transport.UDP);
                    KContentMsg msg = new BasicContentMsg(header, news);
                    trigger(msg, networkPort);
                }

                updateLocalNewsView(m_arrReceivedNews.size());    // Update the overlay of the new news view                
            }
        }        
    }
    
    private void floodNewsToNeighbors(KAddress srcAddress, NewsItem newsItem) {

        CroupierSample<NewsView> tempView = m_croupNeighborView;
        if(tempView != null) {
            if( !tempView.publicSample.isEmpty() ) {
                
                // Distribute to its neighbors
                Iterator<Identifier> iter = tempView.publicSample.keySet().iterator();
                while(iter.hasNext()) {
                    KAddress neighbor = tempView.publicSample.get(iter.next()).getSource();
                    if( !neighbor.getId().equals(srcAddress.getId()) ) {
                        KHeader header = new BasicHeader(m_selfAdr, neighbor, Transport.UDP);
                        KContentMsg msg = new BasicContentMsg(header, newsItem);
                        trigger(msg, networkPort);
                    }
                }
            }
        }          
    }

    public static class CroupierStabilizationTimeout extends Timeout {
        
        public CroupierStabilizationTimeout(ScheduleTimeout spt) {
            super(spt);
        }
    }
        
    public static class NewsFloodTimeout extends Timeout {

        public NewsFloodTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }    
  
    // ***************************************************************************************
    //                      TASK 3.2/4.1/4.2 - NEWS DISSEMINATION VIA LEADER
    // ***************************************************************************************
    Handler handleGradientSample = new Handler<TGradientSample>() {
        @Override
        public void handle(TGradientSample sample) {
            
            m_gradSample = sample;
        }
    };

    Handler handleLeader = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate event) {
            
            if(event.leaderAdr != null) {
                
                m_addrLeader = event.leaderAdr;
                m_bStopFloodingNews = true;
                m_bLeaderAlive = true;
            }
        }
    };

    Handler handlePeriodicNewsDissemTimeout = new Handler<PeriodicNewsDisseminationTimeout>() {
        @Override
        public void handle(PeriodicNewsDisseminationTimeout event) {

            // Get node ID from the assigned IP address ( x.x.x.2 to x.x.x.2+NUM_OF_NODES)
            int nNodeID = Integer.parseInt( m_selfAdr.getIp().toString().split("\\.")[3] );
            if(nNodeID % 28 == 0) { // Just to limit the number of disseminations

                // Check if the news previously generated and sent to the leader 
                // was disseminated i.e., if the leader responded to the request (If Alive)
                if(m_bPreviousNewsDisseminated) {                    
                    if(m_addrLeader != null && m_bStopFloodingNews && m_bLeaderAlive) {
                    
                        // Generate the new News Item to disseminate via leader
                        m_bPreviousNewsDisseminated = false;
                        m_nPeriodicNewsDissCtr++;                   
                        String strNews = m_selfAdr.getIp().toString() + "_" + m_nPeriodicNewsDissCtr 
                                                                    + "_" + "Hai...News via leader";
                        NewsItem news = new NewsItem(0, strNews);
                        m_strPreviousDisseminatedNews = news.getNewsString();

                        // Send the news to the leader
                        KHeader header = new BasicHeader(m_selfAdr, m_addrLeader, Transport.UDP);
                        KContentMsg msg = new BasicContentMsg(header, new NewsDisseminationRequest(news));
                        LOG.info(m_selfAdr.getIp().toString() + "Sending news dissemination "
                                + "request to the leader " + m_addrLeader.getIp().toString());
                        trigger(msg, networkPort);
                    }
                } else {
                    // Leader did not respond with an ACK/NACK to the previous request
                    // It could be the case that the leader has crashed, lets inform the leader 
                    // election component so that a new leader election is triggered and
                    // once the new leader is elected, it will update me.
                    m_addrLeader = null;
                    m_bLeaderAlive = false;
                    trigger(new LeaderFailureUpdate(), leaderFailureUpdatePort);
                }
            }
        }
    };
    
    // As a leader, handle the news dissemination request
    ClassMatchedHandler handleNewsDisseminationRequest 
        = new ClassMatchedHandler<NewsDisseminationRequest, KContentMsg<?, ?, NewsDisseminationRequest>>() {
        @Override
        public void handle(NewsDisseminationRequest req, KContentMsg<?, ?, NewsDisseminationRequest> msg) {
            
            // Distribute the news to the neighbors
            NewsItem news = req.m_newsToDisseminate;
            if( !m_arrReceivedNews.contains(news.getNewsString()) ) {
                // Add it to its own view (Leader here)
                m_arrReceivedNews.add(news.getNewsString());
                updateLocalNewsView(m_arrReceivedNews.size());
                
                // Distribute only to its immediate neighbors so that the other nodes 
                // will eventually get the news item if in case the leader dies
                TGradientSample<NewsView> tmpGradSample = m_gradSample;
                Iterator<Container<KAddress, NewsView>> iter = tmpGradSample.gradientNeighbours.iterator();
                while(iter.hasNext()) {
                    KAddress neighbor = iter.next().getSource();
                    KHeader header = new BasicHeader(m_selfAdr, neighbor, Transport.UDP);
                    KContentMsg msgContent = new BasicContentMsg(header, news);
                    trigger(msgContent, networkPort);
                }
                
                KHeader respHeader = new BasicHeader(m_selfAdr, msg.getHeader().getSource(), Transport.UDP);
                KContentMsg respMsg = new BasicContentMsg(respHeader, new NewsDisseminationResponse(true, "SUCCESS"));
                trigger(respMsg, networkPort);
            }
        }                
    };
    
    ClassMatchedHandler handleNewsDisseminationResponse
        = new ClassMatchedHandler<NewsDisseminationResponse, KContentMsg<?, ?, NewsDisseminationResponse>>() {
        @Override
        public void handle(NewsDisseminationResponse v, KContentMsg<?, ?, NewsDisseminationResponse> e) {
            
            m_bPreviousNewsDisseminated = true;
            if( !m_arrReceivedNews.contains(m_strPreviousDisseminatedNews) ) {
                
                m_arrReceivedNews.add(m_strPreviousDisseminatedNews);
                updateLocalNewsView(m_arrReceivedNews.size());
            }
        }            
    };

    Handler handleNewsSynchronizationTimeout = new Handler<PeriodicNewsSynchronizationTimeout>() {
        @Override
        public void handle(PeriodicNewsSynchronizationTimeout e) {
            
            // Nodes shall pull the news periodically from the highest 
            // finger as it will be close to the leader/ center of the network and
            // will possibly have most of the latest news received
            TGradientSample<NewsView> tmpGradSample = m_gradSample;
            Container<KAddress, NewsView> node = null;
            if(tmpGradSample.getGradientFingers().size() > 0) {
                node = tmpGradSample.getGradientFingers().get(0);
                for(int nIndex = 1; nIndex < tmpGradSample.getGradientFingers().size(); nIndex++) {
                    if(viewComparator.compare(node.getContent(), tmpGradSample.getGradientFingers().get(nIndex).getContent()) < 0) {
                        node = tmpGradSample.getGradientFingers().get(nIndex);
                    }          
                }
            }
            
            // Send a news pull request to the highest finger
            if(node != null) {
                
                KHeader header = new BasicHeader(m_selfAdr, node.getSource(), Transport.UDP);
                KContentMsg msg = new BasicContentMsg(header, new NewsPullRequest());
                trigger(msg, networkPort);
            }            
        }        
    };
    
    ClassMatchedHandler handleNewsPullRequest 
        = new ClassMatchedHandler<NewsPullRequest, KContentMsg<?, ?, NewsPullRequest>>() {
        @Override
        public void handle(NewsPullRequest req, KContentMsg<?, ?, NewsPullRequest> contentMsg) {
            
            KHeader header = new BasicHeader(m_selfAdr, contentMsg.getHeader().getSource(), Transport.UDP);
            KContentMsg msg = new BasicContentMsg(header, new NewsPullResponse(m_arrReceivedNews));
            LOG.info(m_selfAdr.getIp().toString() + " received news pull request from " + 
                                        contentMsg.getHeader().getSource().getIp().toString());
            trigger(msg, networkPort);
        }
    };
    
    ClassMatchedHandler handleNewsPullResponse
        = new ClassMatchedHandler<NewsPullResponse, KContentMsg<?, ?, NewsPullResponse>>() {
        @Override
        public void handle(NewsPullResponse resp, KContentMsg<?, ?, NewsPullResponse> contentMsg) {
            
            if(resp.arrNewsItems != null) {
                if(resp.arrNewsItems.size() > 0) {
                    
                    // Sync the local news items list with the received news list
                    LOG.info(m_selfAdr.getIp().toString() + " got news pull response from " + 
                                            contentMsg.getHeader().getSource().getIp().toString());
                    m_arrReceivedNews.addAll(resp.arrNewsItems);
                }
            }
        }       
    };
    
    public static class PeriodicNewsDisseminationTimeout extends Timeout {

        public PeriodicNewsDisseminationTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }    
  
    public static class PeriodicNewsSynchronizationTimeout extends Timeout {
    
        public PeriodicNewsSynchronizationTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
    
    public static class Init extends se.sics.kompics.Init<NewsComp> {

        public final KAddress selfAdr;
        public final Identifier gradientOId;
        public final Comparator viewComparator;

        public Init(KAddress selfAdr, Identifier gradientOId, Comparator viewComp) {
            this.selfAdr = selfAdr;
            this.gradientOId = gradientOId;
            this.viewComparator = viewComp;
        }
    }
}
