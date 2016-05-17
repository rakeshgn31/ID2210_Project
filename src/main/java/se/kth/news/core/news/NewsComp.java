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
import java.util.Iterator;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.leader.LeaderSelectPort;
import se.kth.news.core.leader.LeaderUpdate;
import se.kth.news.core.news.util.NewsView;
import se.kth.news.play.NewsItem;
import se.kth.news.play.Ping;
import se.kth.news.play.Pong;
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
    Positive<LeaderSelectPort> leaderPort = requires(LeaderSelectPort.class);
    Negative<OverlayViewUpdatePort> viewUpdatePort = provides(OverlayViewUpdatePort.class);
    //*******************************EXTERNAL_STATE*****************************
    private KAddress selfAdr;
    private Identifier gradientOId;
    //*******************************INTERNAL_STATE*****************************
    private UUID m_nTimeoutID;
    private int nNewsSeqCounter;
    private CroupierSample<NewsView> croupNeighborView;
    private ArrayList<String> arrReceivedNews;
    private NewsView localNewsView;
    
    // Leader is elected for the first time stop the news flooding
    private KAddress m_addrLeader;
    private TGradientSample<NewsView> gradSample;

    public NewsComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);

        gradientOId = init.gradientOId;
        croupNeighborView = null;
        nNewsSeqCounter = 0;
        arrReceivedNews = new ArrayList<>();

        m_addrLeader = null;
        
        subscribe(handleStart, control);
        subscribe(handleCroupierSample, croupierPort);
        subscribe(handleGradientSample, gradientPort);
        
        // Task - 1 related events
        subscribe(handleNewsItem, networkPort);
        subscribe(handleNewsFloodTimeout, timerPort);
        subscribe(handleLeader, leaderPort);
        
        // subscribe(handlePing, networkPort);
        // subscribe(handlePong, networkPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            updateLocalNewsView(0);
            
            // Schedule a timeout for the news flood and initial topology stabilization
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(60000, 5000);
            NewsFloodTimeout floodTO = new NewsFloodTimeout(spt);
            spt.setTimeoutEvent(floodTO);
            trigger(spt, timerPort);
            m_nTimeoutID = floodTO.getTimeoutId();
        }
    };

    @Override
    public void tearDown() {
        if(m_nTimeoutID != null) {
            trigger(new CancelPeriodicTimeout(m_nTimeoutID), timerPort);
        }
    }
    
    private void updateLocalNewsView(int nCount) {
        
        localNewsView = new NewsView(selfAdr.getId(), nCount);
        LOG.debug("{}informing overlays of new view", logPrefix);
        trigger(new OverlayViewUpdate.Indication<>(gradientOId, false, localNewsView.copy()), viewUpdatePort);
    }

    Handler handleCroupierSample = new Handler<CroupierSample<NewsView>>() {
        @Override
        public void handle(CroupierSample<NewsView> castSample) {
            if (castSample.publicSample.isEmpty()) {
                return;
            }
            
            croupNeighborView = castSample;
        }
    };

    Handler handleNewsFloodTimeout = new Handler<NewsFloodTimeout>() {
        @Override
        public void handle(NewsFloodTimeout event) {
         
            if(m_addrLeader == null) {
                // Get node ID from the assigned IP address ( x.x.x.2 to x.x.x.2+NUM_OF_NODES)
                int nNodeID = Integer.parseInt( selfAdr.getIp().toString().split("\\.")[3] );
                if(nNodeID % 15 == 0) {
                    generateNews();                
                }
            }
        } 
    }; 
    
    ClassMatchedHandler handleNewsItem = new 
            ClassMatchedHandler<NewsItem, KContentMsg<?, ?, NewsItem>>() {
        @Override
        public void handle(NewsItem newsItem, KContentMsg<?, ?, NewsItem> container) {
            String strNews = newsItem.getNewsItem();
            int nTTL = newsItem.getTTLValue();
            // Check if the news is not already received
            if( !arrReceivedNews.contains(strNews) ) {
                arrReceivedNews.add(strNews);
                LOG.info(selfAdr.getIp().toString() + " received news item : " + strNews);
                updateLocalNewsView(arrReceivedNews.size());    // Update regarding receiving new news item 
                nTTL--;
                if(nTTL > 0) {
                    NewsItem news = new NewsItem(nTTL, strNews);
                    floodNewsToNeighbors(container.getHeader().getSource(), news);
                }
            }
        }    
    };
 
    private void generateNews() {

        CroupierSample<NewsView> tempView = croupNeighborView;
        if(tempView != null) {
            if( !tempView.publicSample.isEmpty() ) {
                
                // Generate the new News Item
                nNewsSeqCounter++;
                String strNews = selfAdr.getIp().toString() + "_" + nNewsSeqCounter 
                                                            + "_" + "Hai...I have the file";
                NewsItem news = new NewsItem(5, strNews);
                arrReceivedNews.add(strNews);                   // Add news to its own received set                
                
                // Distribute to its neighbors
                Iterator<Identifier> iter = tempView.publicSample.keySet().iterator();
                while(iter.hasNext()) {
                    KAddress neighbor = tempView.publicSample.get(iter.next()).getSource();
                    KHeader header = new BasicHeader(selfAdr, neighbor, Transport.UDP);
                    KContentMsg msg = new BasicContentMsg(header, news);
                    trigger(msg, networkPort);
                }

                updateLocalNewsView(arrReceivedNews.size());    // Update the overlay of the new news view                
            }
        }        
    }
    
    private void floodNewsToNeighbors(KAddress srcAddress, NewsItem newsItem) {

        CroupierSample<NewsView> tempView = croupNeighborView;
        if(tempView != null) {
            if( !tempView.publicSample.isEmpty() ) {
                
                // Distribute to its neighbors
                Iterator<Identifier> iter = tempView.publicSample.keySet().iterator();
                while(iter.hasNext()) {
                    KAddress neighbor = tempView.publicSample.get(iter.next()).getSource();
                    if( !neighbor.getId().equals(srcAddress.getId()) ) {
                        KHeader header = new BasicHeader(selfAdr, neighbor, Transport.UDP);
                        KContentMsg msg = new BasicContentMsg(header, newsItem);
                        trigger(msg, networkPort);
                    }
                }
            }
        }          
    }
        
    Handler handleGradientSample = new Handler<TGradientSample>() {
        @Override
        public void handle(TGradientSample sample) {
            
            // Store it in for later usage
            gradSample = sample;
        }
    };

    Handler handleLeader = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate event) {
        }
    };

    public static class NewsFloodTimeout extends Timeout {

        public NewsFloodTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }    
    
    ClassMatchedHandler handlePing
            = new ClassMatchedHandler<Ping, KContentMsg<?, ?, Ping>>() {

                @Override
                public void handle(Ping content, KContentMsg<?, ?, Ping> container) {
                    LOG.info("{}received ping from:{}", logPrefix, container.getHeader().getSource());
                    trigger(container.answer(new Pong()), networkPort);
                }
            };

    ClassMatchedHandler handlePong
            = new ClassMatchedHandler<Pong, KContentMsg<?, KHeader<?>, Pong>>() {

                @Override
                public void handle(Pong content, KContentMsg<?, KHeader<?>, Pong> container) {
                    LOG.info("{}received pong from:{}", logPrefix, container.getHeader().getSource());
                }
            };
        
    public static class Init extends se.sics.kompics.Init<NewsComp> {

        public final KAddress selfAdr;
        public final Identifier gradientOId;

        public Init(KAddress selfAdr, Identifier gradientOId) {
            this.selfAdr = selfAdr;
            this.gradientOId = gradientOId;
        }
    }
}
