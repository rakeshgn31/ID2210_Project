/*
 * Welcome to NetBeans...!!!
 */
package se.kth.news.play;

/**
 *
 * @author admin
 */
public class NewsItem {
    
    private int TTL;
    private String strNews;
        
    public NewsItem(int TTL, String news) {
        
        this.TTL = TTL;
        this.strNews = news;
    }
    
    public String getNewsItem() {
        
        return strNews;
    }
    
    public int getTTLValue() {
    
        return TTL;
    }
    
    public NewsItem getCopy() {
        
        return new NewsItem(this.TTL, this.strNews);
    }
}
