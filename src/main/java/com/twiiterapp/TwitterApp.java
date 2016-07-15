package com.twiiterapp;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.Date;

/**
 * Created by laercio on 14/07/16.
 */

public class TwitterApp {

    public static void main(String[] args) throws Exception
    {

        //CONEXAO COM TWITTER
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("lWTZjl4oTqiGkWZzSB2JUQkwv")
                .setOAuthConsumerSecret("Hdwsf8nKnIrT70gp95Hqc1cN6OzxZg3sKNR6cJFuyLOnVGTEUQ")
                .setOAuthAccessToken("26041193-4LA3eQGOIZLbDNamMmAuLYLSqsjmHz31ZUZqmWoFN")
                .setOAuthAccessTokenSecret("9gRJYv0vryzYpWEWJbinsRvPfNH0HHGzN9zFoBd5zo4tG");

        Twitter twitter = new TwitterFactory(cb.build()).getInstance();

        //CONEXAO COM O CASSANDRA
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect("demo");

        for(String hastag : args){

            System.out.println("################# "+ hastag + "#####################");

            Query query = new Query(hastag);
            long lastID = Long.MAX_VALUE;
            ArrayList<Status> tweets = new ArrayList<Status>();
            query.setCount(100);
            try {
                QueryResult result = twitter.search(query);
                tweets.addAll(result.getTweets());
                for (Status t: tweets)
                    if(t.getId() < lastID)
                        lastID = t.getId();

            }

            catch (TwitterException te) {
                System.out.println("Couldn't connect: " + te);
            }

            for (int i = 0; i < tweets.size(); i++) {
                Status t = (Status) tweets.get(i);

                String username = t.getUser().getScreenName();
                String msg = t.getText();
                String lang = t.getLang();
                Integer userFollowers = t.getUser().getFollowersCount();
                Date createdAt = t.getCreatedAt();
                System.out. println(i + " USER: " + username + " followers: "+ userFollowers+ " createdAt: " +createdAt+ " lang: "+ lang + " wrote: " + msg + "\n");

                //GRAVA NO CASSANDRA
                session.execute("INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");
            }
        }

        cluster.close();
    }

}
