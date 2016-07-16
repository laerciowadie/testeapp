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
        if(args == null || args.length == 0){
            System.out.println("Informar uma hastag!");
            return;
        }
        //CONEXAO COM TWITTER
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("lWTZjl4oTqiGkWZzSB2JUQkwv")
                .setOAuthConsumerSecret("Hdwsf8nKnIrT70gp95Hqc1cN6OzxZg3sKNR6cJFuyLOnVGTEUQ")
                .setOAuthAccessToken("26041193-4LA3eQGOIZLbDNamMmAuLYLSqsjmHz31ZUZqmWoFN")
                .setOAuthAccessTokenSecret("9gRJYv0vryzYpWEWJbinsRvPfNH0HHGzN9zFoBd5zo4tG");

        Twitter twitter = new TwitterFactory(cb.build()).getInstance();

        //CONEXAO COM O CASSANDRA e criacao em tempo de execucao do keyspace e columFamilies
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect();

        String queryCreate = "CREATE KEYSPACE IF NOT EXISTS twitterapp WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':1};";
        session.execute(queryCreate);
        session.execute("USE twitterapp");

        //CRIANDO TABELA tweet
        String queryCreateTableTweet = "CREATE TABLE IF NOT EXISTS tweet(id bigint PRIMARY KEY,"
                +"username varchar, "
                +"hashtag varchar, "
                + "msg text, "
                + "lang varchar, "
                + "userFollowers varint, "
                + "createdAt timestamp );";

        session.execute(queryCreateTableTweet);


        //CRIANDO TABELA topFiveUsers
        String queryCreateTableTopFiveUsers = "CREATE TABLE IF NOT EXISTS topFiveUsers(uuid bigint PRIMARY KEY,"
                +"username varchar, "
                + "userFollowers varint);";

        session.execute(queryCreateTableTopFiveUsers);

        //CRIANDO TABELA resumeByTag
        String queryCreateTableResumeByTag = "CREATE TABLE IF NOT EXISTS resumeByTag(uuid bigint PRIMARY KEY,"
                +"hashtag varchar, "
                + "count varint);";

        session.execute(queryCreateTableResumeByTag);

        //CRIANDO TABELA resumeByDayHour
        String queryCreateTableResumeByDayHour = "CREATE TABLE IF NOT EXISTS resumeByDayHour(uuid bigint PRIMARY KEY,"
                +"dayhour varchar, "
                + "count varint);";

        session.execute(queryCreateTableResumeByDayHour);


        for(String hashtag : args){

            System.out.println("################# "+ hashtag + "#####################");

            Query query = new Query(hashtag);
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
                te.printStackTrace();
                return;
            }

            for (int i = 0; i < tweets.size(); i++) {
                Status t = (Status) tweets.get(i);


                Long id = t.getId();
                String username = t.getUser().getScreenName();
                String msg = t.getText();
                String lang = t.getLang();
                Integer userFollowers = t.getUser().getFollowersCount();
                Date createdAt = t.getCreatedAt();
                System.out. println(i + " USER: " + username + " followers: "+ userFollowers+ " createdAt: " +createdAt+ " lang: "+ lang + " wrote: " + msg + "\n");

                //GRAVA NO CASSANDRA
                session.execute("INSERT INTO tweet (id, username, hashtag, msg, lang, userFollowers, createdAt) " +
                        "VALUES ("+id+", '"+username+"', '"+hashtag+"', '"+msg.replaceAll("'","")+"', '"+lang+"', "+userFollowers+", '"+createdAt.getTime()+"')");
            }
        }

        cluster.close();
    }

}
