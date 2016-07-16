package com.frameworks.twitter.example;

/**
 * Created by christiantest on 5/20/16.
 */

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;

//import main.java.com.twitter.hbc.example.SampleStreamExample;

public class TimeLineExample {

    private static final String TWITTER_CONSUMER_KEY = "eyWwndjRndYFVnXnmR8bhHHwY";
    private static final String TWITTER_SECRET_KEY = "D51mfr245ApuZleMQBDL4tTH6OVtVKLs6OTR2p0XK0X6MqHllt";
    private static final String TWITTER_ACCESS_TOKEN = "478097758-KF02CU9ASLI8QjJvxEONXWSjYq3kISlNDLnsk7z4";
    private static final String TWITTER_ACCESS_TOKEN_SECRET = "op1FOyILSIXgdDVvGbtYJ8fcSr8rucT4XjBJ6IJpHZmFg";

    public static void run() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(TWITTER_CONSUMER_KEY)
                .setOAuthConsumerSecret(TWITTER_SECRET_KEY)
                .setOAuthAccessToken(TWITTER_ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(TWITTER_ACCESS_TOKEN_SECRET);
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        try {
            Query query = new Query("JustinBeiber");
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                    System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
                }
            } while ((query = result.nextQuery()) != null);
            System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }
    }

    public static void main(String[] args) {
        run();
    }
}
