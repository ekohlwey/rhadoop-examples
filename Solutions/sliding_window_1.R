library(rmr2)
library(tm)
library(topicmodels)

rmr.options(backend="hadoop")

time_extractor = function(tweet) {
  floor(unclass(as.POSIXct(
    tweet$created_at, format='%a, %d %b %Y %X %z', tz="UTC"
  ))[1]/3600)
}

tweet_mapper = function(null,tweet_text) {
  tweets = lapply(tweet_text, fromJSON)
  tweets_time_frame = lapply(tweets, time_extractor)
  tweets_text = lapply(tweets, function(tweet)tweet$text)
  keyval(unlist(tweets_time_frame), unlist(tweets_text))
}

topics_reducer = function(time,tweets){
  print(time)
  window_corpus=Corpus(VectorSource(tweets))
  window_matrix=DocumentTermMatrix(window_corpus)
  topic_model=LDA(window_matrix,2) #adjust the number of topics here
  keyval(time, topic_model)
}

tweet_timeframes = from.dfs(mapreduce("~/Data/small_sample_twitter_data",
                                      input.format="text",
                                      map=tweet_mapper,
                                      #reduce=topics_reducer
                                     ))
