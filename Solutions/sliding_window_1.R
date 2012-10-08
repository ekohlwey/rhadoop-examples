library(rmr2)
library(tm)
library(topicmodels)

rmr.options(backend="local")

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
    tryCatch({
      window_corpus=Corpus(VectorSource(tweets))
      window_matrix=DocumentTermMatrix(window_corpus)
      topic_model=LDA(window_matrix,3) #adjust the number of topics here
      keyval(time, c(topic_model))
    }, error= function(e){return()})
}

tweet_timeframes = from.dfs(mapreduce("~/Data/sample_twitter_data",
                                      input.format="text",
                                      map=tweet_mapper,
                                      reduce=topics_reducer
                                     ))
str(tweet_timeframes)