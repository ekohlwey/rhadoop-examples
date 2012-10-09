library(rmr)
library(tm)
library(topicmodels)

rmr.options.set(backend="local")

time_extractor = function(tweet) {
  floor(unclass(as.POSIXct(
    tweet$created_at, format='%a, %d %b %Y %X %z', tz="UTC"
  ))[1]/3600)
}

tweet_mapper = function(null,tweet_text) {
  tweet = fromJSON(tweet_text)
  tweet_time_frame = time_extractor(tweet)
  keyval(tweet_time_frame, tweet$text)
}

topics_reducer = function(time,tweets){
    tryCatch({
      window_corpus=Corpus(VectorSource(tweets))
      window_matrix=DocumentTermMatrix(window_corpus)
      topic_model=LDA(window_matrix,3) #adjust the number of topics here
      keyval(time, topic_model)
    }, error= function(e){return()})
}

tweet_timeframes = from.dfs(mapreduce("~/Data/sample_twitter_data",
                                      input.format="text",
                                      map=tweet_mapper,
                                      reduce=topics_reducer
                                     ))

first_topic_weights = lapply(tweet_timeframes, function(kv) attr(unclass(kv$val),"beta")[1,] )
frame_terms = lapply(tweet_timeframes, function(kv) attr(unclass(kv$val), "terms"))
top_term_orders = sapply(first_topic_weights, function(weights) order(sapply(weights, function(x)10^x), decreasing=T)[1:4])
barplot(unlist(first_topic_weights)[top_term_orders[1:40]], names.arg=unlist(frame_terms)[top_term_orders[1:40]])
