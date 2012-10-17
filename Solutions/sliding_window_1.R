library(rmr)
library(tm)
library(topicmodels)

rmr.options.set(backend="local")

time_extractor = function(tweet) {
  floor(unclass(as.POSIXct(
    tweet["created_at"], format='%a, %d %b %Y %X %z', tz="UTC"
  ))[1]/3600)
}

tweet_mapper = function(null,tweet_text) {
  tweet = fromJSON(tweet_text)
  tweet_time_frame = time_extractor(tweet)
  tweet_text = tweet["text"]
  if(nchar(tweet_text)>20)
    keyval(tweet_time_frame, tweet_text)
  else
    return()
}

error_func= function(e) print(e)

topics_reducer = function(time,tweets){
    window_corpus=Corpus(VectorSource(tweets))
    window_matrix=DocumentTermMatrix(window_corpus, )
               # control = list(removePunctuation=T, stopwords=T))
    topic_model=LDA(window_matrix,3) #adjust the number of topics here
    ret = keyval(time, topic_model)
    print (ret)
    return(ret)
}

tweet_timeframes = from.dfs(mapreduce("~/Data/sample_twitter_data",
                                      input.format="text",
                                      map=tweet_mapper,
                                      reduce=topics_reducer
                                     ))

terms = lapply(tweet_timeframes, function(kv) terms(kv["val"]))
frequencies = lapply(tweet_timeframes, function(kv) posterior(["val"]))
