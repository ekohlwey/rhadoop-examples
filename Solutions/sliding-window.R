library(rmr)
library(tm)
library(topicmodels)

rmr.options.set(backend="local")

stop_file = file("~/Data/stopwords.txt")
stopwords = readLines(stop_file)
close(stop_file)
stopexpr = do.call(paste, args=as.list(c(stopwords, sep="\\b|\\b")))

time_extractor = function(tweet) {
  floor(unclass(as.POSIXct(
    tweet["created_at"], format='%a, %d %b %Y %X %z', tz="UTC"
  ))[1]/3600)
}

tweet_mapper = function(null,tweet_text) {
  tweet = fromJSON(tweet_text)
  tweet_time_frame = time_extractor(tweet)
  tweet_text = tolower(tweet["text"])
  tweet_text = gsub(pattern=",|'",
                    x=tweet_text, perl=T, replacement="")
  tweet_text = gsub(pattern="-|\\?|\\.|;|\\(|\\)", x=tweet_text, perl=T, replacement=" ")
  tweet_text = gsub(pattern="\\s+\\w\\s", x = tweet_text, perl=T, replacement="")
  tweet_text = gsub(pattern=stopexpr, x=tweet_text, perl=T, replacement="", ignore.case=T)
  tweet_text = gsub(pattern="^\\s+|\\s+$", x = tweet_text, perl=T, replacement="")
  tweet_text = gsub(pattern="\\s+", x = tweet_text, perl=T, replacement=" ")
  if(nchar(tweet_text)>20)
    keyval(tweet_time_frame, tweet_text)
  else
    return()
}

error_func= function(e) print(e)

topics_reducer = function(time,tweets){
    window_corpus=Corpus(VectorSource(tweets))
    window_matrix=DocumentTermMatrix(window_corpus, )
    topic_model=LDA(window_matrix,3) #adjust the number of topics here
    ret = keyval(time, topic_model)
    return(ret)
}

tweet_timeframes = from.dfs(mapreduce("~/Data/small_sample_twitter_data",
                                      input.format="text",
                                      map=tweet_mapper,
                                      reduce=topics_reducer
                                     ))

n_results = 5
terms = lapply(tweet_timeframes, function(kv) terms(kv$val, n_results))
topics = lapply(tweet_timeframes, function(kv){
  topics = posterior(kv$val)$topics
  topics = topics[order(topics, decreasing=T)]
  topics[1:n_results]
})

print(terms)