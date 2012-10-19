library(rmr)
library(hash)

# sample the data using the "look at the first few
# lines" method
# use the file() function, readChar function, and strsplit
# function to break the words up.
# then use R's tapply function to find the word counts


hist_order=order(unlist(word_count_samples), decreasing=T)
word_histogram = data.frame(freq=unlist(unname(word_count_samples))[hist_order],
                            word=names(word_count_samples)[hist_order])
# this barplot will let you see quite clearly that a good
# estimate of the "tail" threshold is about 5.
barplot(word_histogram$freq, names.arg=word_histogram$word)
# now that you know the threshold, set up a hash to test for it.
# use the > operator to find the high frequency words
# then save them in a hash, using the constant T as the value


# now we want to break the task up by the degree of parallelism in our cluster
num_slots = 10

rmr.options.set(backend="local")

partitioned_wordcount_map = function(null,line){ 
  words = unlist(strsplit(line, split="\\s+", perl=T))
  words = words[nzchar(words)]
  high_freq_part=floor(runif(1)*num_slots)
  # create a partition assigner function that 
  # checks if it's input is in the high frequency map
  # using the is.null function. If it isn't,
  # assign it to the partition 0. Otherwise, assign it to
  # the partition high_freq_part. Do so by
  # concatenating the word and the partition number
  # using the c function
  partitioned_words = lapply(words, partiton_assigner)
  lapply(partitioned_words, function(word)keyval(word,1))
}
partitioned_wordcount_combine = function(word_and_parts, counts){
  # sum the counts, but don't strip off the partition number
}
partitioned_wordcount_reduce = function(word_and_parts, counts){
  # sum the counts, strip off the partition number
}
wordcount_reduce = function(words, counts){
  # sum the counts again
}

phase_1_counts = mapreduce("~/Data/federalist_papers",
      input.format="text",
      map=partitioned_wordcount_map, 
      reduce = partitioned_wordcount_reduce,
      combine = partitioned_wordcount_combine
  )
result = from.dfs(mapreduce(phase_1_counts,
                reduce=wordcount_reduce))

counts = unlist(lapply(result, function(kv) kv$val))
words = unlist(lapply(result, function(kv) kv$key))
orders = order(counts,decreasing=T)[1:50]

barplot(counts[orders], names.arg=words[orders] )

