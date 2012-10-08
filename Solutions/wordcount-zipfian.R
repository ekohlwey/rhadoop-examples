library(rmr2)
library(hash)

# sample the data using the "look at the first few
# lines" method
sample_input = file("~/Data/federalist_papers")
word_sample = readChar(sample_input,10000)
close(sample_input)
words = unlist(strsplit(word_sample,"\\s+",perl=T))
word_count_samples = list()
for(word in words){
  if(is.null(word_count_samples[[word]])){
    word_count_samples[[word]] = 1
  } else {
    word_count_samples[[word]] = word_count_samples[[word]]+1
  }
}
hist_order=order(unlist(word_count_samples), decreasing=T)
word_histogram = data.frame(freq=unlist(word_count_samples)[hist_order],
                            word=names(word_count_samples)[hist_order])
# this barplot will let you see quite clearly that a good
# estimate of the "tail" threshold is about 5.
barplot(word_histogram$freq, names.arg=word_histogram$word)
# now that you know the threshold, set up a hash to test for it.
high_freq_words = word_histogram$word[word_histogram$freq>5]
is_high_frequency = hash(keys=high_freq_words, values=rep(T,length(high_freq_words)))


# now we want to break the task up by the degree of parallelism in our cluster
num_slots = 10

rmr.options(backend="local")

partitioned_wordcount_map = function(nulls,lines){ 
  words = unlist(lapply(lines, strsplit, split="\\s+", perl=T))
  words = words[nzchar(words)]
  high_freq_part=floor(runif(1)*num_slots)
  partiton_assigner = function(word) {
    if(!is.null(is_high_frequency[[word]])) 
      c(high_freq_part,word=word)
    else
      c(part=0, word=word)
  }
  words = sapply(words, partiton_assigner)
  keyval(words, rep(1, length(words)))
}
partitioned_wordcount_combine = function(word_and_parts, counts){
  keyval(word_and_parts, sum(unlist(counts)))
}
partitioned_wordcount_reduce = function(word_and_parts, counts){
  keyval(word_and_parts["word"], sum(unlist(counts)))
}
wordcount_reduce = function(words, counts){
  keyval(words, sum(unlist(counts)))
}

phase_1_counts = mapreduce("~/foo-file",
      input.format="text",
      map=partitioned_wordcount_map, 
#       reduce = partitioned_wordcount_reduce,
#       combine = partitioned_wordcount_combine
  )
counts = from.dfs(phase_1_counts)
# counts = from.dfs(mapreduce(phase_1_counts,
#                 reduce=wordcount_reduce))

orders = order(counts$val, decreasing=T)[1:50]
barplot(counts$val[orders], names.arg=counts$key[orders] )

