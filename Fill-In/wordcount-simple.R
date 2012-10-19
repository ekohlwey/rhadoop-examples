library(rmr)
rmr.options.set(backend="local")

wordcount_mapper = function(null,line) {
  # use the strsplit function to break the words on the
  # newline character
  # you will need to unlist the results of the strsplit
  # function
  # use lapply to turn the split words into key value
  # pairs, using the word as the key and 1 as the value
}

wordcount_reducer = function(word,occurrences) {
  # use the sum, unlist, and keyval functions
  # to return a keyval with the word as the key and
  # the sum of the list # of occurrences as the value
}

results = mapreduce("~/Data/federalist_papers", input.format="text",
                    map=wordcount_mapper, reduce=wordcount_reducer, 
                    combine=T)

frequencies = from.dfs(results)

counts = unlist(lapply(frequencies, function(kv) kv$val))
words = unlist(lapply(frequencies, function(kv) kv$key))
orders = order(counts,decreasing=T)[1:10]

barplot(counts[orders], names.arg=words[orders] )







library(rmr)
rmr.options.set(backend="local")
result = from.dfs(mapreduce("~/Data/federalist_papers",
      input.format="text",
      map=function(null,line){ 
        words = unlist(strsplit(line, split="\\s+", perl=T))
        lapply(words, function(word) keyval(word,1))
      }, 
      reduce = function(word, counts){
        keyval(word, sum(unlist(counts)))
      }
  )
)

counts = unlist(lapply(result, function(kv) kv$val))
words = unlist(lapply(result, function(kv) kv$key))
orders = order(counts,decreasing=T)[1:10]

barplot(counts[orders], names.arg=words[orders] )

