library(rmr)
rmr.options.set(backend="local")

wordcount_mapper = function(null,line) {
  split_words = unlist(strsplit(line, split="\\s+", perl=TRUE))
  lapply(split_words, function(word) keyval(word,1))
}

wordcount_reducer = function(word,occurrences) {
  keyval(word, sum(unlist(occurrences)))
}

results = mapreduce("~/Data/federalist_papers", input.format="text",
                    map=wordcount_mapper, reduce=wordcount_reducer, 
                    combine=T)

frequencies = from.dfs(results)

counts = unlist(lapply(frequencies, function(kv) kv$val))
words = unlist(lapply(frequencies, function(kv) kv$key))
orders = order(counts,decreasing=T)[1:10]

barplot(counts[orders], names.arg=words[orders] )
