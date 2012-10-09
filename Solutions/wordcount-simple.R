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

