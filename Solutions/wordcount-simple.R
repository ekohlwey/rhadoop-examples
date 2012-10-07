library(rmr2)
rmr.options(backend="local")
counts = from.dfs(mapreduce("~/Data/federalist_papers",
      input.format="text",
      map=function(nulls,lines){ 
        words = unlist(lapply(lines, strsplit, split="\\s+", perl=T))
        keyval(words, rep(1, length(words)))
      }, 
      reduce = function(word, counts){
        keyval(word, sum(unlist(counts)))
      }
  )
)

orders = order(counts$val, decreasing=T)[1:50]
barplot(counts$val[orders], names.arg=counts$key[orders] )

