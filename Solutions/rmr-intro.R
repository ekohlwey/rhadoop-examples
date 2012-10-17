#Import the rmr library
library(rmr)

#Set rmr to use local backend
rmr.options.set(backend="local")

#“Send” data to the distributed filesystem. Data will have null keys. 
input = to.dfs(1:10)

#Pull data back from the distributed filesystem
output = from.dfs(input)
output
## Structure of mapreduce in rmr
# mapreduce(  	indicates the start of a mapreduce           
#     input,		specify the input data stored in DFS
#     input.format,	specify the input format (i.e. text, csv, etc.) 
#     map,		specify the map function, which consists of key-value pairs as input and output  
#     reduce,		specify the reduce function, which consists 			of key-value pairs as input  
#     *other parameters (i.e. combiner, output.format, etc.)             
# )

## Other useful tips:
# input.format=‘text’  will read a text file line by line with key=NULL and value=text line
# "keyval” is the function used to return key-value pairs in the map and reduce


#Basic MapReduce example

#Square the numbers 1 through 10
squares = 
  from.dfs(
    mapreduce(input, 
              map=function(null, num) keyval(NULL,num^2)))

plot(sapply(squares, function(kv) kv$val))

#Assign a key (even or odd) to the squares
squares2 = 
  from.dfs(
    mapreduce(input, 
              map=function(null, num) {
                if(num %% 2 == 0)
                  keyval("even",num^2)
                else
                  keyval("odd",num^2)
              }
              ))
squares2
#use data frames to "clean up" the results
data = data.frame(key=unlist(lapply(squares2, function(x)x$key)), 
                  squares=unlist(lapply(squares2, function(x)x$val)))
data

# using the keys emitted by the previous job, find the sum of the squares for each key
squares2 = 
  from.dfs(
    mapreduce(input, 
              map=function(null, num) {
                if(num %% 2 == 0)
                  keyval("even",num^2)
                else
                  keyval("odd",num^2)
              },
              reduce=function(k,squares) keyval(k,sum(unlist(squares)))
    ))

#Clean up the mapreduce output
data = data.frame(key=unlist(lapply(squares2, function(x)x$key)), 
                  sum=unlist(lapply(squares2, function(x)x$val)))
data

