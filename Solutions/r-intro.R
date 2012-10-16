# help is perhaps the most important function in R
# it can also be accessed using the ? operator
?help
help(help)
# Everything in R is a list or a vector
# create a numeric vector of length 1, whose content is 3
3
# create a list of length 1, whose content is 3.
# note the different notation for indexing a list vs vector
# note that the element itself is a numeric vector
list(3)
# you can create a range of numbers using the colon
# operator
1:10
# Assignment is similar to other languages
# notice that ordered structures are 1-indexed
# c() produces vectors by concatenating vectors
a = c(1,2,3, 7:100)
a
a[5]
a = list(45,65,72)
a[[2]]
# functions may be bound to variables
b = function(c,d) return (c+d)
b(43,8)
# longer functions can be declared using curly braces
e = function (f,g) {
  h = f+g ; i = h*g # semicolons can be used to separate statements
  return(i)
}
e(7,4)
# functions will also return the last call
# if there's no explicit return
# they can also refer to variables that were
# previously declared in their enclosing scope
a = 6
j = function (k) e(k, a)
j (4)
# users are discouraged from using loops - use lapply instead
lapply(1:10, function (x) x^2)
# you can use a integer vector to index a vector
rnorm(20)[4:7]
# you can use a boolean operator on a numeric vector
# to create a vector of booleans
runif(20)>.5
# you can use a boolean vector to index a vector
l = rnorm(20)
l>.75
l[l>.75]
# all the random generator functions start with r and their first argument
# is the number of random elements to generate
runif(4)
rexp(3)
rnorm(4)
# rstudio has control-space completion
rgamma(5 , shape=4)
# rstudio also has tab-completion for paths
my_file = file("~/Data/federalist_papers")
readLines(my_file,3)
readChar(my_file,100)
close(my_file)
# you can create a map using lists
my_map = list()
my_map[["foo"]] = 1
my_map[["bar"]] = 2
my_map$foo
my_map
# you can check for unset items using is.null
if(is.null(my_map[["baz"]]))
  my_map[["baz"]] = my_map[["foo"]]+my_map[["bar"]]
my_map
# thats it!