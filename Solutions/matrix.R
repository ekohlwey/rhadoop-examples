library(rmr)
rmr.options.set(backend="local")

n = 10
m = 5
p = 10
A = matrix(rnorm(n*(m+1)), nrow=n, ncol=m+1)
A[,1] = as.integer(1:n)
B = matrix(rnorm(m*(p+1)), nrow=m, ncol=p+1)
B[,1] = as.integer(1:m) 
write.table(A, file="~/Data/A.csv", sep=",", eol="\r\n", col.names=F, row.names=F)
write.table(B, file="~/Data/B.csv", sep=",", eol="\r\n", col.names=F, row.names=F)

left_mapper = function(null,row){
  in_row = row[1]
  point_generator = function(out_col) {
    lapply(2:length(row), function(in_col) 
      keyval(c(row=unname(as.integer(in_row)), col=unname(as.integer(out_col))), list(i=in_col-1,val=row[in_col])))
  }
  rows_points = lapply(1:p, point_generator)
  do.call(c, args=rows_points)
}

right_mapper = function(null,row){
  in_row = row[1]
  point_generator = function(out_row) {
    lapply(2:length(row), function(in_col) 
      keyval(c(row=unname(as.integer(out_row)), col=unname(as.integer(in_col-1))), list(i=in_row,val=row[in_col])))
  }
  rows_points = lapply(1:n, point_generator)
  do.call(c, args=rows_points)
}

is.even = function(num) num%%2 == 0
even_elements = function(v) {
  v[is.even(1:length(v))]
}
odd_elements = function(v) {
  v[!is.even(1:length(v))]
}

product_reducer = function(out_index, is_and_values){
  is = unlist(lapply(is_and_values, function(ival) ival$i))
  vals = unlist(lapply(is_and_values, function(ival) ival$val))
  sorted_is = order(is)
  product = even_elements(vals) * odd_elements(vals)
  keyval(out_index["row"], list(col=out_index["col"], val=sum(product)))
}

to_row_reducer = function(row_index, cols_and_vals) {
  cols = unlist(lapply(cols_and_vals,function(cv) cv$col))
  vals = unlist(lapply(cols_and_vals,function(cv) cv$val))
  col_order = order(cols)
  keyval(NULL, c(row_index,vals[col_order]))
}

simple_csv_in=make.input.format("csv",sep=",")
left_intermediate = mapreduce("~/Data/A.csv", map = left_mapper, input.format=simple_csv_in)
right_intermediate = mapreduce("~/Data/B.csv", map = right_mapper, input.format=simple_csv_in)
merged_intermediate = mapreduce(list(left_intermediate, right_intermediate), reduce=product_reducer)
final_rows = mapreduce(merged_intermediate, reduce=to_row_reducer)