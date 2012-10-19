library(rmr)
rmr.options.set(backend="local")

n = 10
m = 5
p = 10
#Notice our matrices are pre-formatted with row indices
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
     # For each output column, output the whole row
     # use the output matrix index (row, col) as the
     # key and the input column number and cell value
     # as the value.
      )
  }
  rows_points = lapply(1:p, point_generator)
  do.call(c, args=rows_points)
}

right_mapper = function(null,row){
  in_row = row[1]
  point_generator = function(out_row) {
    lapply(2:length(row), function(in_col) 
      # For each output row, output the whole column
      # use the output matrix index (row, col) as the
      # key and the input row number and cell value
      # as the value.
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
  # use the order function to find the sorted index
  # values of i (the is) variable
  # use the even_elements, odd_elements, and * operator to find the
  # element-wise product
  # output a keyval with the row number as the key, and the
  # column number and value of the column as the value.
}

to_row_reducer = function(row_index, cols_and_vals) {
  # Use unlist and lapply to unpack the columns and values, 
  # then use the order function to find the ordered
  # indices for the final result. Then emit a key, val
  # where the key is NULL and the value is the concatenation
  # of the row index and the sorted column values
}

simple_csv_in=make.input.format("csv",sep=",")
left_intermediate = mapreduce("~/Data/A.csv", map = left_mapper, input.format=simple_csv_in)
right_intermediate = mapreduce("~/Data/B.csv", map = right_mapper, input.format=simple_csv_in)
merged_intermediate = mapreduce(list(left_intermediate, right_intermediate), reduce=product_reducer)
final_rows = mapreduce(merged_intermediate, reduce=to_row_reducer)