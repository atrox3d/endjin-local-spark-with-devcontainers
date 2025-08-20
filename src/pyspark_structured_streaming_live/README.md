# PySpark structured streaming live


Tasks:

1. implement a simple streaming use-case:
    - given a stream of purchase transactions from a retail store
    - filter the purchases by their total order volume
    - filter uses upper and lower bound

2. test implementation with a unit test

3. do this in a test-driven manner: start with the test


## Session 2: aggregation query and batched unit tests

aggregation use-case:

- find total order volume per product from the stream
- find the latest purdhase timestamp from the stream
- return the aggregated metrics as stream


content:

- output modes: complete, append and update

todos:

- implement an aggregation unit test (complete mode)
- implement the fuunctionality
- run test in update mode
- implement a unit test using two batches to test correctness of the aggregations (in complete mode)
- run test in update mode; implement window function to find last row
- run test in update mode; implement window function to find last row
    - we will have to use a window function to select the correct row; an aggregation could yield incorrect results
    - use a callable parameter which we pass in of one use-case, not the other

 