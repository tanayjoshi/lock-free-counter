A lock-free (multiple stream) counter 

This is an implementation of an atomic Iterator, starting from a value or streaming data from an array. Also contains the more complicated merge Iterator that combines multiple incoming values in correct order and without using locks. Also contains a binary tree version of the merge iterator.


This implementation uses ox.cads concurrency library used in Concurrent Algorithms and Data Structures at University of Oxford.
