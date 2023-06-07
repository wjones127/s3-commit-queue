# S3 commit queue

Experimental prototype for a protocol to allow only one of many concurrent writers
successfully write a particular object.

This is useful, for example, in table formats where writing a file means
committing a transaction and you only want one concurrent writer to commit
the next version of the table.

Other solutions involve some external service, but this just requires basic 
operations on S3. The only requirement is that the S3 implementation has
strong consistency.
