# db_storage_poc_rust

This is purely a PoC of some concepts we believe will apply to a generalized database that we could use as an application database as well as an analytics platform.


# Notes

Ignore this, it should be deleted by the time this is usable by anyone other than the devs.

High level steps:
- Generate data (done!)
- Generate it into a nice structure for backing with files
- - This is probably a map of column name to an array of values
- - Call this a Table obviously
- Write data to files with some chunk size limit, do it naively
- Do a "query" from previously written files, just some counts
- Do a couple more complicated "queries" that join in a couple directions
- If there's time, write out to Parquet for others to validate on their tools

