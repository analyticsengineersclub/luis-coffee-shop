### Think about when you might want to use a view vs a table. What are the relevant tradeoffs?

**Use Table and Incremental Materialization:** 
-  Almost all use cases where data is queried frequently
    - Better to use when queried by BI tools because of their speed performance
- Storage space used is larger compared to a view that saves only a query. 

**Use Views Materializations:**
- Very few use cases
    - When data is huge and does not need to be called frequently as performance is terrible
    - Simon Data example where we don't need thousands of tables which only purpose is to sync data from a different system. No need to keep that in storage. 
- Since this is a query that is saved there is a tradeoff between freshness of data and speed of query obtain from a table, depending on lineage. 
- Views are best suited for models that do not do significant transformation, e.g. renaming, recasting columns.
- Citing William Moor: "IMHO views don’t serve a lot of purpose in terms of analytics"
