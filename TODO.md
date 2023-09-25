FEATURES
- Figure out how to get data from SE -> calculate (average/sum/weighted-avg) average for all municipalities
  - Calculate weights for regions
- Migrate to Postgres 


FIXES
- Add proper logger + use it for errors and warnings
- Write missing tests
- Fix relative paths so files can be run from wherever
- Proper (1st iteration) validation of datablocks in run_integration.py
- Add basic ratelimiter for incoming requests
- Add Auth service + Bearer token validation
- Try running the app within docker
- Fetch ts for communes in multiple threads (speed things up)  
- Solution for calculating resolution
    - [2012-01-01, 2013-01-01, 2014-01-01]
    - [2012-01-01. 2012-02-01, ...]
- Consider using SQLAlchemy (ORM)
