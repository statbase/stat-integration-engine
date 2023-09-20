- Store integrations in code (as BaseIntegration), loop through instead 
of explicitly calling Kolada
- Fetch communes in multiple threads (speed things up)  
- Add proper logger + use it for errors and warnings
- Write missing tests for functions
- Fix relative paths so tests can run from wherever
- Figure out how to get data from SE -> calculate (weighted?) average for all municipalities