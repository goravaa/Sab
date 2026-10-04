# Sab Intended User Experience

## Goal

Sab should let users connect their databases and S3 buckets, index their data, and interact with it through a simple query interface.

## Expected Experience

1. The user opens the Data Sources section.
2. The user adds a database and/or S3 bucket through a form.
3. Sab validates the provided connection details.
4. If the connection fails, Sab clearly shows the error.
5. If the connection succeeds, Sab shows basic information about the source:
   - Number of rows or objects.
   - Data size.
   - Available files or tables.
6. The user submits the data source for indexing.
7. Sab creates an indexing job through the Data Integration Service.
8. The user can see the indexing job and its current state.
9. Once indexing is complete, the data becomes available for querying.
10. The user opens the Query Dashboard.
11. For the initial version, the dashboard provides a chat box for asking questions about the indexed data.
12. New files or data added to connected sources should be detected and indexed automatically.
13. The user should not need to manually rebuild the entire index when source data changes.

## Design Principle

Sab should make connecting and querying data simple.

The user should only need to:

- connect their data,
- submit it for indexing,
- and ask questions about it.

Sab should handle connection checks, indexing, updates, and query execution in the background.
