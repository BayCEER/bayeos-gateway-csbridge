# Campbell Scientific Logger Bridge for BayEOS Gateway

This software allows you to connect Campbell Scientific loggers to a BayEOS Gateway.

## Main characteristics:

- Runs as a daemon on debian systems
- Polls logger tables for new observation data
- Saves data in a local queue
- Sends data asynchronous to a BayEOS Gateway

## Version History

| Date       | Version | Notes                                         |
| ---------- | ------- | --------------------------------------------- |
| 2018-05-06 | 1.1     | Initial import with most-recent mode          |
| 2022-07-06 | 1.2     | Deb Package, Usage of python3-campbell-logger |
