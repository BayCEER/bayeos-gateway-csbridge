# Campbell Scientific Logger Bridge for BayEOS Gateway

This software allows you to connect Campbell Scientific loggers to a BayEOS Gateway.

## Main characteristics:

- Runs as a systemd daemon on Debian systems
- Polls logger tables for new observation data
- Saves data in a local queue
- Sends data asynchronous to a BayEOS Gateway

## Version History

| Date       | Version | Notes                                                                                     |
| ---------- | ------- | ----------------------------------------------------------------------------------------- |
| 2018-05-06 | 1.1     | Initial import with most-recent mode                                                      |
| 2022-07-06 | 1.2     | Debian package built with make, python3-campbell-logger dependency, get data since record |
| 2022-10-27 | 1.2.1   | Fix: Import of none float types                                                           |
