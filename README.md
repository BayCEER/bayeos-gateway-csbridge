# Campbell Scientific Logger Bridge for BayEOS Gateway

This software allows you to connect Campbell Scientific loggers to a [BayEOS Gateway](https://github.com/BayCEER/bayeos-gateway).

## Main characteristics:

- Runs as a systemd daemon on Debian systems
- Polls logger tables for new observation data using the [python-campbell-logger](https://github.com/BayCEER/python-campbell-logger) module
- Saves and sends data asynchronously to a BayEOS Gateway by using the [bayeos-gateway transport client](https://github.com/BayCEER/bayeosgatewayclient)

## Version History

| Date       | Version | Notes                                                                                     |
| ---------- | ------- | ----------------------------------------------------------------------------------------- |
| 2018-05-06 | 1.1     | Initial import with most-recent mode                                                      |
| 2022-07-06 | 1.2     | Debian package built with make, python3-campbell-logger dependency, get data since record |
| 2022-10-27 | 1.2.1   | Fix: Import of none float types                                                           |
| 2025-01-30 | 1.3.0   | Changed initial import to use dataMostRecent instead of dataSinceTime                     |
