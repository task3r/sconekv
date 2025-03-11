#!/bin/bash

(cd server && docker build -t sconekv-node .)
(cd client && docker build -t sconekv-client .)
(cd ycsb && docker build -t sconekv-ycsb .)
(cd tracker && docker build -t tracker .)
