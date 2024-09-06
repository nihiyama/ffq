#!/usr/bin/env bash

set -Eeou pipefail

# add graphviz
sudo apt update
sudo apt install -y graphviz

# install taskfile
go install github.com/go-task/task/v3/cmd/task@latest
