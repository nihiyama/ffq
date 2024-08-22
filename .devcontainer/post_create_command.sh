#!/usr/bin/env bash

set -Eeou pipefail

# install taskfile
go install github.com/go-task/task/v3/cmd/task@latest
