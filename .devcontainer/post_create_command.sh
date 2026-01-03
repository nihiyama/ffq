#!/usr/bin/env bash

set -Eeou pipefail

# add graphviz
sudo apt update
sudo apt install -y graphviz

# install taskfile
npm install -g @go-task/cli

# install codex
npm install -g @openai/codex
