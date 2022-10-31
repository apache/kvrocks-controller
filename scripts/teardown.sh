#!/usr/bin/env bash
set -e

cd docker && docker-compose -p kvrocks-controller down && cd ..