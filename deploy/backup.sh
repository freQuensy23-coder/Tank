#!/bin/sh
set -eu
exec tank backup --force "$@"
