#!/bin/bash

diff -U 2    -t --label="original $filepath" --label="adapted $filepath" ~/projects/spooq/docs/source/loader/overview.rst overview.rst.code > overview.diff
