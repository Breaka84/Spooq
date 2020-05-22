#!/bin/bash

diff -U 2    -t --label="original $filepath" --label="adapted $filepath" ~/projects/spooq2/docs/source/loader/overview.rst overview.rst.code > overview.diff
