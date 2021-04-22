#!/bin/bash

diff -U 4 -t --label="original $filepath" --label="adapted $filepath" ~/projects/spooq/docs/source/extractor/overview.rst overview.rst.code > overview.diff
