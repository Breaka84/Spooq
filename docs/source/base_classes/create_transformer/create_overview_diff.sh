#!/bin/bash

diff -U 7 -t --label="original $filepath" --label="adapted $filepath" ~/projects/spooq2/docs/source/transformer/overview.rst overview.rst.code > overview.diff
