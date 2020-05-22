#!/bin/bash

diff -U 2 -t --label="original $filepath" --label="adapted $filepath" ~/projects/spooq2/src/spooq2/loader/__init__.py init.py > init.diff
