#!/bin/bash

diff -U 10 -t --label="original $filepath" --label="adapted $filepath" ~/projects/spooq2/src/spooq2/transformer/__init__.py init.py > init.diff
