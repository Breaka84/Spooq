#!/bin/bash

diff -U 10 -t --label="original $filepath" --label="adapted $filepath" ~/projects/spooq/spooq/transformer/__init__.py init.py > init.diff
