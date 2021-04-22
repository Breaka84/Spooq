#!/bin/bash

diff -U 2 -t --label="original $filepath" --label="adapted $filepath" ~/projects/spooq/spooq/loader/__init__.py init.py > init.diff
