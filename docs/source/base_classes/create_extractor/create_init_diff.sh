#!/bin/bash

original_file = "spooq/extractor/__init__.py"
adapted_file = ""

diff -U 3 -t --label="original " --label="adapted $filepath" ~/projects/spooq/spooq/extractor/__init__.py init.py > init.diff
