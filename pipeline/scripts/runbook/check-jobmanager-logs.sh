#!/usr/bin/env bash
#
# Searches the jobmanager logs for common issues.
#
# Supported args:
# -d <directory> = Copy the tmp path from the previous script.

# For debugging: set -eux
set -eu

dir=""
POSITIONAL=()
while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
    -d|--dir)
      dir="$2"
      shift # past argument
      shift # past value
      ;;
    *)    # unknown option
      POSITIONAL+=("$1") # save it in an array for later
      shift # past argument
      ;;
  esac
done

if [ -z "$dir" ]
then
  echo "Need to specify argument --dir"
  exit 1
fi

errors=$(grep -iR "error\|exception" "$dir")
if [ -z "$errors" ]
then
  echo "No 'error' or 'exception' in the logs.  If these are tailed logs, increase the size of tail"
else
  echo "Found 'error' or 'exception' in the logs."
fi

echo ""
echo "Checking for known errors:"

found_error=0
if [ ! -z $(grep -iR "no space left on device" $dir)]
then
  found_error=1
  echo "- 'no space left on device' error - Found.  Run `check-disk.sh`"
else 
  echo "- 'no space left on device' error - Not found."
fi

if [ ! -z $(grep -iR "java.io.IOException: Insufficient number of network buffers" $dir)]
then
  found_error=1
  echo "- 'java.io.IOException: Insufficient number of network buffers' error - Found.  Too many operators and task slots.  Refactor or increase taskmanager.memory.network settings."
else 
  echo "- 'java.io.IOException: Insufficient number of network buffers' error - Not found."
fi

if [ "$found_error" -eq "0" ]; then
  echo ""
  echo "Did not detect a known error pattern.  Please inspect logs manually and update this script."
fi
