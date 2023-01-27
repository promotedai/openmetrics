#!/usr/bin/env python3
#
# Copies jobmanager logs to local tmp disk.
#
# Warning: when trying to copy the whole file, you will probably hit "file changed as we read it".


from cp_logs import copy


if __name__ == "__main__":
    copy("flink-jobmanager")
