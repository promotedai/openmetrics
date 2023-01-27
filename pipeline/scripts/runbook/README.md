# Runbook

This directory contains scripts to:
- Get logs from machines.
- Check available Flink disk space.
- Check for errors.

## CloudWatch logs

If you want to investigate the text logs, start by looking at the CloudWatch Flink logs.  Flink TM and JM text logs get saved to CloudWatch.  We might miss some final log messages if the jobs/pods die in an abrupt way. 

Steps:
1. Go to CloudWatch and search for the EKS cluster.  E.g. `eee-core`.

https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/$252Faws$252Feks$252Feee-core

2. Click on `Search all log streams`.

3. Search for `Exception` or `Error`

```
{ $.kubernetes.container_name = "taskmanager" && ($.log = "*Exception*" || $.log = "*Error*")}
```

4. Remove other exceptions and errors.

```
{ $.kubernetes.container_name = "taskmanager" && ($.log = "*Exception*" && $.log != "*TooLongFrameException*")}
```

5. Search for some of the following messages in quotes.

```
no space left on device
```
For this problem, increase Flink disk.

```
Insufficient number of network buffers
```
For this problem, refactor or increase taskmanager.memory.network settings.

