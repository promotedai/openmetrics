import subprocess
import time


num_retries = 3
# Simple backoff that scales linearly with the retry attempt.
sleep_multiplier_seconds = 3


def subprocess_run(command, retry=False):
    print("\nCommand: subprocess.run('%s')" % (command))
    if retry:
        latest_result = None
        for i in range(num_retries):
            # Since we control the input, running as shell is fine.
            latest_result = subprocess.run(command, capture_output=True, shell=True)
            if latest_result.returncode == 0:
                return latest_result
            time.sleep((i + 1) * sleep_multiplier_seconds)
        return latest_result
    else:
        return subprocess.run(command, capture_output=True, shell=True)
