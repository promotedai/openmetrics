import argparse
import os
import subprocess
import tempfile


def copy(pod_filter):
    parser = argparse.ArgumentParser()
    # TODO - should this stuff be down in main?
    parser.add_argument(
        "-t",
        "--tail",
        help="Number of tail lines to fetch.  Defaults to 0 (which includes all rows)",
        const=0,
        nargs='?',
        type=int,
        default=0)
    parser.add_argument(
        "-n",
        "--namespace",
        help="K8s namespace.  Defaults to default",
        const="default",
        nargs='?',
        type=str,
        default="default")
    parser.add_argument(
        "-d",
        "--debug",
        help="Turn on debugging",
        const=False,
        nargs='?',
        type=bool,
        default=False)
    args = parser.parse_args()
    inner_copy(pod_filter, args.tail, args.namespace, args.debug)


def inner_copy(pod_filter, tail_size, namespace, debug):
    pods = subprocess_run(
        f"kubectl -n {namespace} get pods --no-headers -o custom-columns=':metadata.name'  | grep {pod_filter}",
        debug)
    dirpath = tempfile.mkdtemp()
    if tail_size == 0:
        fetch_type = "logs"
    else:
        fetch_type = "tailed logs"
    print(f"Copying {fetch_type} to {dirpath}")

    for pod in pods.splitlines():
        if debug:
            print("pod=" + pod)
        out_pod_dir = os.path.join(dirpath, pod)
        os.mkdir(out_pod_dir)
        log_dir = os.path.join(out_pod_dir, "log")
        os.mkdir(log_dir)
        if tail_size == 0:
            subprocess_run(f"kubectl -n {namespace} cp {pod}:log {log_dir}", debug)
        else:
            log_files = subprocess_run(f"kubectl -n {namespace} exec pod/{str(pod)} -- ls log", debug)
            for log_file in log_files.splitlines():
                subprocess_run(
                    f"kubectl -n {namespace} exec pod/{pod} -- tail log/{log_file} -n {tail_size} "
                    "> {os.path.join(log_dir, log_file)}",
                    debug)
    print("Done copying logs")


def subprocess_run(command, debug):
    if debug:
        print("command=" + command)
    result = subprocess.run(command, shell=True, universal_newlines=True, stdout=subprocess.PIPE)
    if result.returncode != 0:
        raise SubprocessRunException(result.returncode, command, result.stdout)
    return str(result.stdout)


class SubprocessRunException(Exception):
    """Exception for non-zero result code.

    Attributes:
        returncode -- command return code
        command -- command
        stdout -- stdout
    """
    def __init__(self, returncode, command, stdout):
        self.returncode = returncode
        self.command = command
        self.stdout = stdout
