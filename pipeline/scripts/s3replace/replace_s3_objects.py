import boto3
import concurrent.futures
import os
import re
from retrying import retry
from tap import Tap
from typing import Iterator, List, Optional, Set, Tuple
import tempfile


# Algorithm:
# 1. Validate inputs.
# 2. Create a list of `dt`s to process.
# 3. For each dt:
#   3a. Clear out the destination directory.
#   3b. Add a 'special-cleared-destination' object to the source directory to indicate
#       that the destination has been cleared.
#   3c. Copy over objects from objects to destination.
#   3d. Delete source objects.


# We write a special object indiciating that we've cleared the destination.
# If this object is written, do not clear it again.
cleared_destination_object = 'special-cleared-destination'
full_copy_done = 'special-full-copy-done'


class ReplaceArgs(Tap):
    region: str
    bucket: str
    source_key_prefix: str
    destination_key_prefix: str
    start_dt: str  # inclusive YYYY-MM-DD
    exclusive_end_dt: str  # YYYY-MM-DD
    apply: bool = False
    force: bool = False
    num_workers: Optional[int] = None


def replace_s3_objects(args: ReplaceArgs):
    if not args.source_key_prefix.endswith('/'):
        args.source_key_prefix = args.source_key_prefix + '/'
    if not args.destination_key_prefix.endswith('/'):
        args.destination_key_prefix = args.destination_key_prefix + '/'

    s3 = boto3.client('s3', region_name=args.region)
    source_keys = retrieve_s3_objects(s3, args.bucket, args.source_key_prefix)
    validate_has_dt_after_prefix(source_keys, args.source_key_prefix)
    print_list("source_keys", source_keys)

    source_dts = filter_dts(parse_dts(source_keys), args.start_dt, args.exclusive_end_dt)
    source_dts_list = list(source_dts)
    source_dts_list.sort()
    print_list("source_dts", source_dts_list)

    destination_keys = retrieve_s3_objects(s3, args.bucket, args.destination_key_prefix)
    validate_has_dt_after_prefix(destination_keys, args.destination_key_prefix)

    destination_dts = filter_dts(parse_dts(destination_keys), args.start_dt, args.exclusive_end_dt)
    destination_dts_list = list(destination_dts)
    destination_dts_list.sort()
    print_list("destination_dts", destination_dts_list)

    missing_dts = destination_dts.difference(source_dts)
    if not args.force and len(missing_dts) > 0:
        missing_dts = list(missing_dts)
        missing_dts.sort()
        raise Exception(f"Encountered missing source days.  If this is intentional, "
                        f"run again with --force.  missing_dts={missing_dts}")

    missing_dts = source_dts.difference(destination_dts)
    if not args.force and len(missing_dts) > 0:
        missing_dts = list(missing_dts)
        missing_dts.sort()
        raise Exception(f"Encountered missing destination days.  If this is intentional, "
                        f"run again with --force.  missing_dts={missing_dts}")

    dts = list(destination_dts.union(source_dts))
    dts.sort()
    for dt in dts:
        replace_for_dt(s3,
                       args.num_workers,
                       args.apply,
                       args.bucket,
                       dt,
                       args.source_key_prefix,
                       source_keys,
                       args.destination_key_prefix,
                       destination_keys)


def validate_has_dt_after_prefix(keys: List[str], prefix: str):
    prefix = prefix + "dt="
    for key in keys:
        if not key.startswith(prefix):
            raise Exception(f"found an unexpected object; key={key}, requiredprefix={prefix}")


def print_list(header: str, list: List[str]):
    if len(list) == 0:
        return
    print(header)
    for value in list:
        print(value)
    print("")


def replace_for_dt(
        s3,
        num_workers: Optional[int],
        apply: bool,
        bucket: str,
        dt: str,
        source_prefix: str,
        source_keys: List[str],
        destination_prefix: str,
        destination_keys: List[str]):

    source_keys = filter_keys(source_keys, dt)
    destination_keys = filter_keys(destination_keys, dt)
    source_keys, cleared_object_keys, full_copy_done_keys = split_process_objects(source_keys)

    if len(full_copy_done_keys) > 0:
        print("Skipping dt=" + dt + ".  Already copied")
        return

    if len(cleared_object_keys) == 0:
        if apply:
            delete_objects(s3, destination_keys)
            cleared_object_key = f"{trim_separator(source_prefix)}/dt={dt}/{cleared_destination_object}"
            write_special_object(s3, bucket, cleared_object_key)
            cleared_object_keys.append(cleared_object_key)
        else:
            print_list("Would delete these objects", destination_keys)

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        # TODO - maybe move this check to an early validation step.
        for source_key in source_keys:
            if not source_key.startswith(source_prefix):
                raise Exception(f"source_key needs to start with source_prefix; source_key="
                                f"{source_key}, source_prefix={source_prefix}")
            destination_key = (
                f"{trim_separator(destination_prefix)}/{trim_separator(source_key[len(source_prefix):])}")
            if apply:
                futures.append(executor.submit(copy_object, s3, bucket, source_key, destination_key))
            else:
                print(f"Would move CopySource={source_key}, Key={destination_key}")
                print("")
        for future in concurrent.futures.as_completed(futures):
            future.result()

    full_copy_done_object_key = f"{trim_separator(source_prefix)}/dt={dt}/{full_copy_done}"
    write_special_object(s3, bucket, full_copy_done_object_key)


def trim_separator(s: str) -> str:
    while s.startswith('/'):
        s = s[1:]
    while s.endswith('/'):
        s = s[:-1]
    return s


def split_process_objects(keys: List[str]) -> Tuple[List[str], List[str], List[str]]:
    normal_keys = []
    cleared_object_keys = []
    full_copy_done_object_keys = []
    for key in keys:
        if cleared_destination_object in key:
            cleared_object_keys.append(key)
        elif full_copy_done in key:
            full_copy_done_object_keys.append(key)
        else:
            normal_keys.append(key)
    return (normal_keys, cleared_object_keys, full_copy_done_object_keys)


def parse_dts(object_keys: List[str]) -> Set[str]:
    dts = set()
    for object_key in object_keys:
        result = re.search(r"dt=(\d{4}-\d{2}-\d{2})", object_key)
        dts.add(result.group(1))
    return dts


def filter_dts(dts: Set[str], start_dt: str, exclusive_end_dt: str) -> Set[str]:
    result = set()
    for dt in dts:
        if start_dt <= dt and dt < exclusive_end_dt:
            result.add(dt)
    return result


def filter_keys(keys: List[str], dt: str) -> List[str]:
    result = []
    filter = "/dt=" + dt
    for key in keys:
        if filter in key:
            result.append(key)
    return result


def chunk(list: List[str], chunk_size: int) -> Iterator[List[str]]:
    for i in range(0, len(list), chunk_size):
        yield list[i:i + chunk_size]


@retry(stop_max_attempt_number=10, wait_random_min=1000, wait_random_max=30000)
def copy_object(s3, bucket: str, source_key: str, destination_key: str):
    print("copy_object CopySource=" + source_key + ", Key=" + destination_key)
    s3.copy_object(
        Bucket=bucket,
        CopySource={
            'Bucket': bucket,
            'Key': source_key,
        },
        Key=destination_key,
    )


@retry(stop_max_attempt_number=10, wait_random_min=1000, wait_random_max=30000)
def delete_objects(s3, keys: List[str]):
    for keys in chunk(keys, 1000):
        for key in keys:
            print("delete_object key=" + key)
        s3.delete_objects(
            Bucket=args.bucket,
            Delete={
                'Objects': [{'Key': key} for key in keys],
                'Quiet': False,
            },
        )


def write_special_object(s3, bucket: str, object_key: str):
    try:
        clearned_file: Optional[str] = None
        with tempfile.NamedTemporaryFile('w+b', delete=False) as tmp:
            write_file_name = tmp.name
            tmp.close()
            with open(write_file_name, 'rb') as data:
                print("put_object key=" + object_key)
                s3.put_object(
                    Bucket=bucket,
                    Body=data,
                    Key=object_key
                )
    finally:
        if clearned_file:
            os.remove(clearned_file)


def retrieve_s3_objects(s3, bucket: str, prefix: str) -> List[str]:
    result = []

    response = s3.list_objects_v2(
        Bucket=bucket,
        MaxKeys=1000,
        Prefix=prefix,
    )
    for object in response.get('Contents', []):
        result.append(object['Key'])

    while response['IsTruncated']:
        response = s3.list_objects_v2(
            Bucket=bucket,
            MaxKeys=1000,
            Prefix=prefix,
            ContinuationToken=response['NextContinuationToken'],
        )
        for object in response.get('Contents', []):
            result.append(object['Key'])
    return result


if __name__ == "__main__":
    args = ReplaceArgs().parse_args()
    replace_s3_objects(args)
