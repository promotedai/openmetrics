import unittest
from deploy.config import new_config, to_flags


class ToFlagsTest(unittest.TestCase):
    def test_min_args(self):
        self.assertEqual(
            to_flags(new_config({
                "namespace": "default",
                "job_name": "log-user",
                "stream_job_file": "/some/config",
                "sub_job_type": None,
            })),
            "--namespace default --job-name log-user --stream-job-file /some/config"
        )

    def test_max_args(self):
        self.assertEqual(
            to_flags(new_config({
                "namespace": "default",
                "job_name": "log-user",
                "stream_job_file": "/some/config",
                "sub_job_type": 'RAW_LOG_USER',
                "new": True,
                "create_new_if_stopped": True,
                "deploy": False,
                "start_from_savepoint": "s3://fake/path",
                "allow_non_restored_state": True,
                "dry_run": True,
            })),
            "--namespace default --job-name log-user --stream-job-file /some/config "
            "--sub-job-type RAW_LOG_USER --new --create-new-if-stopped --no-deploy "
            "--start-from-savepoint s3://fake/path --allow-non-restored-state --dry-run"
        )
