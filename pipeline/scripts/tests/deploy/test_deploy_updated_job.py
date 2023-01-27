import tempfile
import unittest
from unittest.mock import call, patch
from deploy.config import new_config
from deploy.deploy_updated_job import create_job_from_savepoint
from deploy.deploy_updated_job import stop_job, update_existing_job


# Creates a Dict in a similar form as we'd expect from Flink's REST API.
def job_dict(id, name, state, start_time):
    return {
        "jid": id,
        "name": name,
        "state": state,
        "start-time": start_time,
    }


def get_test_config():
    return new_config({
        "namespace": "ns",
        "job_name": "log-user",
        "sub_job_type": "RAW_LOG_USER",
        "new": True,
    })


class UpdateExistingJobTest(unittest.TestCase):

    # Mock the print so our tests are not printing lines of code.
    @patch('builtins.print')
    @patch('deploy.deploy_updated_job.create_job_from_savepoint')
    @patch('deploy.deploy_updated_job.stop_job')
    def test_success(self, mock_stop_job, mock_create_job_from_savepoint, mock_print):
        s3_savepoint_path = "s3a://example/path"
        mock_stop_job.return_value = s3_savepoint_path
        stream_job_config_path = "~/some/k8/config.yaml"
        config = get_test_config()
        config = config._replace(new=False, stream_job_file=stream_job_config_path)
        self.assertEqual(
            update_existing_job(job_dict("10", "log-user", "RUNNING", 1000), config),
            s3_savepoint_path)
        self.assertEqual(mock_stop_job.call_count, 1)
        mock_stop_job.assert_has_calls([call("ns", "10", False)])
        self.assertEqual(mock_create_job_from_savepoint.call_count, 1)
        copy_config = config._replace(start_from_savepoint="s3a://example/path")
        mock_create_job_from_savepoint.assert_has_calls([
            call("10", copy_config)
        ])


class StopJobTest(unittest.TestCase):

    @patch('builtins.print')
    @patch('deploy.deploy_updated_job.wait_for_savepoint')
    @patch('deploy.deploy_updated_job.async_stop_job')
    def test_success(self, mock_async_stop_job, mock_wait_for_savepoint, mock_print):
        mock_async_stop_job.return_value = 'requestId1'
        s3_savepoint_path = "s3a://example/path"
        mock_wait_for_savepoint.return_value = s3_savepoint_path
        self.assertEqual(stop_job("ns", "jobId1", False), s3_savepoint_path)
        mock_async_stop_job.assert_has_calls([
            call("ns", "jobId1", False)
        ])
        mock_wait_for_savepoint.assert_has_calls([
            call("ns", "jobId1", "requestId1")
        ])

    @patch('builtins.print')
    @patch('deploy.deploy_updated_job.wait_for_savepoint')
    @patch('deploy.deploy_updated_job.async_stop_job')
    def test_dry_run(self, mock_async_stop_job, mock_wait_for_savepoint, mock_print):
        mock_async_stop_job.return_value = 'requestId1'
        self.assertEqual(stop_job("ns", "jobId1", True), "s3://fake-path/for/dry/run")
        mock_async_stop_job.assert_has_calls([
            call("ns", "jobId1", True)
        ])
        mock_wait_for_savepoint.assert_not_called()


class CreateJobFromSavepointTest(unittest.TestCase):

    # Mock the print so our tests are not printing lines of code.
    @patch('builtins.print')
    @patch('deploy.deploy_updated_job.wait_for_flink_job')
    @patch('deploy.deploy_updated_job.subprocess_kubernetes_delete_then_apply')
    @patch('deploy.deploy_updated_job.create_modified_kubernetes_config')
    def test_success(self,
                     mock_create_modified_kubernetes_config,
                     mock_subprocess_kubernetes_delete_then_apply,
                     mock_wait_for_flink_job,
                     mock_print):

        original_fp = tempfile.NamedTemporaryFile(delete=False)
        with open(original_fp.name, 'w') as f:
            f.write("original")

        new_fp = tempfile.NamedTemporaryFile(delete=False)
        with open(new_fp.name, 'w') as f:
            f.write("new")

        config = get_test_config()
        config = config._replace(stream_job_file=original_fp.name, start_from_savepoint="s3a://example/path")
        mock_create_modified_kubernetes_config.return_value = new_fp.name

        create_job_from_savepoint("jobId1", config)
        self.assertEqual(mock_create_modified_kubernetes_config.call_count, 1)
        mock_create_modified_kubernetes_config.assert_has_calls([call(config)])
        self.assertEqual(mock_subprocess_kubernetes_delete_then_apply.call_count, 1)
        mock_subprocess_kubernetes_delete_then_apply.assert_has_calls([call("ns", new_fp.name, False)])
        self.assertEqual(mock_wait_for_flink_job.call_count, 1)
        mock_wait_for_flink_job.assert_has_calls([call("ns", "log-user", "jobId1", False)])
