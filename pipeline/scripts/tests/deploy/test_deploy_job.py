import unittest
from unittest.mock import call, patch
from deploy.config import new_config
from deploy.deploy_job import deploy_job, validate_stream_job_file_arg
from deploy.unexpected_value_error import UnexpectedValueError


# Creates a Dict in a similar form as we'd expect from Flink's REST API.
def job_dict(id, name, state, start_time):
    return {
        "jid": id,
        "name": name,
        "state": state,
        "start-time": start_time,
    }


class ValidateStreamJobFileArgTest(unittest.TestCase):

    @patch('os.path.isfile')
    def test_success(self, mock_isfile):
        mock_isfile.return_value = True
        validate_stream_job_file_arg("~/stream/job/file.yaml")
        mock_isfile.assert_has_calls([call("~/stream/job/file.yaml")])

    def test_error_empty(self):
        self.assertRaises(UnexpectedValueError, validate_stream_job_file_arg, "")

    @patch('os.path.isfile')
    def test_error_file_does_not_exist(self, mock_isfile):
        mock_isfile.return_value = False
        self.assertRaises(UnexpectedValueError, validate_stream_job_file_arg, "~/stream/job/file.yaml")
        self.assertEqual(mock_isfile.call_count, 1)
        mock_isfile.assert_has_calls([call("~/stream/job/file.yaml")])


class DeployJobTest(unittest.TestCase):

    # Mock the print so our tests are not printing lines of code.
    @patch('builtins.print')
    @patch('deploy.deploy_job.update_existing_job')
    @patch('deploy.deploy_job.deploy_new_job')
    @patch('deploy.deploy_job.get_job_name_to_latest_jobs')
    def test_existing_job_redeploy(self, mock_get_job_name_to_latest_jobs, mock_deploy_new_job,
                                   mock_update_existing_job, mock_print):
        mock_get_job_name_to_latest_jobs.return_value = {
            "log-user": job_dict("10", "log-user", "RUNNING", 1000)
        }
        mock_update_existing_job.return_value = "s3://some/path"
        stream_job_config_path = "~/some/k8/config.yaml"
        config = new_config({
            "namespace": "ns",
            "job_name": "log-user",
            "sub_job_type": "RAW_LOG_USER",
            "stream_job_file": stream_job_config_path,
        })
        self.assertEqual(deploy_job(config), config)
        self.assertFalse(mock_deploy_new_job.called)
        self.assertEqual(mock_update_existing_job.call_count, 1)
        mock_update_existing_job.assert_has_calls([call(job_dict("10", "log-user", "RUNNING", 1000), config)])

    # Mock the print so our tests are not printing lines of code.
    @patch('builtins.print')
    @patch('deploy.deploy_job.update_existing_job')
    @patch('deploy.deploy_job.deploy_new_job')
    @patch('deploy.deploy_job.get_job_name_to_latest_jobs')
    def test_existing_job_no_redeploy(self, mock_get_job_name_to_latest_jobs, mock_deploy_new_job,
                                      mock_update_existing_job, mock_print):
        mock_get_job_name_to_latest_jobs.return_value = {
            "log-user": job_dict("10", "log-user", "RUNNING", 1000)
        }
        s3_savepoint_path = "s3://some/path"
        mock_update_existing_job.return_value = s3_savepoint_path
        stream_job_config_path = "~/some/k8/config.yaml"
        config = new_config({
            "namespace": "ns",
            "job_name": "log-user",
            "sub_job_type": "RAW_LOG_USER",
            "deploy": False,
            "stream_job_file": stream_job_config_path,
        })
        cont_config = config._replace(start_from_savepoint=s3_savepoint_path, deploy=True)
        self.assertEqual(deploy_job(config), cont_config)
        self.assertFalse(mock_deploy_new_job.called)
        self.assertEqual(mock_update_existing_job.call_count, 1)
        mock_update_existing_job.assert_has_calls([call(job_dict("10", "log-user", "RUNNING", 1000), config)])

    # Mock the print so our tests are not printing lines of code.
    @patch('builtins.print')
    @patch('deploy.deploy_job.update_existing_job')
    @patch('deploy.deploy_job.deploy_new_job')
    @patch('deploy.deploy_job.get_job_name_to_latest_jobs')
    def test_new_job(self, mock_get_job_name_to_latest_jobs, mock_deploy_new_job, mock_update_existing_job,
                     mock_print):
        mock_get_job_name_to_latest_jobs.return_value = {}
        stream_job_config_path = "~/some/k8/config.yaml"
        config = new_config({
            "namespace": "ns",
            "job_name": "log-user",
            "sub_job_type": "RAW_LOG_USER",
            "stream_job_file": stream_job_config_path,
        })
        self.assertEqual(deploy_job(config), config)
        mock_deploy_new_job.assert_has_calls([call(config)])
        self.assertFalse(mock_update_existing_job.called)
