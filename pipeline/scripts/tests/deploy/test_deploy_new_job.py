import tempfile
import unittest
from unittest.mock import call, patch
from deploy.config import new_config
from deploy.deploy_new_job import deploy_new_job


# Creates a Dict in a similar form as we'd expect from Flink's REST API.
def job_dict(id, name, state, start_time):
    return {
        "jid": id,
        "name": name,
        "state": state,
        "start-time": start_time,
    }


class DeployNewJobTest(unittest.TestCase):

    # Mock the print so our tests are not printing lines of code.
    @patch('builtins.print')
    @patch('deploy.deploy_new_job.wait_for_flink_job')
    @patch('deploy.deploy_new_job.subprocess_kubernetes_delete_then_apply')
    @patch('deploy.deploy_new_job.create_modified_kubernetes_config')
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

        config = new_config({
            "namespace": "ns",
            "job_name": "log-user",
            "sub_job_type": "RAW_LOG_USER",
            "new": True,
            "stream_job_file": original_fp.name,
        })
        mock_create_modified_kubernetes_config.return_value = new_fp.name
        deploy_new_job(config)
        self.assertEqual(mock_create_modified_kubernetes_config.call_count, 1)
        mock_create_modified_kubernetes_config.assert_has_calls([call(config)])
        self.assertEqual(mock_subprocess_kubernetes_delete_then_apply.call_count, 1)
        mock_subprocess_kubernetes_delete_then_apply.assert_has_calls([call("ns", new_fp.name, False)])
        self.assertEqual(mock_wait_for_flink_job.call_count, 1)
        mock_wait_for_flink_job.assert_has_calls([call("ns", "log-user", None, False)])
