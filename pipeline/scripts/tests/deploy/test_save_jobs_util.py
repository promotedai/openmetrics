import json
import tempfile
import unittest
from unittest.mock import Mock, call, patch
from deploy.config import new_config
from deploy.save_jobs_util import async_stop_job, create_modified_kubernetes_config, get_savepoint_summary
from deploy.save_jobs_util import subprocess_kubernetes_delete_then_apply, wait_for_savepoint
from deploy.unexpected_value_error import UnexpectedValueError


class SubprocessKubernetesDeleteThenApplyTest(unittest.TestCase):

    @patch('subprocess.run')
    def test_success(self, mock_subprocess_run):
        process_mock = Mock()
        attrs = {
            "returncode": 0,
            "stdout": "success"
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_run.return_value = process_mock
        subprocess_kubernetes_delete_then_apply("ns", "~/config/path", False)
        self.assertEqual(mock_subprocess_run.call_count, 1)
        mock_subprocess_run.assert_has_calls([
            call("kubectl delete -n ns -f ~/config/path; kubectl apply -n ns -f ~/config/path",
                 capture_output=True, shell=True)
        ])

    @patch('subprocess.run')
    def test_failure(self, mock_subprocess_run):
        process_mock = Mock()
        attrs = {
            "returncode": 1,
            "stdout": "failed"
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_run.return_value = process_mock
        self.assertRaises(UnexpectedValueError, subprocess_kubernetes_delete_then_apply, "ns", "~/config/path", False)
        self.assertEqual(mock_subprocess_run.call_count, 1)
        mock_subprocess_run.assert_has_calls([
            call("kubectl delete -n ns -f ~/config/path; kubectl apply -n ns -f ~/config/path",
                 capture_output=True, shell=True)
        ])

    @patch('subprocess.run')
    def test_dry_run(self, mock_subprocess_run):
        subprocess_kubernetes_delete_then_apply("ns", "~/config/path", True)
        mock_subprocess_run.assert_not_called()


class AsyncStopJobTest(unittest.TestCase):

    @patch('subprocess.run')
    def test_success(self, mock_subprocess_run):
        process_mock = Mock()
        attrs = {
            "returncode": 0,
            "stdout": json.dumps({
                "request-id": "requestId1"
            })
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_run.return_value = process_mock
        self.assertEqual(
            async_stop_job("ns", "jobId1", False),
            "requestId1"
        )
        self.assertEqual(mock_subprocess_run.call_count, 1)
        mock_subprocess_run.assert_has_calls([
            call("kubectl exec -n ns -i -t flink-jobmanager-0 -- curl -d '{\"drain\":false}' "
                 "-H \"Content-Type: application/json\" -X POST http://localhost:8081/jobs/jobId1/stop",
                 capture_output=True, shell=True)
        ])

    @patch('subprocess.run')
    def test_failure(self, mock_subprocess_run):
        process_mock = Mock()
        attrs = {
            "returncode": 1,
            "stdout": json.dumps({
                "jobs": []
            })
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_run.return_value = process_mock
        self.assertRaises(UnexpectedValueError, async_stop_job, "ns", "jobId1", False)

    @patch('subprocess.run')
    def test_dry_run(self, mock_subprocess_run):
        self.assertEqual(
            async_stop_job("ns", "jobId1", True),
            None
        )
        mock_subprocess_run.assert_not_called()


class WaitForSavepointJobTest(unittest.TestCase):

    @patch('builtins.print')
    @patch('time.sleep')
    @patch('deploy.save_jobs_util.get_savepoint_summary')
    def test_one_wait(self, mock_get_savepoint_summary, mock_sleep, mock_print):
        # Return an unsupported response and finish with a supported response.
        mock_get_savepoint_summary.side_effect = [
            {},
            {
                "status": {
                    "id": "COMPLETED"
                },
                "operation": {
                    "location": "s3a://promoted-event-logs/savepoints/savepoint-ff18f0-28f6f5ba8d95"
                }
            }
        ]
        wait_for_savepoint("ns", "jobId1", "requestId1")
        self.assertEqual(mock_sleep.call_count, 1)
        mock_sleep.assert_has_calls([call(1)])

    @patch('builtins.print')
    @patch('time.sleep')
    @patch('deploy.save_jobs_util.get_savepoint_summary')
    def test_two_waits(self, mock_get_savepoint_summary, mock_sleep, mock_print):
        # Return two unsupported responses and finish with a supported response.
        mock_get_savepoint_summary.side_effect = [
            {},
            {},
            {
                "status": {
                    "id": "COMPLETED"
                },
                "operation": {
                    "location": "s3a://promoted-event-logs/savepoints/savepoint-ff18f0-28f6f5ba8d95"
                }
            }
        ]
        wait_for_savepoint("ns", "jobId1", "requestId1")
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_has_calls([call(1), call(1)])

    @patch('builtins.print')
    @patch('time.sleep')
    @patch('deploy.save_jobs_util.get_savepoint_summary')
    def test_multiple_calls(self, mock_get_savepoint_summary, mock_sleep, mock_print):
        # Return multiple unsupported responses and finish with a supported response.
        mock_get_savepoint_summary.side_effect = [
            {},
            {},
            {},
            {},
            {},
            {},
            {
                "status": {
                    "id": "COMPLETED"
                },
                "operation": {
                    "location": "s3a://promoted-event-logs/savepoints/savepoint-ff18f0-28f6f5ba8d95"
                }
            }
        ]
        wait_for_savepoint("ns", "jobId1", "requestId1")
        self.assertEqual(mock_sleep.call_count, 6)
        mock_sleep.assert_has_calls([call(1), call(1), call(1), call(1), call(1), call(5)])

    @patch('builtins.print')
    @patch('time.sleep')
    @patch('deploy.save_jobs_util.get_savepoint_summary')
    def test_bad_status(self, mock_get_savepoint_summary, mock_sleep, mock_print):
        # Return an unsupported response and finish with a supported failed response.
        mock_get_savepoint_summary.side_effect = [
            {},
            {
                "status": {
                    "id": "FAILED"
                }
            }
        ]
        self.assertRaises(UnexpectedValueError, wait_for_savepoint, "ns", "jobId1", "requestId1")
        mock_sleep.assert_has_calls([call(1)])

    @patch('builtins.print')
    @patch('time.sleep')
    @patch('deploy.save_jobs_util.get_savepoint_summary')
    def test_no_location(self, mock_get_savepoint_summary, mock_sleep, mock_print):
        # Return an unsupported response and finish with a bad response.
        mock_get_savepoint_summary.side_effect = [
            {},
            {},
            {
                "status": {
                    "id": "COMPLETED"
                },
                "operation": {
                    "location": ""
                }
            }
        ]
        self.assertRaises(UnexpectedValueError, wait_for_savepoint, "ns", "jobId1", "requestId1")
        mock_sleep.assert_has_calls([call(1)])


class GetSavepointSummaryTest(unittest.TestCase):

    @patch('subprocess.run')
    def test_success(self, mock_subprocess_run):
        process_mock = Mock()
        attrs = {
            "returncode": 0,
            "stdout": json.dumps({
                "fake": "result"
            })
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_run.return_value = process_mock
        self.assertEqual(
            get_savepoint_summary("ns", "jobId1", "requestId1"),
            {
                "fake": "result"
            }
        )
        self.assertEqual(mock_subprocess_run.call_count, 1)
        mock_subprocess_run.assert_has_calls([
            call('kubectl exec -n ns -i -t flink-jobmanager-0 -- '
                 'curl http://localhost:8081/jobs/jobId1/savepoints/requestId1',
                 capture_output=True, shell=True)
        ])

    @patch('subprocess.run')
    def test_failure(self, mock_subprocess_run):
        process_mock = Mock()
        attrs = {
            "returncode": 1,
            "stdout": json.dumps({
                "fake": "result"
            })
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_run.return_value = process_mock
        self.assertRaises(UnexpectedValueError, get_savepoint_summary, "ns", "jobId1", "requestId1")


def get_test_config():
    return new_config({
        "sub_job_type": "RAW_LOG_USER",
        "new": True,
    })


class CreateModifiedKubernetesConfigTest(unittest.TestCase):
    def test_success_with_no_additional_args(self):
        input = """spec:
        template:
            spec:
            containers:
            - name: client
                image: 055315558257.dkr.ecr.us-east-1.amazonaws.com/prm-metrics-flat-output-job:dev-latest
                imagePullPolicy: "Always"
                env:
                - name: JOB_MANAGER_RPC_ADDRESS
                value: jobmanager
                command: ["flink", "run", "-d", "/opt/FlatOutputJob_deploy.jar"]
                args: [
                "--platform=prm",
                "--s3Bucket=prm-dev-event-logs"
                ]
        """

        expected = """spec:
        template:
            spec:
            containers:
            - name: client
                image: 055315558257.dkr.ecr.us-east-1.amazonaws.com/prm-metrics-flat-output-job:dev-latest
                imagePullPolicy: "Always"
                env:
                - name: JOB_MANAGER_RPC_ADDRESS
                value: jobmanager
                command: ["flink", "run", "-d", "/opt/FlatOutputJob_deploy.jar"]
                args: ["--subJob=RAW_LOG_USER",
                "--platform=prm",
                "--s3Bucket=prm-dev-event-logs"
                ]
        """

        original_fp = tempfile.NamedTemporaryFile(delete=False)
        with open(original_fp.name, 'w') as f:
            f.write(input)

        config = get_test_config()
        config = config._replace(stream_job_file=original_fp.name)
        new_fp_path = create_modified_kubernetes_config(config)
        with open(new_fp_path) as f:
            self.assertEqual(f.read(), expected)

    def test_success_with_savepoint(self):
        input = """spec:
        template:
            spec:
            containers:
            - name: client
                image: 055315558257.dkr.ecr.us-east-1.amazonaws.com/prm-metrics-flat-output-job:dev-latest
                imagePullPolicy: "Always"
                env:
                - name: JOB_MANAGER_RPC_ADDRESS
                value: jobmanager
                command: ["flink", "run", "-d", "/opt/FlatOutputJob_deploy.jar"]
                args: [
                "--platform=prm",
                "--s3Bucket=prm-dev-event-logs"
                ]
        """

        s3_savepoint_path = "s3a://some/path"
        expected = """spec:
        template:
            spec:
            containers:
            - name: client
                image: 055315558257.dkr.ecr.us-east-1.amazonaws.com/prm-metrics-flat-output-job:dev-latest
                imagePullPolicy: "Always"
                env:
                - name: JOB_MANAGER_RPC_ADDRESS
                value: jobmanager
                command: ["flink", "run", "-s", "s3a://some/path", "-d", "/opt/FlatOutputJob_deploy.jar"]
                args: ["--subJob=RAW_LOG_USER",
                "--platform=prm",
                "--s3Bucket=prm-dev-event-logs"
                ]
        """

        original_fp = tempfile.NamedTemporaryFile(delete=False)
        with open(original_fp.name, 'w') as f:
            f.write(input)

        config = get_test_config()
        config = config._replace(stream_job_file=original_fp.name, start_from_savepoint=s3_savepoint_path)
        new_fp_path = create_modified_kubernetes_config(config)
        with open(new_fp_path) as f:
            self.assertEqual(f.read(), expected)

    def test_success_allow_non_restore_state(self):
        input = """spec:
        template:
            spec:
            containers:
            - name: client
                image: 055315558257.dkr.ecr.us-east-1.amazonaws.com/prm-metrics-flat-output-job:dev-latest
                imagePullPolicy: "Always"
                env:
                - name: JOB_MANAGER_RPC_ADDRESS
                value: jobmanager
                command: ["flink", "run", "-d", "/opt/FlatOutputJob_deploy.jar"]
                args: [
                "--platform=prm",
                "--s3Bucket=prm-dev-event-logs"
                ]
        """

        s3_savepoint_path = "s3a://some/path"
        expected = """spec:
        template:
            spec:
            containers:
            - name: client
                image: 055315558257.dkr.ecr.us-east-1.amazonaws.com/prm-metrics-flat-output-job:dev-latest
                imagePullPolicy: "Always"
                env:
                - name: JOB_MANAGER_RPC_ADDRESS
                value: jobmanager
                command: ["flink", "run", "-s", "s3a://some/path", "--allowNonRestoredState", "-d", "/opt/FlatOutputJob_deploy.jar"]
                args: ["--subJob=RAW_LOG_USER",
                "--platform=prm",
                "--s3Bucket=prm-dev-event-logs"
                ]
        """  # noqa: E501
        original_fp = tempfile.NamedTemporaryFile(delete=False)
        with open(original_fp.name, 'w') as f:
            f.write(input)

        config = new_config({
            "sub_job_type": "RAW_LOG_USER",
            "new": True,
            "stream_job_file": original_fp.name,
            "start_from_savepoint": s3_savepoint_path,
            "allow_non_restored_state": True,
        })
        new_fp_path = create_modified_kubernetes_config(config)
        with open(new_fp_path) as f:
            self.assertEqual(f.read(), expected)
