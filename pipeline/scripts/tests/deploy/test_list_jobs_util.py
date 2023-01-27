import json
import unittest
from unittest.mock import Mock, call, patch
from deploy.list_jobs_util import get_job_name_to_latest_jobs, is_stopped, wait_for_flink_job
from deploy.unexpected_value_error import UnexpectedValueError


def job_dict(id, name, state, start_time):
    return {
        "jid": id,
        "name": name,
        "state": state,
        "start-time": start_time,
    }


class IsStoppedTest(unittest.TestCase):
    def test_is_stopped(self):
        self.assertTrue(is_stopped("FAILED"))
        self.assertTrue(is_stopped("FINISHED"))
        self.assertFalse(is_stopped("CREATED"))
        self.assertFalse(is_stopped("RUNNING"))


class GetJobNameToLatestJobsTest(unittest.TestCase):

    @patch('time.sleep')
    @patch('subprocess.run')
    def test_failure(self, mock_subprocess_run, mock_sleep):
        process_mock = Mock()
        attrs = {
            "returncode": 1,
            "stdout": json.dumps({
                "jobs": []
            })
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_run.return_value = process_mock
        self.assertRaises(UnexpectedValueError, get_job_name_to_latest_jobs, "ns")

    @patch('subprocess.run')
    def test_one_success(self, mock_subprocess_run):
        process_mock = Mock()
        attrs = {
            "returncode": 0,
            "stdout": json.dumps({
                "jobs": [
                    job_dict("123", "log-user", "RUNNING", 123)
                ]
            })
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_run.return_value = process_mock
        self.assertEqual(
            get_job_name_to_latest_jobs("ns"),
            {
                "log-user": job_dict("123", "log-user", "RUNNING", 123)
            }
        )
        self.assertEqual(mock_subprocess_run.call_count, 1)
        mock_subprocess_run.assert_has_calls([
            call('kubectl exec -n ns -i -t flink-jobmanager-0 -- curl http://localhost:8081/jobs/overview',
                 capture_output=True, shell=True)
        ])

    @patch('subprocess.run')
    def test_multiple_early_first(self, mock_subprocess_run):
        process_mock = Mock()
        attrs = {
            "returncode": 0,
            "stdout": json.dumps({
                "jobs": [
                    job_dict("123", "log-user", "FAILED", 123),
                    job_dict("234", "log-user", "RUNNING", 124),
                ]
            })
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_run.return_value = process_mock
        self.assertEqual(
            get_job_name_to_latest_jobs("ns"),
            {
                "log-user": job_dict("234", "log-user", "RUNNING", 124),
            }
        )
        self.assertEqual(mock_subprocess_run.call_count, 1)
        mock_subprocess_run.assert_has_calls([
            call('kubectl exec -n ns -i -t flink-jobmanager-0 -- curl http://localhost:8081/jobs/overview',
                 capture_output=True, shell=True)
        ])

    @patch('subprocess.run')
    def test_multiple_early_last(self, mock_subprocess_run):
        process_mock = Mock()
        attrs = {
            "returncode": 0,
            "stdout": json.dumps({
                "jobs": [
                    job_dict("234", "log-user", "RUNNING", 124),
                    job_dict("123", "log-user", "FAILED", 123),
                ]
            })
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_run.return_value = process_mock
        self.assertEqual(
            get_job_name_to_latest_jobs("ns"),
            {
                "log-user": job_dict("234", "log-user", "RUNNING", 124),
            }
        )
        self.assertEqual(mock_subprocess_run.call_count, 1)
        mock_subprocess_run.assert_has_calls([
            call('kubectl exec -n ns -i -t flink-jobmanager-0 -- curl http://localhost:8081/jobs/overview',
                 capture_output=True, shell=True)
        ])

    @patch('subprocess.run')
    def test_multiple_mixed(self, mock_subprocess_run):
        process_mock = Mock()
        attrs = {
            "returncode": 0,
            "stdout": json.dumps({
                "jobs": [
                    job_dict("10", "log-user", "RUNNING", 1000),
                    job_dict("1", "log-user", "FAILING", 100),
                    job_dict("3", "log-user", "FAILED", 120),
                    job_dict("11", "log-cohort-membership", "RUNNING", 1100),
                    job_dict("12", "log-view", "RUNNING", 1200),
                    job_dict("2", "log-cohort-membership", "FAILED", 110),
                ]
            })
        }
        process_mock.configure_mock(**attrs)
        mock_subprocess_run.return_value = process_mock
        self.assertEqual(
            get_job_name_to_latest_jobs("ns"),
            {
                "log-user": job_dict("10", "log-user", "RUNNING", 1000),
                "log-cohort-membership": job_dict("11", "log-cohort-membership", "RUNNING", 1100),
                "log-view": job_dict("12", "log-view", "RUNNING", 1200),
            }
        )
        self.assertEqual(mock_subprocess_run.call_count, 1)
        mock_subprocess_run.assert_has_calls([
            call('kubectl exec -n ns -i -t flink-jobmanager-0 -- curl http://localhost:8081/jobs/overview',
                 capture_output=True, shell=True)
        ])


class WaitForFlinkJobTest(unittest.TestCase):
    @patch('builtins.print')
    @patch('time.sleep')
    @patch('deploy.list_jobs_util.get_job_name_to_latest_jobs')
    def test_no_previous_job(self, mock_get_job_name_to_latest_jobs, mock_sleep, mock_print):
        mock_get_job_name_to_latest_jobs.return_value = {
            "log-user": job_dict("oldJobId1", "log-user", "RUNNING", 1000),
        }
        wait_for_flink_job("ns", "log-user", None, False)
        self.assertFalse(mock_sleep.called)

    @patch('builtins.print')
    @patch('time.sleep')
    @patch('deploy.list_jobs_util.get_job_name_to_latest_jobs')
    def test_found_previous_one_wait(self, mock_get_job_name_to_latest_jobs, mock_sleep, mock_print):
        mock_get_job_name_to_latest_jobs.side_effect = [
            {
                "log-user": job_dict("oldJobId1", "log-user", "RUNNING", 1000),
            },
            {
                "log-user": job_dict("newJobId2", "log-user", "RUNNING", 1000),
            }
        ]
        wait_for_flink_job("ns", "log-user", "oldJobId1", False)
        self.assertEqual(mock_sleep.call_count, 1)
        mock_sleep.assert_has_calls([call(5)])

    @patch('builtins.print')
    @patch('time.sleep')
    @patch('deploy.list_jobs_util.get_job_name_to_latest_jobs')
    def test_found_previous_two_waits(self, mock_get_job_name_to_latest_jobs, mock_sleep, mock_print):
        mock_get_job_name_to_latest_jobs.side_effect = [
            {
                "log-user": job_dict("oldJobId1", "log-user", "RUNNING", 1000),
            },
            {
                "log-user": job_dict("oldJobId1", "log-user", "FINISHED", 1000),
            },
            {
                "log-user": job_dict("newJobId2", "log-user", "RUNNING", 1000),
            }
        ]
        wait_for_flink_job("ns", "log-user", "oldJobId1", False)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_has_calls([call(5), call(5)])

    @patch('builtins.print')
    @patch('time.sleep')
    @patch('deploy.list_jobs_util.get_job_name_to_latest_jobs')
    def test_found_different_canceled_job(self, mock_get_job_name_to_latest_jobs, mock_sleep, mock_print):
        """It's possible that there are multiple cancelled jobs.  We should ignore them."""
        mock_get_job_name_to_latest_jobs.side_effect = [
            {
                # The script finds a different finished job first.  We should ignore this case.
                "log-user": job_dict("differentOldJobId3", "log-user", "FINISHED", 1000),
            },
            {
                "log-user": job_dict("newJobId", "log-user", "RUNNING", 1000),
            }
        ]
        wait_for_flink_job("ns", "log-user", "oldJobId", False)
        self.assertEqual(mock_sleep.call_count, 1)
        mock_sleep.assert_has_calls([call(5)])

    @patch('builtins.print')
    @patch('deploy.list_jobs_util.get_job_name_to_latest_jobs')
    def test_dry_run(self, mock_get_job_name_to_latest_jobs, mock_print):
        wait_for_flink_job("ns", "log-user", None, True)
        mock_get_job_name_to_latest_jobs.assert_not_called()
