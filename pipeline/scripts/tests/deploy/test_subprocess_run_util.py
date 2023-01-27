import unittest
from unittest.mock import call, patch
from deploy.subprocess_run_util import subprocess_run


class MockRunResult:
    def __init__(self, returncode, stdout):
        self.returncode = returncode
        self.stdout = stdout

    def __repr__(self):
        return "MockRunResult({0},{1})".format(self.returncode, self.stdout)

    def __str__(self):
        return "{0},{1}".format(self.returncode, self.stdout)


class NoRetrySubprocessRunTest(unittest.TestCase):
    @patch('subprocess.run')
    def test_success(self, mock_subprocess_run):
        mock_result = MockRunResult(0, "success")
        mock_subprocess_run.return_value = mock_result

        command = "kubectl apply -f ~/config/path"
        self.assertEqual(mock_result, subprocess_run(command=command))
        self.assertEqual(mock_subprocess_run.call_count, 1)
        mock_subprocess_run.assert_has_calls([
            call(command, capture_output=True, shell=True)
        ])

    @patch('subprocess.run')
    def test_failure(self, mock_subprocess_run):
        mock_result = MockRunResult(0, "success")
        mock_subprocess_run.return_value = mock_result

        command = "kubectl apply -f ~/config/path"
        self.assertEqual(mock_result, subprocess_run(command=command))
        self.assertEqual(mock_subprocess_run.call_count, 1)
        mock_subprocess_run.assert_has_calls([
            call(command, capture_output=True, shell=True)
        ])


class RetrySubprocessRunTest(unittest.TestCase):
    @patch('subprocess.run')
    def test_success(self, mock_subprocess_run):
        mock_result = MockRunResult(0, "success")
        mock_subprocess_run.return_value = mock_result

        command = "kubectl apply -f ~/config/path"
        self.assertEqual(mock_result, subprocess_run(command=command, retry=True))
        self.assertEqual(mock_subprocess_run.call_count, 1)
        mock_subprocess_run.assert_has_calls([
            call(command, capture_output=True, shell=True)
        ])

    @patch('time.sleep')
    @patch('subprocess.run')
    def test_success_retry_failure(self, mock_subprocess_run, mock_sleep):
        failed_result = MockRunResult(1, "failed")
        success_result = MockRunResult(0, "success")
        mock_subprocess_run.side_effect = [
            failed_result,
            success_result,
        ]
        command = "kubectl apply -f ~/config/path"
        self.assertEqual(success_result, subprocess_run(command=command, retry=True))
        self.assertEqual(mock_subprocess_run.call_count, 2)
        mock_subprocess_run.assert_has_calls([
            call(command, capture_output=True, shell=True),
            call(command, capture_output=True, shell=True)
        ])

    @patch('time.sleep')
    @patch('subprocess.run')
    def test_failure(self, mock_subprocess_run, mock_sleep):
        failed_result = MockRunResult(1, "failed")
        mock_subprocess_run.side_effect = [
            failed_result,
            failed_result,
            failed_result,
        ]
        command = "kubectl apply -f ~/config/path"
        self.assertEqual(failed_result, subprocess_run(command=command, retry=True))
        self.assertEqual(mock_subprocess_run.call_count, 3)
        mock_subprocess_run.assert_has_calls([
            call(command, capture_output=True, shell=True),
            call(command, capture_output=True, shell=True),
            call(command, capture_output=True, shell=True)
        ])
