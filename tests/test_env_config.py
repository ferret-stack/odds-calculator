"""
Tests for .env loading and odds-api.com key failover.

Covers the two things this replaces:
  1. Three API keys hardcoded directly in odds_calculator.py, public in this
     repo's git history since Day 1 -- keys now come from the environment
     (real env or a gitignored .env), never from source.
  2. No failover at all: only API_KEYS[0] was ever used, so a single key
     running out of quota mid-matchweek stopped the pipeline outright, even
     though a second key was sitting unused in the same list.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from env_config import (
    load_dotenv, odds_api_keys, quota_exhausted, request_with_key_failover,
)


class TestLoadDotenv(unittest.TestCase):
    def _write(self, text):
        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.env', delete=False)
        tmp.write(text)
        tmp.close()
        self.addCleanup(os.unlink, tmp.name)
        return Path(tmp.name)

    def test_missing_file_is_not_an_error(self):
        load_dotenv(Path('/nonexistent/path/.env'))  # must not raise

    def test_simple_key_value(self):
        path = self._write('ODDS_API_KEY=abc123\n')
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('ODDS_API_KEY', None)
            load_dotenv(path)
            self.assertEqual(os.environ['ODDS_API_KEY'], 'abc123')

    def test_quoted_values_are_unquoted(self):
        path = self._write('ODDS_API_KEY="abc 123"\nODDS_API_KEY_BACKUP=\'xyz\'\n')
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('ODDS_API_KEY', None)
            os.environ.pop('ODDS_API_KEY_BACKUP', None)
            load_dotenv(path)
            self.assertEqual(os.environ['ODDS_API_KEY'], 'abc 123')
            self.assertEqual(os.environ['ODDS_API_KEY_BACKUP'], 'xyz')

    def test_comments_and_blank_lines_are_ignored(self):
        path = self._write(
            '# a comment\n\n   \nODDS_API_KEY=real\n# ODDS_API_KEY=commented-out\n')
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('ODDS_API_KEY', None)
            load_dotenv(path)
            self.assertEqual(os.environ['ODDS_API_KEY'], 'real')

    def test_a_real_env_var_is_never_overridden_by_the_file(self):
        """
        A shell export or CI secret must always win over a stray .env file --
        the file is a local convenience, not an authority.
        """
        path = self._write('ODDS_API_KEY=from-file\n')
        with patch.dict(os.environ, {'ODDS_API_KEY': 'from-shell'}):
            load_dotenv(path)
            self.assertEqual(os.environ['ODDS_API_KEY'], 'from-shell')

    def test_line_with_no_equals_sign_is_skipped_not_fatal(self):
        path = self._write('this is not a valid line\nODDS_API_KEY=fine\n')
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('ODDS_API_KEY', None)
            load_dotenv(path)
            self.assertEqual(os.environ['ODDS_API_KEY'], 'fine')


class TestOddsApiKeys(unittest.TestCase):
    def test_empty_when_nothing_configured(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('ODDS_API_KEY', None)
            os.environ.pop('ODDS_API_KEY_BACKUP', None)
            self.assertEqual(odds_api_keys(), [])

    def test_primary_only(self):
        with patch.dict(os.environ, {'ODDS_API_KEY': 'p'}, clear=False):
            os.environ.pop('ODDS_API_KEY_BACKUP', None)
            self.assertEqual(odds_api_keys(), ['p'])

    def test_primary_then_backup_in_order(self):
        with patch.dict(os.environ,
                        {'ODDS_API_KEY': 'p', 'ODDS_API_KEY_BACKUP': 'b'}):
            self.assertEqual(odds_api_keys(), ['p', 'b'])

    def test_blank_string_counts_as_unset(self):
        """A .env with `ODDS_API_KEY_BACKUP=` (empty) must not add a blank key."""
        with patch.dict(os.environ,
                        {'ODDS_API_KEY': 'p', 'ODDS_API_KEY_BACKUP': ''}):
            self.assertEqual(odds_api_keys(), ['p'])


class TestQuotaExhausted(unittest.TestCase):
    def test_429_is_always_exhausted(self):
        self.assertTrue(quota_exhausted(429, None))
        self.assertTrue(quota_exhausted(429, {'anything': 'here'}))

    def test_401_with_quota_error_code_is_exhausted(self):
        self.assertTrue(quota_exhausted(401, {'error_code': 'OUT_OF_USAGE_CREDITS'}))

    def test_401_error_code_check_is_case_insensitive(self):
        self.assertTrue(quota_exhausted(401, {'error_code': 'out_of_usage_credits'}))

    def test_401_with_no_body_is_not_treated_as_quota(self):
        """A body-less 401 gives no evidence it's quota, not a bad key."""
        self.assertFalse(quota_exhausted(401, None))

    def test_401_for_a_different_reason_is_not_quota(self):
        """
        A genuinely bad/revoked key also returns 401. Treating that as
        quota-exhausted would silently fail over to a second key that then
        fails identically -- worse than just reporting the real 401.
        """
        self.assertFalse(quota_exhausted(401, {'error_code': 'INVALID_KEY'}))

    def test_200_is_never_exhausted(self):
        self.assertFalse(quota_exhausted(200, {'anything': 'here'}))

    def test_500_is_not_treated_as_a_key_problem(self):
        self.assertFalse(quota_exhausted(500, None))


class _FakeResponse:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body

    def json(self):
        if self._body is None:
            raise ValueError('no body')
        return self._body


class TestRequestWithKeyFailover(unittest.TestCase):
    def test_no_keys_configured_returns_none_without_calling_get(self):
        calls = []
        response, index = request_with_key_failover(
            lambda url, params: calls.append(params) or _FakeResponse(200),
            'http://x', {}, keys=[])
        self.assertIsNone(response)
        self.assertEqual(index, 0)
        self.assertEqual(calls, [])

    def test_single_working_key_succeeds_on_first_try(self):
        calls = []

        def get(url, params):
            calls.append(params['apiKey'])
            return _FakeResponse(200)

        response, index = request_with_key_failover(get, 'http://x', {}, ['k1'])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(index, 0)
        self.assertEqual(calls, ['k1'])

    def test_exhausted_primary_falls_over_to_backup(self):
        calls = []

        def get(url, params):
            key = params['apiKey']
            calls.append(key)
            if key == 'primary':
                return _FakeResponse(401, {'error_code': 'OUT_OF_USAGE_CREDITS'})
            return _FakeResponse(200)

        response, index = request_with_key_failover(
            get, 'http://x', {}, ['primary', 'backup'])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(index, 1, 'must report the backup as the key that worked')
        self.assertEqual(calls, ['primary', 'backup'])

    def test_every_key_exhausted_returns_the_last_failure(self):
        def get(url, params):
            return _FakeResponse(429)

        response, index = request_with_key_failover(
            get, 'http://x', {}, ['primary', 'backup'])
        self.assertEqual(response.status_code, 429)
        self.assertEqual(index, 1)

    def test_a_non_quota_failure_is_not_retried_on_the_backup(self):
        """
        A network/param/site error would fail identically on the second key,
        so retrying it would waste a request and misreport which key failed.
        """
        calls = []

        def get(url, params):
            calls.append(params['apiKey'])
            return _FakeResponse(500)

        response, index = request_with_key_failover(
            get, 'http://x', {}, ['primary', 'backup'])
        self.assertEqual(response.status_code, 500)
        self.assertEqual(index, 0)
        self.assertEqual(calls, ['primary'], 'backup must not have been tried')

    def test_start_at_skips_a_key_already_known_dead(self):
        """
        The sticky-index contract: once a caller has learned key 0 is dead
        this run, later calls in the same loop must not spend a request
        re-confirming that.
        """
        calls = []

        def get(url, params):
            calls.append(params['apiKey'])
            return _FakeResponse(200)

        response, index = request_with_key_failover(
            get, 'http://x', {}, ['primary', 'backup'], start_at=1)
        self.assertEqual(index, 1)
        self.assertEqual(calls, ['backup'], 'must not have retried the dead primary')

    def test_apikey_param_does_not_leak_into_the_base_params(self):
        """The dict passed in as `params` must not be mutated with apiKey."""
        base_params = {'regions': 'uk'}

        def get(url, params):
            return _FakeResponse(200)

        request_with_key_failover(get, 'http://x', base_params, ['k1'])
        self.assertNotIn('apiKey', base_params)


if __name__ == '__main__':
    unittest.main(verbosity=2)
