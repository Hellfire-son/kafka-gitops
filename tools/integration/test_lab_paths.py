# SPDX-License-Identifier: Apache-2.0
"""Offline path portability checks; no lab, Docker or Kafka operations."""
import os
import pathlib
import unittest
from unittest.mock import patch
from lab_paths import REPO, bind_source_paths, lab_directory, java_executable


class PortablePathsTest(unittest.TestCase):
    def test_profile_defaults_are_relative_to_checkout(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(lab_directory('323'), REPO.parent / 'kafka-lab-323')
            self.assertEqual(lab_directory('371'), REPO.parent / 'kafka-lab-371')
            self.assertEqual(lab_directory('44'), REPO.parent / 'kafka-lab')

    def test_profile_override_does_not_change_other_profile(self):
        with patch.dict(os.environ, {'KAFKA_GITOPS_LAB_323_DIR': '.'}, clear=True):
            self.assertEqual(lab_directory('323'), pathlib.Path('.').resolve())
            self.assertEqual(lab_directory('371'), REPO.parent / 'kafka-lab-371')
        with self.assertRaises(ValueError):
            lab_directory('unknown')

    def test_windows_mirror_is_exact(self):
        self.assertEqual(bind_source_paths('Q:/test-labs/legacy/config/broker.properties'), {
            'Q:/test-labs/legacy/config/broker.properties',
            '/run/desktop/mnt/host/q/test-labs/legacy/config/broker.properties'})

    def test_posix_path_does_not_admit_unrelated_windows_mirror(self):
        self.assertEqual(bind_source_paths('/test-labs/config/broker.properties'), {'/test-labs/config/broker.properties'})

    def test_java_uses_path_when_home_is_absent_and_rejects_broken_home(self):
        with patch.dict(os.environ, {}, clear=True), patch('lab_paths.shutil.which', return_value='java-on-path'):
            self.assertEqual(java_executable(), 'java-on-path')
        with patch.dict(os.environ, {'JAVA_HOME': 'missing-jdk'}, clear=True), patch('pathlib.Path.is_file', return_value=False):
            with self.assertRaises(RuntimeError):
                java_executable()


if __name__ == '__main__':
    unittest.main()
