# SPDX-License-Identifier: Apache-2.0
"""Portable lab/JDK paths. Path selection never replaces the runner identity gates."""
import os
import pathlib
import shutil

REPO = pathlib.Path(__file__).resolve().parents[2]


def lab_directory(profile):
    names = {'44': 'kafka-lab', '371': 'kafka-lab-371', '323': 'kafka-lab-323'}
    if profile not in names:
        raise ValueError('Unknown lab profile')
    configured = os.environ.get('KAFKA_GITOPS_LAB_' + profile + '_DIR')
    return pathlib.Path(configured).expanduser().resolve() if configured else REPO.parent / names[profile]


def java_executable():
    home = os.environ.get('JAVA_HOME')
    if home:
        executable = pathlib.Path(home) / 'bin' / ('java.exe' if os.name == 'nt' else 'java')
        if executable.is_file():
            return str(executable)
        raise RuntimeError('JAVA_HOME does not contain the Java executable')
    executable = shutil.which('java')
    if not executable:
        raise RuntimeError('Java 21 is required: set JAVA_HOME or PATH')
    return executable


def bind_source_paths(resolved_host_path):
    """Accept only the selected exact host path and its Docker Desktop mirror."""
    sources = {resolved_host_path}
    windows = pathlib.PureWindowsPath(resolved_host_path)
    if len(windows.drive) == 2 and windows.drive[1] == ':':
        sources.add('/run/desktop/mnt/host/' + windows.drive[0].lower() + '/' + windows.as_posix()[3:])
    return sources
