"""Empty."""

"""Utilities for build commandline applications."""
import logging
import sys

import structlog

EXTERNAL_LOGGERS = {
    "evergreen",
    "git",
    "inject",
    "urllib3",
}


def enable_logging(verbose: bool) -> None:
    """
    Enable logging for execution.

    :param verbose: Should verbose logging be enabled.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="[%(asctime)s - %(name)s - %(levelname)s] %(message)s",
        level=level,
        stream=sys.stdout,
    )
    structlog.configure(logger_factory=structlog.stdlib.LoggerFactory())
    for log_name in EXTERNAL_LOGGERS:
        logging.getLogger(log_name).setLevel(logging.WARNING)

"""Utility to support file operations."""
import os
from typing import Dict, Any

import yaml


def create_empty(path):
    """Create an empty file specified by 'path'."""
    with open(path, "w") as file_handle:
        file_handle.write("")


def getmtime(path):
    """Return the modified time of 'path', or 0 if is does not exist."""
    if not os.path.isfile(path):
        return 0
    return os.path.getmtime(path)


def is_empty(path):
    """Return True if 'path' has a zero size."""
    return os.stat(path).st_size == 0


def get_file_handle(path, append_file=False):
    """Open 'path', truncate it if 'append_file' is False, and return file handle."""
    mode = "a+" if append_file else "w"
    return open(path, mode)


def write_file(path: str, contents: str) -> None:
    """
    Write the contents provided to the file in the specified path.

    :param path: Path of file to write.
    :param contents: Contents to write to file.
    """
    with open(path, "w") as file_handle:
        file_handle.write(contents)


def write_file_to_dir(directory: str, file: str, contents: str, overwrite: bool = True) -> None:
    """
    Write the contents provided to the file in the given directory.

    The directory will be created if it does not exist.

    :param directory: Directory to write to.
    :param file: Name of file to write.
    :param contents: Contents to write to file.
    :param overwrite: If True it is ok to overwrite an existing file.
    """
    target_file = os.path.join(directory, file)
    if not overwrite:
        if os.path.exists(target_file):
            raise FileExistsError(target_file)

    if not os.path.exists(directory):
        os.makedirs(directory)

    write_file(target_file, contents)


def read_yaml_file(path: str) -> Dict[str, Any]:
    """
    Read the yaml file at the given path and return the contents.

    :param path: Path to file to read.
    :return: Contents of given file.
    """
    with open(path) as file_handle:
        return yaml.safe_load(file_handle)

"""Functions to read configuration files."""
import yaml


def get_config_value(attrib, cmd_line_options, config_file_data, required=False, default=None):
    """
    Get the configuration value to use.

    First use command line options, then config file option, then the default. If required is
    true, throw an exception if the value is not found.

    :param attrib: Attribute to search for.
    :param cmd_line_options: Command line options.
    :param config_file_data: Config file data.
    :param required: Is this option required.
    :param default: Default value if option is not found.
    :return: value to use for this option.
    """
    value = getattr(cmd_line_options, attrib, None)
    if value is not None:
        return value

    if attrib in config_file_data:
        return config_file_data[attrib]

    if required:
        raise KeyError("{0} must be specified".format(attrib))

    return default


def read_config_file(config_file):
    """
    Read the yaml config file specified.

    :param config_file: path to config file.
    :return: Object representing contents of config file.
    """
    config_file_data = {}
    if config_file:
        with open(config_file) as file_handle:
            config_file_data = yaml.safe_load(file_handle)

    return config_file_data

"""Utility to support running a command in a subprocess."""

import os
import pipes
import shlex
import sys
import subprocess

from . import fileops


class RunCommand(object):
    """Class to abstract executing a subprocess."""

    def __init__(  # pylint: disable=too-many-arguments
            self, string=None, output_file=None, append_file=False, propagate_signals=True):
        """Initialize the RunCommand object."""
        self._command = string if string else ""
        self.output_file = output_file
        self.append_file = append_file
        self._process = None
        if propagate_signals or os.name != "posix":
            # The function os.setpgrp is not supported on Windows.
            self._preexec_kargs = {}
        elif subprocess.__name__ == "subprocess32":
            self._preexec_kargs = {"start_new_session": True}
        else:
            self._preexec_kargs = {"preexec_fn": os.setpgrp}

    def add(self, string):
        """Add a string to the command."""
        self._command = "{}{}{}".format(self._command, self._space(), string)

    def add_file(self, path):
        """Add a file path to the command."""
        # For Windows compatability, use pipes.quote around file paths.
        self._command = "{}{}{}".format(self._command, self._space(), pipes.quote(path))

    def _space(self):
        """Return a space if the command has been started to be built."""
        if self._command:
            return " "
        return ""

    def _cmd_list(self):
        """Return 'cmd' as a list of strings."""
        cmd = self._command
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        return cmd

    def execute(self):
        """Execute 'cmd' and return err_code and output."""
        self._process = subprocess.Popen(self._cmd_list(), stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT, **self._preexec_kargs)
        output, _ = self._process.communicate()
        error_code = self._process.returncode
        return error_code, output

    def execute_with_output(self):
        """Execute the command, return result as a string."""
        return subprocess.check_output(self._cmd_list()).decode('utf-8')

    def execute_save_output(self):
        """Execute the command, save result in 'self.output_file' and return returncode."""
        with fileops.get_file_handle(self.output_file, self.append_file) as file_handle:
            ret = subprocess.check_call(self._cmd_list(), stdout=file_handle)
        return ret

    def start_process(self):
        """Start to execute the command."""
        # Do not propagate interrupts to the child process.
        with fileops.get_file_handle(self.output_file, self.append_file) as file_handle:
            self._process = subprocess.Popen(self._cmd_list(), stdin=subprocess.PIPE,
                                             stdout=file_handle, stderr=subprocess.STDOUT,
                                             **self._preexec_kargs)

    def send_to_process(self, string=None):
        """Send 'string' to a running processs and return stdout, stderr."""
        return self._process.communicate(string)

    def wait_for_process(self):
        """Wait for a running processs to end and return stdout, stderr."""
        return self.send_to_process()

    def stop_process(self):
        """Stop the running process."""
        self._process.terminate()

    def kill_process(self):
        """Kill the running process."""
        self._process.kill()

    def is_process_running(self):
        """Return True if the process is still running."""
        return self._process.poll() is None

    @property
    def command(self):
        """Get the command."""
        return self._command

    @property
    def process(self):
        """Get the process object."""
        return self._process

"""Functions for working with resmoke task names."""

import math

GEN_SUFFIX = "_gen"


def name_generated_task(parent_name, task_index, total_tasks, variant=None):
    """
    Create a zero-padded sub-task name.

    :param parent_name: Name of the parent task.
    :param task_index: Index of this sub-task.
    :param total_tasks: Total number of sub-tasks being generated.
    :param variant: Build variant to run task in.
    :return: Zero-padded name of sub-task.
    """
    suffix = ""
    if variant:
        suffix = f"_{variant}"

    index_width = int(math.ceil(math.log10(total_tasks)))
    return f"{parent_name}_{str(task_index).zfill(index_width)}{suffix}"


def remove_gen_suffix(task_name: str) -> str:
    """
    Remove '_gen' suffix from task_name.

    :param task_name: Original task name.
    :return: Task name with '_gen' removed, if it exists.
    """
    if task_name.endswith(GEN_SUFFIX):
        return task_name[:-4]
    return task_name

"""Functions for working with resmoke test names."""

import os

HOOK_DELIMITER = ":"


def is_resmoke_hook(test_name):
    """Determine whether the given test name is for a resmoke hook."""
    return test_name.find(HOOK_DELIMITER) != -1


def split_test_hook_name(hook_name):
    """
    Split a hook name into the test name and the resmoke hook name.

    Note: This method uses ':' to separate the test name from the resmoke hook name. If the test
    name has a ':' in it (such as burn_in_test.py tests), it will not work correctly.
    """
    assert is_resmoke_hook(hook_name) is True

    hook_name_parts = hook_name.split(HOOK_DELIMITER)

    return hook_name_parts[0], hook_name_parts[1]


def get_short_name_from_test_file(test_file):
    """Determine the short name a test would use based on the given test_file."""

    return os.path.splitext(os.path.basename(test_file))[0]


def normalize_test_file(test_file):
    """
    Normalize the given test file.

    If 'test_file' represents a Windows-style path, then it is converted to a POSIX-style path
    with

    - backslashes (\\) as the path separator replaced with forward slashes (/) and
    - the ".exe" extension, if present, removed.

    If 'test_file' already represents a POSIX-style path, then it is returned unmodified.
    """

    if "\\" in test_file:
        posix_test_file = test_file.replace("\\", "/")
        (test_file_root, test_file_ext) = os.path.splitext(posix_test_file)
        if test_file_ext == ".exe":
            return test_file_root
        return posix_test_file

    return test_file


def denormalize_test_file(test_file):
    """Return a list containing 'test_file' as both a POSIX-style and a Windows-style path.

    The conversion process may involving replacing forward slashes (/) as the path separator
    with backslashes (\\), as well as adding a ".exe" extension if 'test_file' has no file
    extension.
    """

    test_file = normalize_test_file(test_file)

    if "/" in test_file:
        windows_test_file = test_file.replace("/", "\\")
        if not os.path.splitext(test_file)[1]:
            windows_test_file += ".exe"
        return [test_file, windows_test_file]

    return [test_file]

"""Utility to support parsing a TestStat."""
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from itertools import chain
from typing import NamedTuple, List, Callable, Optional

from evergreen import EvergreenApi, TestStats

from buildscripts.util.testname import split_test_hook_name, is_resmoke_hook, get_short_name_from_test_file

TASK_LEVEL_HOOKS = {"CleanEveryN"}


class TestRuntime(NamedTuple):
    """
    Container for the runtime of a test.

    test_name: Name of test.
    runtime: Average of runtime of test.
    """

    test_name: str
    runtime: float


@dataclass
class _RuntimeHistory:
    """
    History of runtime results.

    duration: Average duration of test runtime.
    num_runs: Number of test runs seen.
    """

    duration: float
    num_runs: int

    @classmethod
    def empty(cls) -> "_RuntimeHistory":
        """Create an empty runtime entry."""
        return cls(duration=0.0, num_runs=0)

    def add_runtimes(self, duration: float, num_runs: int) -> None:
        """
        Add the given duration number this history.

        :param duration: Average duration to include.
        :param num_runs: Number of runs to include.
        """
        self.duration = _average(self.duration, self.num_runs, duration, num_runs)
        self.num_runs += num_runs


def normalize_test_name(test_name: str) -> str:
    """Normalize test names that may have been run on windows or unix."""
    return test_name.replace("\\", "/")


def _average(value_a: float, num_a: int, value_b: float, num_b: int) -> float:
    """Compute a weighted average of 2 values with associated numbers."""
    divisor = num_a + num_b
    if divisor == 0:
        return 0
    else:
        return float(value_a * num_a + value_b * num_b) / divisor


class HistoricHookInfo(NamedTuple):
    """Historic information about a test hook."""

    hook_id: str
    num_pass: int
    avg_duration: float

    @classmethod
    def from_test_stats(cls, test_stats: TestStats) -> "HistoricHookInfo":
        """Create an instance from a test_stats object."""
        return cls(hook_id=test_stats.test_file, num_pass=test_stats.num_pass,
                   avg_duration=test_stats.avg_duration_pass)

    def test_name(self) -> str:
        """Get the name of the test associated with this hook."""
        return split_test_hook_name(self.hook_id)[0]

    def hook_name(self) -> str:
        """Get the name of this hook."""
        return split_test_hook_name(self.hook_id)[-1]

    def is_task_level_hook(self) -> bool:
        """Determine if this hook should be counted against the task not the test."""
        return self.hook_name() in TASK_LEVEL_HOOKS


class HistoricTestInfo(NamedTuple):
    """Historic information about a test."""

    test_name: str
    num_pass: int
    avg_duration: float
    hooks: List[HistoricHookInfo]

    @classmethod
    def from_test_stats(cls, test_stats: TestStats,
                        hooks: List[HistoricHookInfo]) -> "HistoricTestInfo":
        """Create an instance from a test_stats object."""
        return cls(test_name=test_stats.test_file, num_pass=test_stats.num_pass,
                   avg_duration=test_stats.avg_duration_pass, hooks=hooks)

    def normalized_test_name(self) -> str:
        """Get the normalized version of the test name."""
        return normalize_test_name(self.test_name)

    def total_hook_runtime(self,
                           predicate: Optional[Callable[[HistoricHookInfo], bool]] = None) -> float:
        """Get the average runtime of all the hooks associated with this test."""
        if not predicate:
            predicate = lambda _: True
        return sum([hook.avg_duration for hook in self.hooks if predicate(hook)])

    def total_test_runtime(self) -> float:
        """Get the average runtime of this test and it's non-task level hooks."""
        return self.avg_duration + self.total_hook_runtime(lambda h: not h.is_task_level_hook())

    def get_hook_overhead(self) -> float:
        """Get the average runtime of this test and it's non-task level hooks."""
        return self.total_hook_runtime(lambda h: h.is_task_level_hook())


class HistoricTaskData(object):
    """Represent the test statistics for the task that is being analyzed."""

    def __init__(self, historic_test_results: List[HistoricTestInfo]) -> None:
        """Initialize the TestStats with raw results from the Evergreen API."""
        self.historic_test_results = historic_test_results

    # pylint: disable=too-many-arguments
    @classmethod
    def from_evg(cls, evg_api: EvergreenApi, project: str, start_date: datetime, end_date: datetime,
                 task: str, variant: str) -> "HistoricTaskData":
        """
        Retrieve test stats from evergreen for a given task.

        :param evg_api: Evergreen API client.
        :param project: Project to query.
        :param start_date: Start date to query.
        :param end_date: End date to query.
        :param task: Task to query.
        :param variant: Build variant to query.
        :return: Test stats for the specified task.
        """
        days = (end_date - start_date).days
        historic_stats = evg_api.test_stats_by_project(
            project, after_date=start_date, before_date=end_date, tasks=[task], variants=[variant],
            group_by="test", group_num_days=days)

        return cls.from_stats_list(historic_stats)

    @classmethod
    def from_stats_list(cls, historic_stats: List[TestStats]) -> "HistoricTaskData":
        """
        Build historic task data from a list of historic stats.

        :param historic_stats: List of historic stats to build from.
        :return: Historic task data from the list of stats.
        """

        hooks = defaultdict(list)
        for hook in [stat for stat in historic_stats if is_resmoke_hook(stat.test_file)]:
            historical_hook = HistoricHookInfo.from_test_stats(hook)
            hooks[historical_hook.test_name()].append(historical_hook)

        return cls([
            HistoricTestInfo.from_test_stats(stat,
                                             hooks[get_short_name_from_test_file(stat.test_file)])
            for stat in historic_stats if not is_resmoke_hook(stat.test_file)
        ])

    def get_tests_runtimes(self) -> List[TestRuntime]:
        """Return the list of (test_file, runtime_in_secs) tuples ordered by decreasing runtime."""
        tests = [
            TestRuntime(test_name=test_stats.normalized_test_name(),
                        runtime=test_stats.total_test_runtime())
            for test_stats in self.historic_test_results
        ]
        return sorted(tests, key=lambda x: x.runtime, reverse=True)

    def get_avg_hook_runtime(self, hook_name: str) -> float:
        """Get the average runtime for the specified hook."""
        hook_instances = list(
            chain.from_iterable([[hook for hook in test.hooks if hook.hook_name() == hook_name]
                                 for test in self.historic_test_results]))

        if not hook_instances:
            return 0
        return sum([hook.avg_duration for hook in hook_instances]) / len(hook_instances)

    def __len__(self) -> int:
        """Get the number of historical entries."""
        return len(self.historic_test_results)

"""Utilities for working with time."""


def ns2sec(ns):
    """Convert ns to seconds."""
    return ns / (10**9)
