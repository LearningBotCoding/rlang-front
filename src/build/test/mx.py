"""Unit tests for the selected_tests script."""
import json
import sys
import unittest
from datetime import datetime, timedelta
from typing import Dict, Any

import inject
from mock import MagicMock, patch
from evergreen import EvergreenApi

# pylint: disable=wrong-import-position
import buildscripts.ciconfig.evergreen as _evergreen
from buildscripts.burn_in_tests import TaskInfo
from buildscripts.patch_builds.selected_tests.selected_tests_client import SelectedTestsClient, \
    TestMappingsResponse, TestMapping, TestFileInstance, TaskMappingsResponse, TaskMapInstance, \
    TaskMapping
from buildscripts.selected_tests import EvgExpansions
from buildscripts.task_generation.gen_config import GenerationConfiguration
from buildscripts.task_generation.resmoke_proxy import ResmokeProxyConfig
from buildscripts.task_generation.suite_split import SuiteSplitConfig
from buildscripts.task_generation.suite_split_strategies import SplitStrategy, greedy_division, \
    FallbackStrategy, round_robin_fallback
from buildscripts.task_generation.task_types.gentask_options import GenTaskOptions
from buildscripts.tests.test_burn_in_tests import get_evergreen_config, mock_changed_git_files
from buildscripts import selected_tests as under_test

# pylint: disable=missing-docstring,invalid-name,unused-argument,protected-access,no-value-for-parameter

NS = "buildscripts.selected_tests"


def ns(relative_name):  # pylint: disable=invalid-name
    """Return a full name from a name relative to the test module"s name space."""
    return NS + "." + relative_name


def empty_build_variant(variant_name: str) -> Dict[str, Any]:
    return {
        "buildvariants": [{
            "name": variant_name,
            "tasks": [],
        }],
        "tasks": [],
    }


def configure_dependencies(evg_api, evg_expansions, evg_project_config, selected_test_client,
                           test_suites_dir=under_test.DEFAULT_TEST_SUITE_DIR):
    start_date = datetime.utcnow()
    end_date = start_date - timedelta(weeks=2)

    def dependencies(binder: inject.Binder) -> None:
        binder.bind(EvgExpansions, evg_expansions)
        binder.bind(_evergreen.EvergreenProjectConfig, evg_project_config)
        binder.bind(SuiteSplitConfig, evg_expansions.build_suite_split_config(start_date, end_date))
        binder.bind(SplitStrategy, greedy_division)
        binder.bind(FallbackStrategy, round_robin_fallback)
        binder.bind(GenTaskOptions, evg_expansions.build_gen_task_options())
        binder.bind(EvergreenApi, evg_api)
        binder.bind(GenerationConfiguration, GenerationConfiguration.from_yaml_file())
        binder.bind(ResmokeProxyConfig, ResmokeProxyConfig(resmoke_suite_dir=test_suites_dir))
        binder.bind(SelectedTestsClient, selected_test_client)

    inject.clear_and_configure(dependencies)


class TestAcceptance(unittest.TestCase):
    """A suite of Acceptance tests for selected_tests."""

    @staticmethod
    def _mock_evg_api():
        evg_api_mock = MagicMock()
        task_mock = evg_api_mock.task_by_id.return_value
        task_mock.execution = 0
        return evg_api_mock

    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    def test_when_no_mappings_are_found_for_changed_files(self):
        mock_evg_api = self._mock_evg_api()
        mock_evg_config = get_evergreen_config("etc/evergreen.yml")
        mock_evg_expansions = under_test.EvgExpansions(
            task_id="task_id",
            task_name="selected_tests_gen",
            build_variant="selected-tests",
            build_id="my_build_id",
            project="mongodb-mongo-master",
            revision="abc123",
            version_id="my_version",
        )
        mock_selected_tests_client = MagicMock()
        mock_selected_tests_client.get_test_mappings.return_value = TestMappingsResponse(
            test_mappings=[])
        configure_dependencies(mock_evg_api, mock_evg_expansions, mock_evg_config,
                               mock_selected_tests_client)
        repos = [mock_changed_git_files([])]

        selected_tests = under_test.SelectedTestsOrchestrator()
        changed_files = selected_tests.find_changed_files(repos, "task_id")
        generated_config = selected_tests.generate_version(changed_files)

        # assert that config_dict does not contain keys for any generated task configs
        self.assertEqual(len(generated_config.file_list), 1)
        self.assertEqual(generated_config.file_list[0].file_name, "selected_tests_config.json")

    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    def test_when_test_mappings_are_found_for_changed_files(self):
        mock_evg_api = self._mock_evg_api()
        mock_evg_config = get_evergreen_config("etc/evergreen.yml")
        mock_evg_expansions = under_test.EvgExpansions(
            task_id="task_id",
            task_name="selected_tests_gen",
            build_variant="selected-tests",
            build_id="my_build_id",
            project="mongodb-mongo-master",
            revision="abc123",
            version_id="my_version",
        )
        mock_test_mapping = TestMapping(
            branch="master", project="mongodb-mongo-master", repo="mongodb/mongo",
            source_file="src/file1.cpp", source_file_seen_count=8,
            test_files=[TestFileInstance(name="jstests/auth/auth1.js", test_file_seen_count=3)])
        mock_selected_tests_client = MagicMock()
        mock_selected_tests_client.get_test_mappings.return_value = TestMappingsResponse(
            test_mappings=[mock_test_mapping])
        configure_dependencies(mock_evg_api, mock_evg_expansions, mock_evg_config,
                               mock_selected_tests_client)
        repos = [mock_changed_git_files(["src/file1.cpp"])]

        selected_tests = under_test.SelectedTestsOrchestrator()
        changed_files = selected_tests.find_changed_files(repos, "task_id")
        generated_config = selected_tests.generate_version(changed_files)

        files_to_generate = {gen_file.file_name for gen_file in generated_config.file_list}
        self.assertIn("selected_tests_config.json", files_to_generate)

        # assert that generated suite files have the suite name and the variant name in the
        # filename, to prevent tasks on different variants from using the same suite file
        self.assertIn("auth_0_enterprise-rhel-80-64-bit-dynamic-required.yml", files_to_generate)

        generated_evg_config_raw = [
            gen_file.content for gen_file in generated_config.file_list
            if gen_file.file_name == "selected_tests_config.json"
        ][0]

        generated_evg_config = json.loads(generated_evg_config_raw)
        build_variants_with_generated_tasks = generated_evg_config["buildvariants"]
        # jstests/auth/auth1.js belongs to two suites, auth and auth_audit,
        rhel_80_with_generated_tasks = next(
            (variant for variant in build_variants_with_generated_tasks
             if variant["name"] == "enterprise-rhel-80-64-bit-dynamic-required"), None)
        self.assertEqual(len(rhel_80_with_generated_tasks["tasks"]), 2)

    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    def test_when_task_mappings_are_found_for_changed_files(self):
        mock_evg_api = self._mock_evg_api()
        mock_evg_config = get_evergreen_config("etc/evergreen.yml")
        mock_evg_expansions = under_test.EvgExpansions(
            task_id="task_id",
            task_name="selected_tests_gen",
            build_variant="selected-tests",
            build_id="my_build_id",
            project="mongodb-mongo-master",
            revision="abc123",
            version_id="my_version",
        )
        mock_task_mapping = TaskMapping(
            branch="master", project="mongodb-mongo-master", repo="mongodb/mongo",
            source_file="src/file1.cpp", source_file_seen_count=8,
            tasks=[TaskMapInstance(name="auth", variant="enterprise-rhel-80", flip_count=5)])
        mock_selected_tests_client = MagicMock()
        mock_selected_tests_client.get_task_mappings.return_value = TaskMappingsResponse(
            task_mappings=[mock_task_mapping])
        configure_dependencies(mock_evg_api, mock_evg_expansions, mock_evg_config,
                               mock_selected_tests_client)
        repos = [mock_changed_git_files(["src/file1.cpp"])]

        selected_tests = under_test.SelectedTestsOrchestrator()
        changed_files = selected_tests.find_changed_files(repos, "task_id")
        generated_config = selected_tests.generate_version(changed_files)

        files_to_generate = {gen_file.file_name for gen_file in generated_config.file_list}
        self.assertIn("selected_tests_config.json", files_to_generate)

        generated_evg_config_raw = [
            gen_file.content for gen_file in generated_config.file_list
            if gen_file.file_name == "selected_tests_config.json"
        ][0]
        generated_evg_config = json.loads(generated_evg_config_raw)
        # the auth task's generator task, max_sub_suites is 3,
        # resulting in 3 subtasks being generated, plus a _misc task, hence 4
        # tasks total
        build_variants_with_generated_tasks = generated_evg_config["buildvariants"]
        rhel_80_with_generated_tasks = next(
            (variant for variant in build_variants_with_generated_tasks
             if variant["name"] == "enterprise-rhel-80-64-bit-dynamic-required"), None)
        self.assertEqual(len(rhel_80_with_generated_tasks["tasks"]), 5)


class TestExcludeTask(unittest.TestCase):
    def test_task_should_not_be_excluded(self):
        task = _evergreen.Task({"name": "regular_task"})

        self.assertEqual(under_test._exclude_task(task), False)

    def test_task_should_be_excluded(self):
        excluded_task = under_test.EXCLUDE_TASK_LIST[0]
        task = _evergreen.Task({"name": excluded_task})

        self.assertEqual(under_test._exclude_task(task), True)

    def test_task_matches_excluded_pattern(self):
        task_that_matches_exclude_pattern = "compile_all"
        task = _evergreen.Task({"name": task_that_matches_exclude_pattern})

        self.assertEqual(under_test._exclude_task(task), True)


def build_mock_evg_task(name, cmd_func="generate resmoke tasks",
                        resmoke_args="--storageEngine=wiredTiger"):
    return _evergreen.Task({
        "name": name,
        "commands": [{
            "func": cmd_func,
            "vars": {"resmoke_args": resmoke_args, },
        }],
    })


class TestGetEvgTaskConfig(unittest.TestCase):
    def test_task_is_a_generate_resmoke_task(self):
        build_variant_conf = MagicMock()
        build_variant_conf.name = "variant"
        task = build_mock_evg_task("auth_gen")

        task_config_service = under_test.TaskConfigService()
        evg_task_config = task_config_service.get_evg_task_config(task, build_variant_conf)

        self.assertEqual(evg_task_config["task_name"], "auth")
        self.assertEqual(evg_task_config["build_variant"], "variant")
        self.assertIsNone(evg_task_config.get("suite"))
        self.assertEqual(
            evg_task_config["resmoke_args"],
            "--storageEngine=wiredTiger",
        )

    def test_task_is_not_a_generate_resmoke_task(self):
        build_variant_conf = MagicMock()
        build_variant_conf.name = "variant"
        task = build_mock_evg_task("jsCore_auth", "run tests",
                                   "--suites=core_auth --storageEngine=wiredTiger")

        task_config_service = under_test.TaskConfigService()
        evg_task_config = task_config_service.get_evg_task_config(task, build_variant_conf)

        self.assertEqual(evg_task_config["task_name"], "jsCore_auth")
        self.assertEqual(evg_task_config["build_variant"], "variant")
        self.assertEqual(evg_task_config["suite"], "core_auth")
        self.assertEqual(
            evg_task_config["resmoke_args"],
            "--storageEngine=wiredTiger",
        )


class TestGetTaskConfigsForTestMappings(unittest.TestCase):
    @patch(ns("_exclude_task"))
    @patch(ns("_find_task"))
    def test_get_config_for_test_mapping(self, find_task_mock, exclude_task_mock):
        find_task_mock.side_effect = [
            build_mock_evg_task("jsCore_auth", "run tests"),
            build_mock_evg_task("auth_gen", "run tests",
                                "--suites=core_auth --storageEngine=wiredTiger"),
        ]
        exclude_task_mock.return_value = False
        tests_by_task = {
            "jsCore_auth":
                TaskInfo(
                    display_task_name="task 1",
                    tests=[
                        "jstests/core/currentop_waiting_for_latch.js",
                        "jstests/core/latch_analyzer.js",
                    ],
                    resmoke_args="",
                    require_multiversion=None,
                    distro="",
                ),
            "auth_gen":
                TaskInfo(
                    display_task_name="task 2",
                    tests=["jstests/auth/auth3.js"],
                    resmoke_args="",
                    require_multiversion=None,
                    distro="",
                ),
        }

        task_config_service = under_test.TaskConfigService()
        task_configs = task_config_service.get_task_configs_for_test_mappings(
            tests_by_task, MagicMock())

        self.assertEqual(task_configs["jsCore_auth"]["resmoke_args"], "--storageEngine=wiredTiger")
        self.assertEqual(
            task_configs["jsCore_auth"]["selected_tests_to_run"],
            {"jstests/core/currentop_waiting_for_latch.js", "jstests/core/latch_analyzer.js"})
        self.assertEqual(task_configs["auth_gen"]["suite"], "core_auth")
        self.assertEqual(task_configs["auth_gen"]["selected_tests_to_run"],
                         {'jstests/auth/auth3.js'})

    @patch(ns("_exclude_task"))
    @patch(ns("_find_task"))
    def test_get_config_for_test_mapping_when_task_should_be_excluded(self, find_task_mock,
                                                                      exclude_task_mock):
        find_task_mock.return_value = build_mock_evg_task(
            "jsCore_auth", "run tests", "--suites=core_auth --storageEngine=wiredTiger")
        exclude_task_mock.return_value = True
        tests_by_task = {
            "jsCore_auth":
                TaskInfo(
                    display_task_name="task 1",
                    tests=[
                        "jstests/core/currentop_waiting_for_latch.js",
                        "jstests/core/latch_analyzer.js",
                    ],
                    resmoke_args="",
                    require_multiversion=None,
                    distro="",
                ),
        }

        task_config_service = under_test.TaskConfigService()
        task_configs = task_config_service.get_task_configs_for_test_mappings(
            tests_by_task, MagicMock())

        self.assertEqual(task_configs, {})

    @patch(ns("_find_task"))
    def test_get_config_for_test_mapping_when_task_does_not_exist(self, find_task_mock):
        find_task_mock.return_value = None
        tests_by_task = {
            "jsCore_auth":
                TaskInfo(
                    display_task_name="task 1",
                    tests=[
                        "jstests/core/currentop_waiting_for_latch.js",
                        "jstests/core/latch_analyzer.js",
                    ],
                    resmoke_args="",
                    require_multiversion=None,
                    distro="",
                ),
        }

        task_config_service = under_test.TaskConfigService()
        task_configs = task_config_service.get_task_configs_for_test_mappings(
            tests_by_task, MagicMock())

        self.assertEqual(task_configs, {})


class TestGetTaskConfigsForTaskMappings(unittest.TestCase):
    @patch(ns("_exclude_task"))
    @patch(ns("_find_task"))
    def test_get_config_for_task_mapping(self, find_task_mock, exclude_task_mock):
        find_task_mock.side_effect = [build_mock_evg_task("task_1"), build_mock_evg_task("task_2")]
        exclude_task_mock.return_value = False
        tasks = ["task_1", "task_2"]

        task_config_service = under_test.TaskConfigService()
        task_configs = task_config_service.get_task_configs_for_task_mappings(tasks, MagicMock())

        self.assertEqual(task_configs["task_1"]["resmoke_args"], "--storageEngine=wiredTiger")
        self.assertEqual(task_configs["task_2"]["resmoke_args"], "--storageEngine=wiredTiger")

    @patch(ns("_exclude_task"))
    @patch(ns("_find_task"))
    def test_get_config_for_task_mapping_when_task_should_be_excluded(self, find_task_mock,
                                                                      exclude_task_mock):
        find_task_mock.return_value = build_mock_evg_task("task_1")
        exclude_task_mock.return_value = True
        tasks = ["task_1"]

        task_config_service = under_test.TaskConfigService()
        task_configs = task_config_service.get_task_configs_for_task_mappings(tasks, MagicMock())

        self.assertEqual(task_configs, {})

    @patch(ns("_find_task"))
    def test_get_config_for_task_mapping_when_task_does_not_exist(self, find_task_mock):
        find_task_mock.return_value = None
        tasks = ["task_1"]

        task_config_service = under_test.TaskConfigService()
        task_configs = task_config_service.get_task_configs_for_task_mappings(tasks, MagicMock())

        self.assertEqual(task_configs, {})


class TestRemoveRepoPathPrefix(unittest.TestCase):
    def test_file_is_in_enterprise_modules(self):
        filepath = under_test._remove_repo_path_prefix(
            "src/mongo/db/modules/enterprise/src/file1.cpp")

        self.assertEqual(filepath, "src/file1.cpp")

    def test_file_is_not_in_enterprise_modules(self):
        filepath = under_test._remove_repo_path_prefix("other_directory/src/file1.cpp")

        self.assertEqual(filepath, "other_directory/src/file1.cpp")

"""Unit tests for todo_check.py."""

from __future__ import absolute_import

import os
import textwrap
import unittest
from tempfile import TemporaryDirectory

from typing import Iterable

import buildscripts.todo_check as under_test

# pylint: disable=missing-docstring,invalid-name,unused-argument,no-self-use,protected-access


def create_file_iterator(file_contents: str) -> Iterable[str]:
    return textwrap.dedent(file_contents.strip()).splitlines()


class TestTodo(unittest.TestCase):
    def test_line_without_a_jira_issues(self):
        content = "a line with some random text"
        todo = under_test.Todo.from_line("file", 42, f"\n{content}\n\t\n")

        self.assertEqual(todo.file_name, "file")
        self.assertEqual(todo.line_number, 42)
        self.assertEqual(todo.line, content)
        self.assertIsNone(todo.ticket)

    def test_line_with_a_server_ticket(self):
        ticket = "SERVER-12345"
        content = f"a line with {ticket} some random text"
        todo = under_test.Todo.from_line("file", 42, f"\n{content}\n\t\n")

        self.assertEqual(todo.file_name, "file")
        self.assertEqual(todo.line_number, 42)
        self.assertEqual(todo.line, content)
        self.assertEqual(todo.ticket, ticket)

    def test_line_with_a_wiredtiger_ticket(self):
        ticket = "WT-5555"
        content = f"a line with {ticket} some random text"
        todo = under_test.Todo.from_line("file", 42, f"\n{content}\n\t\n")

        self.assertEqual(todo.file_name, "file")
        self.assertEqual(todo.line_number, 42)
        self.assertEqual(todo.line, content)
        self.assertEqual(todo.ticket, ticket)


class TestTodoChecker(unittest.TestCase):
    def test_todo_checker_starts_out_empty(self):
        todos = under_test.TodoChecker()

        self.assertEqual(len(todos.found_todos.no_tickets), 0)
        self.assertEqual(len(todos.found_todos.with_tickets), 0)
        self.assertEqual(len(todos.found_todos.by_file), 0)

    def test_a_file_with_no_todos(self):
        file_contents = """
        line 0
        line 1
        this is the file contents.
        
        """
        todos = under_test.TodoChecker()

        todos.check_file("my file", create_file_iterator(file_contents))

        self.assertEqual(len(todos.found_todos.no_tickets), 0)
        self.assertEqual(len(todos.found_todos.with_tickets), 0)
        self.assertEqual(len(todos.found_todos.by_file), 0)

    def test_a_file_with_an_untagged_todo(self):
        file_contents = """
        line 0
        line 1
        /* todo this needs some updating */
        this is the file contents.
        """
        todos = under_test.TodoChecker()

        todos.check_file("my file", create_file_iterator(file_contents))

        self.assertEqual(len(todos.found_todos.no_tickets), 1)
        self.assertEqual(len(todos.found_todos.with_tickets), 0)
        self.assertEqual(len(todos.found_todos.by_file), 1)

        todo = todos.found_todos.no_tickets[0]
        self.assertEqual(todo.file_name, "my file")
        self.assertEqual(todo.line_number, 3)
        self.assertEqual(todo.ticket, None)

        self.assertEqual(todo, todos.found_todos.by_file["my file"][0])

    def test_a_file_with_a_tagged_todo(self):
        file_contents = """
        line 0
        line 1
        line 2
        /* TODO server-1234 this needs some updating */
        this is the file contents.
        """
        todos = under_test.TodoChecker()

        todos.check_file("my file", create_file_iterator(file_contents))

        self.assertEqual(len(todos.found_todos.no_tickets), 0)
        self.assertEqual(len(todos.found_todos.with_tickets), 1)
        self.assertEqual(len(todos.found_todos.by_file), 1)

        todo = todos.found_todos.with_tickets["SERVER-1234"][0]
        self.assertEqual(todo.file_name, "my file")
        self.assertEqual(todo.line_number, 4)
        self.assertEqual(todo.ticket, "SERVER-1234")

        self.assertEqual(todo, todos.found_todos.by_file["my file"][0])

    def test_report_on_ticket_will_return_true_if_ticket_is_found(self):
        file_contents = """
        line 0
        line 1
        line 2
        /* TODO server-1234 this needs some updating */
        this is the file contents.
        """
        todos = under_test.TodoChecker()
        todos.check_file("my file", create_file_iterator(file_contents))

        self.assertTrue(todos.report_on_ticket("SERVER-1234"))

    def test_report_on_ticket_will_return_false_if_ticket_is_not_found(self):
        file_contents = """
        line 0
        line 1
        line 2
        /* TODO server-1234 this needs some updating */
        this is the file contents.
        """
        todos = under_test.TodoChecker()
        todos.check_file("my file", create_file_iterator(file_contents))

        self.assertFalse(todos.report_on_ticket("SERVER-9876"))

    def test_report_all_tickets_will_return_true_if_any_ticket_is_found(self):
        file_contents = """
        line 0
        line 1
        line 2
        /* TODO server-1234 this needs some updating */
        this is the file contents.
        /* TODO server-54321 this also needs some updating */
        """
        todos = under_test.TodoChecker()
        todos.check_file("my file", create_file_iterator(file_contents))

        self.assertTrue(todos.report_on_all_tickets())

    def test_report_all_tickets_will_return_false_if_no_ticket_is_found(self):
        file_contents = """
        line 0
        line 1
        line 2
        this is the file contents.
        """
        todos = under_test.TodoChecker()
        todos.check_file("my file", create_file_iterator(file_contents))

        self.assertFalse(todos.report_on_all_tickets())


class TestValidateCommitQueue(unittest.TestCase):
    def test_revert_commits_should_not_fail(self):
        commit_message = "Reverts commit SERVER-1234"
        file_contents = """
        line 0
        line 1
        line 2
        /* TODO server-1234 this needs some updating */
        this is the file contents.
        /* TODO server-54321 this also needs some updating */
        """
        todos = under_test.TodoChecker()
        todos.check_file("my file", create_file_iterator(file_contents))

        self.assertFalse(todos.validate_commit_queue(commit_message))

    def test_todos_associated_with_commit_message_should_be_found(self):
        commit_message = "SERVER-1234 making a commit"
        file_contents = """
        line 0
        line 1
        line 2
        /* TODO server-1234 this needs some updating */
        this is the file contents.
        /* TODO server-54321 this also needs some updating */
        """
        todos = under_test.TodoChecker()
        todos.check_file("my file", create_file_iterator(file_contents))

        self.assertTrue(todos.validate_commit_queue(commit_message))

    def test_commit_messages_with_multiple_commits_search_all_of_them(self):
        commit_message = """
        Making a wiredtiger drop
        
        WT-1234
        WT-4321
        WT-9876
        """
        file_contents = """
        line 0
        line 1
        line 2
        /* TODO server-1234 this needs some updating */
        this is the file contents.
        /* TODO WT-9876 this also needs some updating */
        """
        todos = under_test.TodoChecker()
        todos.check_file("my file", create_file_iterator(file_contents))

        self.assertTrue(todos.validate_commit_queue(commit_message))

    def test_commit_messages_with_no_tickets_doesnt_cause_issues(self):
        commit_message = "A random commit"
        file_contents = """
        line 0
        line 1
        line 2
        /* TODO server-1234 this needs some updating */
        this is the file contents.
        /* TODO WT-9876 this also needs some updating */
        """
        todos = under_test.TodoChecker()
        todos.check_file("my file", create_file_iterator(file_contents))

        self.assertFalse(todos.validate_commit_queue(commit_message))


def write_file(path: str, contents: str):
    with open(path, "w") as fh:
        fh.write(contents)


class TestWalkFs(unittest.TestCase):
    def test_walk_fs_walks_the_fs(self):
        expected_files = {
            "file1.txt": "The contents of file 1",
            "file2.txt": "The contents of file 2",
        }
        with TemporaryDirectory() as tmpdir:
            write_file(os.path.join(tmpdir, "file1.txt"), expected_files["file1.txt"])
            os.makedirs(os.path.join(tmpdir, "dir0", "dir1"))
            write_file(
                os.path.join(tmpdir, "dir0", "dir1", "file2.txt"), expected_files["file2.txt"])

            seen_files = {}

            def visit_file(file_name, file_contents):
                base_name = os.path.basename(file_name)
                seen_files[base_name] = "\n".join(file_contents)

            under_test.walk_fs(tmpdir, visit_file)

            self.assertDictEqual(expected_files, seen_files)

"""Unit tests for the evergreen_task_timeout script."""
import itertools
import unittest
from typing import List
from mock import MagicMock, patch

import evergreen

from buildscripts.validate_commit_message import main, STATUS_OK, STATUS_ERROR

# pylint: disable=missing-docstring,no-self-use

INVALID_MESSAGES = [
    "",  # You must provide a message
    "RevertEVG-1",  # revert and ticket must be formatted
    "revert EVG-1",  # revert must be capitalized
    "This is not a valid message",  # message must be valid
    "Fix Lint",  # Fix lint is strict in terms of caps
]

NS = "buildscripts.validate_commit_message"


def ns(relative_name):  # pylint: disable=invalid-name
    """Return a full name from a name relative to the test module"s name space."""
    return NS + "." + relative_name


def create_mock_evg_client(code_change_messages: List[str]) -> MagicMock:
    mock_code_change = MagicMock()
    mock_code_change.commit_messages = code_change_messages

    mock_patch = MagicMock()
    mock_patch.module_code_changes = [mock_code_change]

    mock_evg_client = MagicMock()
    mock_evg_client.patch_by_id.return_value = mock_patch
    return mock_evg_client


def interleave_new_format(older):
    """Create a new list containing a new and old format copy of each string."""
    newer = [
        f"Commit Queue Merge: '{old}' into 'mongodb/mongo:SERVER-45949-validate-message-format'"
        for old in older
    ]
    return list(itertools.chain(*zip(older, newer)))


class ValidateCommitMessageTest(unittest.TestCase):
    @patch.object(evergreen.RetryingEvergreenApi, "get_api")
    def test_valid_commits(self, get_api_mock):
        messages = [
            "Fix lint",
            "EVG-1",  # Test valid projects with various number lengths
            "SERVER-20",
            "WT-300",
            "SERVER-44338",
            "Revert EVG-5",
            "Revert SERVER-60",
            "Revert WT-700",
            "Revert 'SERVER-8000",
            'Revert "SERVER-90000',
            "Import wiredtiger: 58115abb6fbb3c1cc7bfd087d41a47347bce9a69 from branch mongodb-4.4",
            "Import tools: 58115abb6fbb3c1cc7bfd087d41a47347bce9a69 from branch mongodb-4.4",
            'Revert "Import wiredtiger: 58115abb6fbb3c1cc7bfd087d41a47347bce9a69 from branch mongodb-4.4"',
        ]
        api_mock = create_mock_evg_client(interleave_new_format(messages))

        get_api_mock.return_value = api_mock
        self.assertTrue(main(["fake_version"]) == STATUS_OK)

    @patch.object(evergreen.RetryingEvergreenApi, "get_api")
    def test_private(self, get_api_mock):
        messages = ["XYZ-1"]
        api_mock = create_mock_evg_client(interleave_new_format(messages))

        get_api_mock.return_value = api_mock
        self.assertTrue(main(["fake_version"]) == STATUS_ERROR)

    @patch.object(evergreen.RetryingEvergreenApi, "get_api")
    def test_private_with_public(self, get_api_mock):
        messages = [
            "Fix lint",
            "EVG-1",  # Test valid projects with various number lengths
            "SERVER-20",
            "XYZ-1"
        ]
        api_mock = create_mock_evg_client(interleave_new_format(messages))

        get_api_mock.return_value = api_mock
        self.assertTrue(main(["fake_version"]) == STATUS_ERROR)

"""Unit tests for the validate_mongocryptd script."""

from __future__ import absolute_import

import unittest

from mock import MagicMock, patch

from buildscripts import validate_mongocryptd as under_test

# pylint: disable=missing-docstring,no-self-use

NS = "buildscripts.validate_mongocryptd"


def ns(relative_name):  # pylint: disable=invalid-name
    """Return a full name from a name relative to the test module"s name space."""
    return NS + "." + relative_name


class TestCanValidationBeSkipped(unittest.TestCase):
    def test_non_existing_variant_can_be_skipped(self):
        mock_evg_config = MagicMock()
        mock_evg_config.get_variant.return_value = None
        self.assertTrue(under_test.can_validation_be_skipped(mock_evg_config, "variant"))

    def test_variant_with_no_push_task_can_be_skipped(self):
        mock_evg_config = MagicMock()
        mock_evg_config.get_variant.return_value.task_names = ["task 1", "task 2"]
        self.assertTrue(under_test.can_validation_be_skipped(mock_evg_config, "variant"))

    def test_variant_with_push_task_cannot_be_skipped(self):
        mock_evg_config = MagicMock()
        mock_evg_config.get_variant.return_value.task_names = ["task 1", "push", "task 2"]
        self.assertFalse(under_test.can_validation_be_skipped(mock_evg_config, "variant"))


class TestReadVariableFromYml(unittest.TestCase):
    @patch(ns("open"))
    @patch(ns("yaml"))
    def test_variable_not_in_variables(self, yaml_mock, _):
        mock_nodes = {
            "variables": {},
        }

        yaml_mock.safe_load.return_value = mock_nodes
        self.assertIsNone(under_test.read_variable_from_yml("filename", "variable"))

    @patch(ns("open"))
    @patch(ns("yaml"))
    def test_variable_is_in_variables(self, yaml_mock, _):
        search_key = "var 2"
        expected_value = "value 2"
        mock_nodes = {
            "variables": [
                {"var 1": "value 1"},
                {search_key: expected_value},
                {"var 3": "value 3"},
            ],
        }

        yaml_mock.safe_load.return_value = mock_nodes
        self.assertEqual(expected_value, under_test.read_variable_from_yml("filename", search_key))

"""Empty."""

"""Unit tests for the burn_in_tags.py script."""
from collections import defaultdict
import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from shrub.v2 import ShrubProject

import buildscripts.ciconfig.evergreen as _evergreen
from buildscripts.burn_in_tests import TaskInfo
from buildscripts.tests.test_burn_in_tests import ns as burn_in_tests_ns
from buildscripts.ciconfig.evergreen import EvergreenProjectConfig

import buildscripts.burn_in_tags as under_test

# pylint: disable=missing-docstring,invalid-name,unused-argument,no-self-use,protected-access

EMPTY_PROJECT = {
    "buildvariants": [],
    "tasks": [],
}
TEST_FILE_PATH = os.path.join(os.path.dirname(__file__), "test_burn_in_tags_evergreen.yml")

NS = "buildscripts.burn_in_tags"


def ns(relative_name):  # pylint: disable-invalid-name
    """Return a full name from a name relative to the test module"s name space."""
    return NS + "." + relative_name


def get_expansions_data():
    return {
            "branch_name": "fake_branch",
            "build_variant": "enterprise-rhel-80-64-bit",
            "check_evergreen": 2,
            "distro_id": "rhel80-small",
            "is_patch": "true",
            "max_revisions": 25,
            "repeat_tests_max": 1000,
            "repeat_tests_min": 2,
            "repeat_tests_secs": 600,
            "revision": "fake_sha",
            "project": "fake_project",
            "task_id": "task id",
    }  # yapf: disable


def get_evergreen_config() -> EvergreenProjectConfig:
    return _evergreen.parse_evergreen_file(TEST_FILE_PATH, evergreen_binary=None)


class TestCreateEvgBuildVariantMap(unittest.TestCase):
    def test_create_evg_buildvariant_map(self):
        evg_conf_mock = get_evergreen_config()
        expansions_file_data = {"build_variant": "enterprise-rhel-80-64-bit"}

        buildvariant_map = under_test._create_evg_build_variant_map(expansions_file_data,
                                                                    evg_conf_mock)

        expected_buildvariant_map = {
            "enterprise-rhel-80-64-bit-majority-read-concern-off":
                "enterprise-rhel-80-64-bit-majority-read-concern-off-required",
            "enterprise-rhel-80-64-bit-inmem":
                "enterprise-rhel-80-64-bit-inmem-required"
        }
        self.assertEqual(buildvariant_map, expected_buildvariant_map)

    def test_create_evg_buildvariant_map_no_base_variants(self):
        evg_conf_mock = MagicMock()
        evg_conf_mock.parse_evergreen_file.return_value = get_evergreen_config()
        expansions_file_data = {"build_variant": "buildvariant-without-burn-in-tag-buildvariants"}

        buildvariant_map = under_test._create_evg_build_variant_map(expansions_file_data,
                                                                    evg_conf_mock)

        self.assertEqual(buildvariant_map, {})


class TestGenerateEvgBuildVariants(unittest.TestCase):
    def test_generate_evg_buildvariant_one_base_variant(self):
        evg_conf_mock = get_evergreen_config()
        base_variant = "enterprise-rhel-80-64-bit-inmem"
        generated_variant = "enterprise-rhel-80-64-bit-inmem-required"
        burn_in_tags_gen_variant = "enterprise-rhel-80-64-bit"
        variant = evg_conf_mock.get_variant(base_variant)

        build_variant = under_test._generate_evg_build_variant(variant, generated_variant,
                                                               burn_in_tags_gen_variant)

        generated_build_variant = build_variant.as_dict()
        self.assertEqual(generated_build_variant["name"], generated_variant)
        self.assertEqual(generated_build_variant["modules"], variant.modules)
        generated_expansions = generated_build_variant["expansions"]
        burn_in_bypass_expansion_value = generated_expansions.pop("burn_in_bypass")
        self.assertEqual(burn_in_bypass_expansion_value, burn_in_tags_gen_variant)
        self.assertEqual(generated_expansions, variant.expansions)


class TestGenerateEvgTasks(unittest.TestCase):
    @patch(ns("create_tests_by_task"))
    def test_generate_evg_tasks_no_tests_changed(self, create_tests_by_task_mock):
        evg_conf_mock = get_evergreen_config()
        create_tests_by_task_mock.return_value = {}
        expansions_file_data = get_expansions_data()
        buildvariant_map = {
            "enterprise-rhel-80-64-bit-inmem": "enterprise-rhel-80-64-bit-inmem-required",
            "enterprise-rhel-80-64-bit-majority-read-concern-off":
                "enterprise-rhel-80-64-bit-majority-read-concern-off-required",
        }  # yapf: disable
        shrub_config = ShrubProject()
        evergreen_api = MagicMock()
        repo = MagicMock(working_dir=os.getcwd())
        under_test._generate_evg_tasks(evergreen_api, shrub_config, expansions_file_data,
                                       buildvariant_map, [repo], evg_conf_mock)

        self.assertEqual(shrub_config.as_dict(), EMPTY_PROJECT)

    @patch(ns("create_tests_by_task"))
    def test_generate_evg_tasks_one_test_changed(self, create_tests_by_task_mock):
        evg_conf_mock = get_evergreen_config()
        create_tests_by_task_mock.return_value = {
            "aggregation_mongos_passthrough": TaskInfo(
                display_task_name="aggregation_mongos_passthrough",
                resmoke_args="--suites=aggregation_mongos_passthrough --storageEngine=wiredTiger",
                tests=["jstests/aggregation/ifnull.js"],
                require_multiversion=None,
                distro="",
            )
        }  # yapf: disable
        expansions_file_data = get_expansions_data()
        buildvariant_map = {
            "enterprise-rhel-80-64-bit-inmem": "enterprise-rhel-80-64-bit-inmem-required",
            "enterprise-rhel-80-64-bit-majority-read-concern-off":
                "enterprise-rhel-80-64-bit-majority-read-concern-off-required",
        }  # yapf: disable
        shrub_config = ShrubProject.empty()
        evergreen_api = MagicMock()
        repo = MagicMock(working_dir=os.getcwd())
        evergreen_api.test_stats_by_project.return_value = [
            MagicMock(test_file="dir/test2.js", avg_duration_pass=10)
        ]
        under_test._generate_evg_tasks(evergreen_api, shrub_config, expansions_file_data,
                                       buildvariant_map, [repo], evg_conf_mock)

        generated_config = shrub_config.as_dict()
        self.assertEqual(len(generated_config["buildvariants"]), 2)
        first_generated_build_variant = generated_config["buildvariants"][0]
        self.assertIn(first_generated_build_variant["name"], buildvariant_map.values())
        self.assertEqual(first_generated_build_variant["display_tasks"][0]["name"], "burn_in_tests")
        self.assertEqual(
            first_generated_build_variant["display_tasks"][0]["execution_tasks"][0],
            f"burn_in:aggregation_mongos_passthrough_0_{first_generated_build_variant['name']}")


EXPANSIONS_FILE_DATA = {
    "build_variant": "enterprise-rhel-80-64-bit",
    "revision": "badf00d000000000000000000000000000000000", "max_revisions": "1000",
    "branch_name": "mongodb-mongo-master", "is_patch": "false", "distro_id": "rhel62-small",
    "repeat_tests_min": "2", "repeat_tests_max": "1000", "repeat_tests_secs": "600", "project":
        "mongodb-mongo-master", "task_id": "task id"
}

CREATE_EVG_BUILD_VARIANT_MAP = {
    'enterprise-rhel-80-64-bit-majority-read-concern-off':
        'enterprise-rhel-80-64-bit-majority-read-concern-off-required',
    'enterprise-rhel-80-64-bit-inmem':
        'enterprise-rhel-80-64-bit-inmem-required'
}

CREATE_TEST_MEMBERSHIP_MAP = {
    "jstests/aggregation/accumulators/accumulator_js.js": [
        "aggregation", "aggregation_auth", "aggregation_disabled_optimization", "aggregation_ese",
        "aggregation_ese_gcm", "aggregation_facet_unwind_passthrough",
        "aggregation_mongos_passthrough", "aggregation_one_shard_sharded_collections",
        "aggregation_read_concern_majority_passthrough", "aggregation_secondary_reads",
        "aggregation_sharded_collections_passthrough"
    ], "jstests/core/create_collection.js": [
        "core", "core_auth", "core_ese", "core_ese_gcm", "core_minimum_batch_size", "core_op_query",
        "cwrwc_passthrough", "cwrwc_rc_majority_passthrough", "cwrwc_wc_majority_passthrough",
        "logical_session_cache_replication_100ms_refresh_jscore_passthrough",
        "logical_session_cache_replication_10sec_refresh_jscore_passthrough",
        "logical_session_cache_replication_1sec_refresh_jscore_passthrough",
        "logical_session_cache_replication_default_refresh_jscore_passthrough",
        "logical_session_cache_standalone_100ms_refresh_jscore_passthrough",
        "logical_session_cache_standalone_10sec_refresh_jscore_passthrough",
        "logical_session_cache_standalone_1sec_refresh_jscore_passthrough",
        "logical_session_cache_standalone_default_refresh_jscore_passthrough",
        "read_concern_linearizable_passthrough", "read_concern_majority_passthrough",
        "causally_consistent_read_concern_snapshot_passthrough",
        "replica_sets_initsync_jscore_passthrough",
        "replica_sets_initsync_static_jscore_passthrough", "replica_sets_jscore_passthrough",
        "replica_sets_kill_primary_jscore_passthrough",
        "replica_sets_kill_secondaries_jscore_passthrough",
        "replica_sets_reconfig_jscore_passthrough",
        "replica_sets_terminate_primary_jscore_passthrough", "retryable_writes_jscore_passthrough",
        "retryable_writes_jscore_stepdown_passthrough", "secondary_reads_passthrough",
        "session_jscore_passthrough", "write_concern_majority_passthrough"
    ]
}


class TestAcceptance(unittest.TestCase):
    @patch(ns("write_file_to_dir"))
    @patch(ns("_create_evg_build_variant_map"))
    @patch(ns("EvergreenFileChangeDetector"))
    def test_no_tests_run_if_none_changed(self, find_changed_tests_mock,
                                          create_evg_build_variant_map_mock, write_to_file_mock):
        """
        Given a git repository with no changes,
        When burn_in_tags is run,
        Then no tests are discovered to run.
        """
        repos = [MagicMock(working_dir=os.getcwd())]
        evg_conf_mock = MagicMock()
        find_changed_tests_mock.return_value.find_changed_tests.return_value = {}

        create_evg_build_variant_map_mock.return_value = CREATE_EVG_BUILD_VARIANT_MAP

        under_test.burn_in(EXPANSIONS_FILE_DATA, evg_conf_mock, MagicMock(), repos)

        write_to_file_mock.assert_called_once()
        shrub_config = write_to_file_mock.call_args[0][2]
        self.assertEqual(EMPTY_PROJECT, json.loads(shrub_config))

    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    @patch(ns("write_file_to_dir"))
    @patch(ns("_create_evg_build_variant_map"))
    @patch(ns("EvergreenFileChangeDetector"))
    @patch(burn_in_tests_ns("create_test_membership_map"))
    def test_tests_generated_if_a_file_changed(
            self, create_test_membership_map_mock, find_changed_tests_mock,
            create_evg_build_variant_map_mock, write_to_file_mock):
        """
        Given a git repository with changes,
        When burn_in_tags is run,
        Then some tags are discovered to run.
        """
        create_test_membership_map_mock.return_value = defaultdict(list, CREATE_TEST_MEMBERSHIP_MAP)

        repos = [MagicMock(working_dir=os.getcwd())]
        evg_conf = get_evergreen_config()
        create_evg_build_variant_map_mock.return_value = CREATE_EVG_BUILD_VARIANT_MAP
        find_changed_tests_mock.return_value.find_changed_tests.return_value = {
            'jstests/slow1/large_role_chain.js',
            'jstests/aggregation/accumulators/accumulator_js.js'
        }

        under_test.burn_in(EXPANSIONS_FILE_DATA, evg_conf, MagicMock(), repos)

        write_to_file_mock.assert_called_once()
        written_config = write_to_file_mock.call_args[0][2]
        written_config_map = json.loads(written_config)

        n_tasks = len(written_config_map["tasks"])
        # Ensure we are generating at least one task for the test.
        self.assertGreaterEqual(n_tasks, 1)

        written_build_variants = written_config_map["buildvariants"]
        written_build_variants_name = [variant['name'] for variant in written_build_variants]
        self.assertEqual(
            set(CREATE_EVG_BUILD_VARIANT_MAP.values()), set(written_build_variants_name))

        tasks = written_config_map["tasks"]
        self.assertGreaterEqual(len(tasks), len(CREATE_EVG_BUILD_VARIANT_MAP))

        self.assertTrue(
            all(
                len(display_tasks) == 1 for display_tasks in
                [build_variant["display_tasks"] for build_variant in written_build_variants]))

functions:
  "fetch source":
    - command: git.get_project
      params:
        directory: src
    - command: shell.exec
      params:
        working_dir: src
        script: |
          echo "this is a 2nd command in the function!"
          ls

tasks:
- name: compile
  depends_on: []
  commands:
    - func: "fetch source"
- name: burn_in_tags_gen
  depends_on: []
  commands:
    - func: "fake command"
- name: compile_all_run_unittests_TG
  depends_on: []
  commands:
    - func: "fake command"
- name: clang_tidy_TG
  depends_on: []
  commands:
    - func: "fake command"
- name: stitch_support_lib_build_and_archive
  depends_on: []
  commands:
    - func: "fake command"
- name: lint_pylinters
  depends_on: []
  commands:
    - func: "fake command"
- name: lint_clang_format
  depends_on: []
  commands:
    - func: "fake command"
- name: burn_in_tests_gen
  depends_on: []
  commands:
    - func: "fake command"
- name: aggregation_multiversion_fuzzer_gen
  depends_on: []
  commands:
    - func: "fake command"
- name: aggregation_expression_multiversion_fuzzer_gen
  depends_on: []
  commands:
    - func: "fake command"
- name: aggregation
  depends_on:
  - name: compile
  commands:
  - func: run tests
    vars:
      resmoke_args: --suites=aggregation --storageEngine=wiredTiger

buildvariants:
- name: enterprise-rhel-80-64-bit
  display_name: "! Enterprise RHEL 8.0"
  expansions:
    multiversion_platform: rhel80
    burn_in_tag_buildvariants: enterprise-rhel-80-64-bit-majority-read-concern-off enterprise-rhel-80-64-bit-inmem
  tasks:
  - name: compile_all_run_unittests_TG
    distros:
    - rhel80-large
  - name: lint_pylinters
  - name: burn_in_tests_gen
  # TODO SERVER-58718: Re-enable this suite.
  # - name: aggregation_multiversion_fuzzer_gen
  - name: aggregation
  - name: burn_in_tags_gen
- name: buildvariant-without-burn-in-tag-buildvariants
  display_name: "Buildvariant without burn in tag buildvariants expansion"
  expansions:
    multiversion_platform: rhel80
  tasks:
  - name: burn_in_tags_gen
- name: enterprise-rhel-80-64-bit-majority-read-concern-off
  display_name: "Enterprise RHEL 8.0 (majority read concern off)"
  modules: ["enterprise"]
  run_on:
  - rhel80-small
  expansions: &enterprise-rhel-80-64-bit-majority-read-concern-off-expansions
    multiversion_edition: enterprise
  tasks:
  - name: compile_all_run_unittests_TG
    distros:
    - rhel80-large
  # TODO SERVER-58718: Re-enable this suite.
  # - name: aggregation_multiversion_fuzzer_gen
  - name: aggregation
- name: enterprise-rhel-80-64-bit-inmem
  display_name: Enterprise RHEL 8.0 (inMemory)
  modules: ["enterprise"]
  run_on:
  - rhel80-small
  expansions: &enterprise-rhel-80-64-bit-inmem-expansions
    test_flags: >-
      --majorityReadConcern=off
      --excludeWithAnyTags=requires_majority_read_concern,uses_prepare_transaction,uses_multi_shard_transaction,uses_atclustertime
    compile_flags: >-
      -j$(grep -c ^processor /proc/cpuinfo)
      --ssl
      --release
      --variables-files=etc/scons/mongodbtoolchain_v3_gcc.vars
      MONGO_DISTMOD=rhel80
    multiversion_platform: rhel80
    multiversion_edition: enterprise
    scons_cache_scope: shared
    tooltags: "ssl sasl gssapi"
    large_distro_name: rhel80-large
  tasks:
  - name: compile
- name: enterprise-rhel-80-64-bit-inmem
  display_name: Enterprise RHEL 8.0 (inMemory)
  expansions:
    additional_targets: archive-mongocryptd archive-mongocryptd-debug
    compile_flags: --ssl MONGO_DISTMOD=rhel80 -j$(grep -c ^processor /proc/cpuinfo)
      --variables-files=etc/scons/mongodbtoolchain_v3_gcc.vars
    large_distro_name: rhel80-large
    multiversion_edition: enterprise
    multiversion_platform: rhel80
    scons_cache_scope: shared
    test_flags: --storageEngine=inMemory --excludeWithAnyTags=requires_persistence,requires_journaling
  modules:
  - enterprise
  run_on:
  - rhel80-small
  tasks:
  - name: compile_all_run_unittests_TG
    distros:
    - rhel80-large
  # TODO SERVER-58718: Re-enable this suite.
  # - name: aggregation_multiversion_fuzzer_gen
  - name: aggregation

"""Unit tests for buildscripts/burn_in_tests.py."""

from __future__ import absolute_import

import collections
import datetime
import os
import sys
import subprocess
import unittest

from mock import Mock, patch, MagicMock

import buildscripts.burn_in_tests as under_test
from buildscripts.ciconfig.evergreen import parse_evergreen_file
import buildscripts.resmokelib.parser as _parser
_parser.set_run_options()

# pylint: disable=missing-docstring,protected-access,too-many-lines,no-self-use


def create_tests_by_task_mock(n_tasks, n_tests):
    return {
        f"task_{i}_gen":
        under_test.TaskInfo(display_task_name=f"task_{i}", resmoke_args=f"--suites=suite_{i}",
                            tests=[f"jstests/tests_{j}" for j in range(n_tests)],
                            require_multiversion=None, distro=f"distro_{i}")
        for i in range(n_tasks)
    }


MV_MOCK_SUITES = ["replica_sets_jscore_passthrough", "sharding_jscore_passthrough"]

_DATE = datetime.datetime(2018, 7, 15)
RESMOKELIB = "buildscripts.resmokelib"

NS = "buildscripts.burn_in_tests"


def ns(relative_name):  # pylint: disable=invalid-name
    """Return a full name from a name relative to the test module"s name space."""
    return NS + "." + relative_name


def mock_a_file(filename):
    change = MagicMock(a_path=filename)
    return change


def mock_git_diff(change_list):
    diff = MagicMock()
    diff.iter_change_type.return_value = change_list
    return diff


def mock_changed_git_files(add_files):
    repo = MagicMock()
    repo.index.diff.return_value = mock_git_diff([mock_a_file(f) for f in add_files])
    repo.working_dir = "."
    return repo


def get_evergreen_config(config_file_path):
    evergreen_home = os.path.expanduser(os.path.join("~", "evergreen"))
    if os.path.exists(evergreen_home):
        return parse_evergreen_file(config_file_path, evergreen_home)
    return parse_evergreen_file(config_file_path)


class TestRepeatConfig(unittest.TestCase):
    def test_validate_no_args(self):
        repeat_config = under_test.RepeatConfig()

        self.assertEqual(repeat_config, repeat_config.validate())

    def test_validate_with_both_repeat_options_specified(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_secs=10, repeat_tests_num=5)

        with self.assertRaises(ValueError):
            repeat_config.validate()

    def test_validate_with_repeat_max_with_no_secs(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_max=10)

        with self.assertRaises(ValueError):
            repeat_config.validate()

    def test_validate_with_repeat_min_greater_than_max(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_max=10, repeat_tests_min=100,
                                                repeat_tests_secs=15)

        with self.assertRaises(ValueError):
            repeat_config.validate()

    def test_validate_with_repeat_min_with_no_secs(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_min=10)

        with self.assertRaises(ValueError):
            repeat_config.validate()

    def test_get_resmoke_repeat_options_num(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_num=5)
        repeat_options = repeat_config.generate_resmoke_options()

        self.assertEqual(repeat_options.strip(), "--repeatSuites=5")

    def test_get_resmoke_repeat_options_secs(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_secs=5)
        repeat_options = repeat_config.generate_resmoke_options()

        self.assertEqual(repeat_options.strip(), "--repeatTestsSecs=5")

    def test_get_resmoke_repeat_options_secs_min(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_secs=5, repeat_tests_min=2)
        repeat_options = repeat_config.generate_resmoke_options()

        self.assertIn("--repeatTestsSecs=5", repeat_options)
        self.assertIn("--repeatTestsMin=2", repeat_options)
        self.assertNotIn("--repeatTestsMax", repeat_options)
        self.assertNotIn("--repeatSuites", repeat_options)

    def test_get_resmoke_repeat_options_secs_max(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_secs=5, repeat_tests_max=2)
        repeat_options = repeat_config.generate_resmoke_options()

        self.assertIn("--repeatTestsSecs=5", repeat_options)
        self.assertIn("--repeatTestsMax=2", repeat_options)
        self.assertNotIn("--repeatTestsMin", repeat_options)
        self.assertNotIn("--repeatSuites", repeat_options)

    def test_get_resmoke_repeat_options_secs_min_max(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_secs=5, repeat_tests_min=2,
                                                repeat_tests_max=2)
        repeat_options = repeat_config.generate_resmoke_options()

        self.assertIn("--repeatTestsSecs=5", repeat_options)
        self.assertIn("--repeatTestsMin=2", repeat_options)
        self.assertIn("--repeatTestsMax=2", repeat_options)
        self.assertNotIn("--repeatSuites", repeat_options)

    def test_get_resmoke_repeat_options_min(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_min=2)
        repeat_options = repeat_config.generate_resmoke_options()

        self.assertEqual(repeat_options.strip(), "--repeatSuites=2")

    def test_get_resmoke_repeat_options_max(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_max=2)
        repeat_options = repeat_config.generate_resmoke_options()

        self.assertEqual(repeat_options.strip(), "--repeatSuites=2")


class TestGetTaskName(unittest.TestCase):
    def test__get_task_name(self):
        name = "mytask"
        task = Mock()
        task.is_generate_resmoke_task = False
        task.name = name
        self.assertEqual(name, under_test._get_task_name(task))

    def test__get_task_name_generate_resmoke_task(self):
        task_name = "mytask"
        task = Mock(is_generate_resmoke_task=True, generated_task_name=task_name)
        self.assertEqual(task_name, under_test._get_task_name(task))


class TestSetResmokeArgs(unittest.TestCase):
    def test__set_resmoke_args(self):
        resmoke_args = "--suites=suite1 test1.js"
        task = Mock()
        task.combined_resmoke_args = resmoke_args
        task.is_generate_resmoke_task = False
        self.assertEqual(resmoke_args, under_test._set_resmoke_args(task))

    def test__set_resmoke_args_gen_resmoke_task(self):
        resmoke_args = "--suites=suite1 test1.js"
        new_suite = "suite2"
        new_resmoke_args = "--suites={} test1.js".format(new_suite)
        task = Mock()
        task.combined_resmoke_args = resmoke_args
        task.is_generate_resmoke_task = True
        task.get_vars_suite_name = lambda cmd_vars: cmd_vars["suite"]
        task.generate_resmoke_tasks_command = {"vars": {"suite": new_suite}}
        self.assertEqual(new_resmoke_args, under_test._set_resmoke_args(task))

    def test__set_resmoke_args_gen_resmoke_task_no_suite(self):
        suite = "suite1"
        resmoke_args = "--suites={} test1.js".format(suite)
        task = Mock()
        task.combined_resmoke_args = resmoke_args
        task.is_generate_resmoke_task = True
        task.get_vars_suite_name = lambda cmd_vars: cmd_vars["task"]
        task.generate_resmoke_tasks_command = {"vars": {"task": suite}}
        self.assertEqual(resmoke_args, under_test._set_resmoke_args(task))


class TestSetResmokeCmd(unittest.TestCase):
    def test__set_resmoke_cmd_no_opts_no_args(self):
        repeat_config = under_test.RepeatConfig()
        resmoke_cmds = under_test._set_resmoke_cmd(repeat_config, [])

        self.assertListEqual(resmoke_cmds,
                             [sys.executable, "buildscripts/resmoke.py", "run", '--repeatSuites=2'])

    def test__set_resmoke_cmd_no_opts(self):
        repeat_config = under_test.RepeatConfig()
        resmoke_args = ["arg1", "arg2"]

        resmoke_cmd = under_test._set_resmoke_cmd(repeat_config, resmoke_args)

        self.assertListEqual(resmoke_args + ['--repeatSuites=2'], resmoke_cmd)

    def test__set_resmoke_cmd(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_num=3)
        resmoke_args = ["arg1", "arg2"]

        resmoke_cmd = under_test._set_resmoke_cmd(repeat_config, resmoke_args)

        self.assertListEqual(resmoke_args + ['--repeatSuites=3'], resmoke_cmd)


class RunTests(unittest.TestCase):
    @patch(ns('subprocess.check_call'))
    def test_run_tests_no_tests(self, check_call_mock):
        tests_by_task = {}
        resmoke_cmd = ["python", "buildscripts/resmoke.py", "run", "--continueOnFailure"]

        under_test.run_tests(tests_by_task, resmoke_cmd)

        check_call_mock.assert_not_called()

    @patch(ns('subprocess.check_call'))
    def test_run_tests_some_test(self, check_call_mock):
        n_tasks = 3
        tests_by_task = create_tests_by_task_mock(n_tasks, 5)
        resmoke_cmd = ["python", "buildscripts/resmoke.py", "run", "--continueOnFailure"]

        under_test.run_tests(tests_by_task, resmoke_cmd)

        self.assertEqual(n_tasks, check_call_mock.call_count)

    @patch(ns('sys.exit'))
    @patch(ns('subprocess.check_call'))
    def test_run_tests_tests_resmoke_failure(self, check_call_mock, exit_mock):
        error_code = 42
        n_tasks = 3
        tests_by_task = create_tests_by_task_mock(n_tasks, 5)
        resmoke_cmd = ["python", "buildscripts/resmoke.py", "run", "--continueOnFailure"]
        check_call_mock.side_effect = subprocess.CalledProcessError(error_code, "err1")
        exit_mock.side_effect = ValueError('exiting')

        with self.assertRaises(ValueError):
            under_test.run_tests(tests_by_task, resmoke_cmd)

        self.assertEqual(1, check_call_mock.call_count)
        exit_mock.assert_called_with(error_code)


MEMBERS_MAP = {
    "test1.js": ["suite1", "suite2"], "test2.js": ["suite1", "suite3"], "test3.js": [],
    "test4.js": ["suite1", "suite2", "suite3"], "test5.js": ["suite2"]
}

SUITE1 = Mock()
SUITE1.tests = ["test1.js", "test2.js", "test4.js"]
SUITE2 = Mock()
SUITE2.tests = ["test1.js"]
SUITE3 = Mock()
SUITE3.tests = ["test2.js", "test4.js"]


def _create_executor_list(suites, exclude_suites):
    with patch(ns("create_test_membership_map"), return_value=MEMBERS_MAP):
        return under_test.create_executor_list(suites, exclude_suites)


class CreateExecutorList(unittest.TestCase):
    def test_create_executor_list_no_excludes(self):
        suites = [SUITE1, SUITE2]
        exclude_suites = []
        executor_list = _create_executor_list(suites, exclude_suites)
        self.assertEqual(executor_list["suite1"], SUITE1.tests)
        self.assertEqual(executor_list["suite2"], ["test1.js", "test4.js"])
        self.assertEqual(executor_list["suite3"], ["test2.js", "test4.js"])

    def test_create_executor_list_excludes(self):
        suites = [SUITE1, SUITE2]
        exclude_suites = ["suite3"]
        executor_list = _create_executor_list(suites, exclude_suites)
        self.assertEqual(executor_list["suite1"], SUITE1.tests)
        self.assertEqual(executor_list["suite2"], ["test1.js", "test4.js"])
        self.assertEqual(executor_list["suite3"], [])

    def test_create_executor_list_nosuites(self):
        executor_list = _create_executor_list([], [])
        self.assertEqual(executor_list, collections.defaultdict(list))

    @patch(RESMOKELIB + ".testing.suite.Suite")
    @patch(RESMOKELIB + ".suitesconfig.get_named_suites")
    def test_create_executor_list_runs_core_suite(self, mock_get_named_suites, mock_suite_class):
        mock_get_named_suites.return_value = ["core"]

        under_test.create_executor_list([], [])
        self.assertEqual(mock_suite_class.call_count, 1)

    @patch(RESMOKELIB + ".testing.suite.Suite")
    @patch(RESMOKELIB + ".suitesconfig.get_named_suites")
    def test_create_executor_list_ignores_dbtest_suite(self, mock_get_named_suites,
                                                       mock_suite_class):
        mock_get_named_suites.return_value = ["dbtest"]

        under_test.create_executor_list([], [])
        self.assertEqual(mock_suite_class.call_count, 0)


def create_variant_task_mock(task_name, suite_name, distro="distro"):
    variant_task = MagicMock()
    variant_task.name = task_name
    variant_task.generated_task_name = task_name
    variant_task.resmoke_suite = suite_name
    variant_task.get_vars_suite_name.return_value = suite_name
    variant_task.combined_resmoke_args = f"--suites={suite_name}"
    variant_task.require_multiversion = None
    variant_task.run_on = [distro]
    return variant_task


class TestTaskInfo(unittest.TestCase):
    def test_non_generated_task(self):
        suite_name = "suite_1"
        distro_name = "distro_1"
        variant = "build_variant"
        evg_conf_mock = MagicMock()
        evg_conf_mock.get_task.return_value.is_generate_resmoke_task = False
        task_mock = create_variant_task_mock("task 1", suite_name, distro_name)
        test_list = [f"test{i}.js" for i in range(3)]
        tests_by_suite = {
            suite_name: test_list,
            "suite 2": [f"test{i}.js" for i in range(1)],
            "suite 3": [f"test{i}.js" for i in range(2)],
        }

        task_info = under_test.TaskInfo.from_task(task_mock, tests_by_suite, evg_conf_mock, variant)

        self.assertIn(suite_name, task_info.resmoke_args)
        for test in test_list:
            self.assertIn(test, task_info.tests)
        self.assertIsNone(task_info.require_multiversion)
        self.assertEqual(distro_name, task_info.distro)

    def test_generated_task_no_large_on_task(self):
        suite_name = "suite_1"
        distro_name = "distro_1"
        variant = "build_variant"
        evg_conf_mock = MagicMock()
        task_def_mock = evg_conf_mock.get_task.return_value
        task_def_mock.is_generate_resmoke_task = True
        task_def_mock.generate_resmoke_tasks_command = {"vars": {}}
        task_mock = create_variant_task_mock("task 1", suite_name, distro_name)
        test_list = [f"test{i}.js" for i in range(3)]
        tests_by_suite = {
            suite_name: test_list,
            "suite 2": [f"test{i}.js" for i in range(1)],
            "suite 3": [f"test{i}.js" for i in range(2)],
        }

        task_info = under_test.TaskInfo.from_task(task_mock, tests_by_suite, evg_conf_mock, variant)

        self.assertIn(suite_name, task_info.resmoke_args)
        for test in test_list:
            self.assertIn(test, task_info.tests)
        self.assertIsNone(task_info.require_multiversion)
        self.assertEqual(distro_name, task_info.distro)

    def test_generated_task_no_large_on_build_variant(self):
        suite_name = "suite_1"
        distro_name = "distro_1"
        variant = "build_variant"
        evg_conf_mock = MagicMock()
        task_def_mock = evg_conf_mock.get_task.return_value
        task_def_mock.is_generate_resmoke_task = True
        task_def_mock.generate_resmoke_tasks_command = {"vars": {"use_large_distro": True}}
        task_mock = create_variant_task_mock("task 1", suite_name, distro_name)
        test_list = [f"test{i}.js" for i in range(3)]
        tests_by_suite = {
            suite_name: test_list,
            "suite 2": [f"test{i}.js" for i in range(1)],
            "suite 3": [f"test{i}.js" for i in range(2)],
        }

        task_info = under_test.TaskInfo.from_task(task_mock, tests_by_suite, evg_conf_mock, variant)

        self.assertIn(suite_name, task_info.resmoke_args)
        for test in test_list:
            self.assertIn(test, task_info.tests)
        self.assertIsNone(task_info.require_multiversion)
        self.assertEqual(distro_name, task_info.distro)

    def test_generated_task_large_distro(self):
        suite_name = "suite_1"
        distro_name = "distro_1"
        large_distro_name = "large_distro_1"
        variant = "build_variant"
        evg_conf_mock = MagicMock()
        task_def_mock = evg_conf_mock.get_task.return_value
        task_def_mock.is_generate_resmoke_task = True
        task_def_mock.generate_resmoke_tasks_command = {"vars": {"use_large_distro": True}}
        evg_conf_mock.get_variant.return_value.raw = {
            "expansions": {
                "large_distro_name": large_distro_name
            }
        }  # yapf: disable
        task_mock = create_variant_task_mock("task 1", suite_name, distro_name)
        test_list = [f"test{i}.js" for i in range(3)]
        tests_by_suite = {
            suite_name: test_list,
            "suite 2": [f"test{i}.js" for i in range(1)],
            "suite 3": [f"test{i}.js" for i in range(2)],
        }

        task_info = under_test.TaskInfo.from_task(task_mock, tests_by_suite, evg_conf_mock, variant)

        self.assertIn(suite_name, task_info.resmoke_args)
        for test in test_list:
            self.assertIn(test, task_info.tests)
        self.assertIsNone(task_info.require_multiversion)
        self.assertEqual(large_distro_name, task_info.distro)


class TestCreateTaskList(unittest.TestCase):
    def test_create_task_list_no_excludes(self):
        variant = "variant name"
        evg_conf_mock = MagicMock()
        evg_conf_mock.get_variant.return_value.tasks = [
            create_variant_task_mock("task 1", "suite 1"),
            create_variant_task_mock("task 2", "suite 2"),
            create_variant_task_mock("task 3", "suite 3"),
        ]
        tests_by_suite = {
            "suite 1": [f"test{i}.js" for i in range(3)],
            "suite 2": [f"test{i}.js" for i in range(1)],
            "suite 3": [f"test{i}.js" for i in range(2)],
        }
        exclude_tasks = []

        task_list = under_test.create_task_list(evg_conf_mock, variant, tests_by_suite,
                                                exclude_tasks)

        self.assertIn("task 1", task_list)
        self.assertIn("task 2", task_list)
        self.assertIn("task 3", task_list)
        self.assertEqual(3, len(task_list))

    def test_create_task_list_has_correct_task_info(self):
        variant = "variant name"
        evg_conf_mock = MagicMock()
        evg_conf_mock.get_variant.return_value.tasks = [
            create_variant_task_mock("task 1", "suite_1", "distro 1"),
        ]
        evg_conf_mock.get_task.return_value.run_on = ["distro 1"]
        tests_by_suite = {
            "suite_1": [f"test{i}.js" for i in range(3)],
        }
        exclude_tasks = []

        task_list = under_test.create_task_list(evg_conf_mock, variant, tests_by_suite,
                                                exclude_tasks)

        self.assertIn("task 1", task_list)
        task_info = task_list["task 1"]
        self.assertIn("suite_1", task_info.resmoke_args)
        for i in range(3):
            self.assertIn(f"test{i}.js", task_info.tests)
        self.assertIsNone(task_info.require_multiversion)
        self.assertEqual("distro 1", task_info.distro)

    def test_create_task_list_with_excludes(self):
        variant = "variant name"
        evg_conf_mock = MagicMock()
        evg_conf_mock.get_variant.return_value.tasks = [
            create_variant_task_mock("task 1", "suite 1"),
            create_variant_task_mock("task 2", "suite 2"),
            create_variant_task_mock("task 3", "suite 3"),
        ]
        tests_by_suite = {
            "suite 1": [f"test{i}.js" for i in range(3)],
            "suite 2": [f"test{i}.js" for i in range(1)],
            "suite 3": [f"test{i}.js" for i in range(2)],
        }
        exclude_tasks = ["task 2"]

        task_list = under_test.create_task_list(evg_conf_mock, variant, tests_by_suite,
                                                exclude_tasks)

        self.assertIn("task 1", task_list)
        self.assertNotIn("task 2", task_list)
        self.assertIn("task 3", task_list)
        self.assertEqual(2, len(task_list))

    def test_create_task_list_no_suites(self):
        variant = "variant name"
        evg_conf_mock = MagicMock()
        suite_dict = {}

        task_list = under_test.create_task_list(evg_conf_mock, variant, suite_dict, [])

        self.assertEqual(task_list, {})

    def test_build_variant_not_in_evg_project_config(self):
        variant = "novariant"
        evg_conf_mock = MagicMock()
        evg_conf_mock.get_variant.return_value = None
        suite_dict = {}

        with self.assertRaises(ValueError):
            under_test.create_task_list(evg_conf_mock, variant, suite_dict, [])


class TestCreateTestsByTask(unittest.TestCase):
    def test_build_variant_not_in_evg_project_config(self):
        variant = "novariant"
        evg_conf_mock = MagicMock()
        evg_conf_mock.get_variant.return_value = None

        with self.assertRaises(ValueError):
            under_test.create_tests_by_task(variant, evg_conf_mock, set())


class TestLocalFileChangeDetector(unittest.TestCase):
    @patch(ns("find_changed_files_in_repos"))
    @patch(ns("os.path.isfile"))
    def test_non_js_files_filtered(self, is_file_mock, changed_files_mock):
        repo_mock = MagicMock()
        file_list = [
            os.path.join("jstests", "test1.js"),
            os.path.join("jstests", "test1.cpp"),
            os.path.join("jstests", "test2.js"),
        ]
        changed_files_mock.return_value = set(file_list)
        is_file_mock.return_value = True

        file_change_detector = under_test.LocalFileChangeDetector(None)
        found_tests = file_change_detector.find_changed_tests([repo_mock])

        self.assertIn(file_list[0], found_tests)
        self.assertIn(file_list[2], found_tests)
        self.assertNotIn(file_list[1], found_tests)

    @patch(ns("find_changed_files_in_repos"))
    @patch(ns("os.path.isfile"))
    def test_missing_files_filtered(self, is_file_mock, changed_files_mock):
        repo_mock = MagicMock()
        file_list = [
            os.path.join("jstests", "test1.js"),
            os.path.join("jstests", "test2.js"),
            os.path.join("jstests", "test3.js"),
        ]
        changed_files_mock.return_value = set(file_list)
        is_file_mock.return_value = False

        file_change_detector = under_test.LocalFileChangeDetector(None)
        found_tests = file_change_detector.find_changed_tests([repo_mock])

        self.assertEqual(0, len(found_tests))

    @patch(ns("find_changed_files_in_repos"))
    @patch(ns("os.path.isfile"))
    def test_non_jstests_files_filtered(self, is_file_mock, changed_files_mock):
        repo_mock = MagicMock()
        file_list = [
            os.path.join("jstests", "test1.js"),
            os.path.join("other", "test2.js"),
            os.path.join("jstests", "test3.js"),
        ]
        changed_files_mock.return_value = set(file_list)
        is_file_mock.return_value = True

        file_change_detector = under_test.LocalFileChangeDetector(None)
        found_tests = file_change_detector.find_changed_tests([repo_mock])

        self.assertIn(file_list[0], found_tests)
        self.assertIn(file_list[2], found_tests)
        self.assertNotIn(file_list[1], found_tests)
        self.assertEqual(2, len(found_tests))

"""Unit tests for buildscripts/burn_in_tests_multiversion.py."""

from __future__ import absolute_import

import json
from datetime import datetime
import os
import sys
import unittest
from mock import MagicMock, patch

import inject
from evergreen import EvergreenApi

import buildscripts.burn_in_tests_multiversion as under_test
from buildscripts.burn_in_tests import TaskInfo
from buildscripts.ciconfig.evergreen import parse_evergreen_file, EvergreenProjectConfig
import buildscripts.resmokelib.parser as _parser
from buildscripts.evergreen_burn_in_tests import EvergreenFileChangeDetector
from buildscripts.task_generation.gen_config import GenerationConfiguration
from buildscripts.task_generation.multiversion_util import REPL_MIXED_VERSION_CONFIGS, \
    SHARDED_MIXED_VERSION_CONFIGS
from buildscripts.task_generation.resmoke_proxy import ResmokeProxyConfig
from buildscripts.task_generation.suite_split import SuiteSplitConfig
from buildscripts.task_generation.suite_split_strategies import greedy_division, SplitStrategy, \
    FallbackStrategy, round_robin_fallback
from buildscripts.task_generation.task_types.gentask_options import GenTaskOptions

_parser.set_run_options()

MONGO_4_2_HASH = "d94888c0d0a8065ca57d354ece33b3c2a1a5a6d6"

# pylint: disable=missing-docstring,invalid-name,unused-argument,no-self-use,protected-access,no-value-for-parameter


def create_tests_by_task_mock(n_tasks, n_tests, multiversion_values=None):
    if multiversion_values is None:
        multiversion_values = [None for _ in range(n_tasks)]
    return {
        f"task_{i}_gen": TaskInfo(
            display_task_name=f"task_{i}",
            resmoke_args=f"--suites=suite_{i}",
            tests=[f"jstests/tests_{j}" for j in range(n_tests)],
            require_multiversion=multiversion_values[i],
            distro="",
        )
        for i in range(n_tasks)
    }


MV_MOCK_SUITES = ["replica_sets_jscore_passthrough", "sharding_jscore_passthrough"]
MV_MOCK_TESTS = {
    "replica_sets_jscore_passthrough": [
        "core/all.js",
        "core/andor.js",
        "core/apitest_db.js",
        "core/auth1.js",
        "core/auth2.js",
    ], "sharding_jscore_passthrough": [
        "core/basic8.js",
        "core/batch_size.js",
        "core/bson.js",
        "core/bulk_insert.js",
        "core/capped.js",
    ]
}


def create_multiversion_tests_by_task_mock(n_tasks, n_tests):
    assert n_tasks <= len(MV_MOCK_SUITES)
    assert n_tests <= len(MV_MOCK_TESTS[MV_MOCK_SUITES[0]])
    return {
        f"{MV_MOCK_SUITES[i]}": TaskInfo(
            display_task_name=f"task_{i}",
            resmoke_args=f"--suites=suite_{i}",
            tests=[f"jstests/{MV_MOCK_TESTS[MV_MOCK_SUITES[i]][j]}" for j in range(n_tests)],
            require_multiversion=None,
            distro="",
        )
        for i in range(n_tasks)
    }


_DATE = datetime(2018, 7, 15)
BURN_IN_TESTS = "buildscripts.burn_in_tests"
NUM_REPL_MIXED_VERSION_CONFIGS = len(REPL_MIXED_VERSION_CONFIGS)
NUM_SHARDED_MIXED_VERSION_CONFIGS = len(SHARDED_MIXED_VERSION_CONFIGS)

NS = "buildscripts.burn_in_tests_multiversion"


def ns(relative_name):  # pylint: disable=invalid-name
    """Return a full name from a name relative to the test module"s name space."""
    return NS + "." + relative_name


def bit_ns(relative_name):
    return BURN_IN_TESTS + "." + relative_name


def mock_a_file(filename):
    change = MagicMock(a_path=filename)
    return change


def mock_git_diff(change_list):
    diff = MagicMock()
    diff.iter_change_type.return_value = change_list
    return diff


def mock_changed_git_files(add_files):
    repo = MagicMock()
    repo.index.diff.return_value = mock_git_diff([mock_a_file(f) for f in add_files])
    return repo


def get_evergreen_config(config_file_path):
    evergreen_home = os.path.expanduser(os.path.join("~", "evergreen"))
    if os.path.exists(evergreen_home):
        return parse_evergreen_file(config_file_path, evergreen_home)
    return parse_evergreen_file(config_file_path)


def create_variant_task_mock(task_name, suite_name, distro="distro"):
    variant_task = MagicMock()
    variant_task.name = task_name
    variant_task.generated_task_name = task_name
    variant_task.resmoke_suite = suite_name
    variant_task.get_vars_suite_name.return_value = suite_name
    variant_task.combined_resmoke_args = f"--suites={suite_name}"
    variant_task.require_multiversion = None
    variant_task.run_on = [distro]
    return variant_task


def build_mock_gen_task_options():
    return GenTaskOptions(
        create_misc_suite=False,
        is_patch=True,
        generated_config_dir=under_test.DEFAULT_CONFIG_DIR,
        use_default_timeouts=False,
    )


def build_mock_split_task_config():
    return SuiteSplitConfig(
        evg_project="my project",
        target_resmoke_time=60,
        max_sub_suites=100,
        max_tests_per_suite=1,
        start_date=datetime.utcnow(),
        end_date=datetime.utcnow(),
        default_to_fallback=True,
    )


def configure_dependencies(evg_api, split_config):
    gen_task_options = build_mock_gen_task_options()

    def dependencies(binder: inject.Binder) -> None:
        binder.bind(SuiteSplitConfig, split_config)
        binder.bind(SplitStrategy, greedy_division)
        binder.bind(FallbackStrategy, round_robin_fallback)
        binder.bind(GenTaskOptions, gen_task_options)
        binder.bind(EvergreenApi, evg_api)
        binder.bind(GenerationConfiguration, GenerationConfiguration.from_yaml_file())
        binder.bind(ResmokeProxyConfig,
                    ResmokeProxyConfig(resmoke_suite_dir=under_test.DEFAULT_TEST_SUITE_DIR))
        binder.bind(EvergreenFileChangeDetector, None)
        binder.bind(EvergreenProjectConfig, MagicMock())
        binder.bind(
            under_test.BurnInConfig,
            under_test.BurnInConfig(build_id="build_id", build_variant="build variant",
                                    revision="revision"))

    inject.clear_and_configure(dependencies)


class TestCreateMultiversionGenerateTasksConfig(unittest.TestCase):
    def tests_no_tasks_given(self):
        target_file = "target_file.json"
        mock_evg_api = MagicMock()
        split_config = build_mock_split_task_config()
        configure_dependencies(mock_evg_api, split_config)

        orchestrator = under_test.MultiversionBurnInOrchestrator()
        generated_config = orchestrator.generate_configuration({}, target_file, "build_variant")

        evg_config = [
            config for config in generated_config.file_list if config.file_name == target_file
        ]
        self.assertEqual(1, len(evg_config))
        evg_config = evg_config[0]
        evg_config_dict = json.loads(evg_config.content)

        self.assertEqual(0, len(evg_config_dict["tasks"]))

    def test_tasks_not_in_multiversion_suites(self):
        n_tasks = 1
        n_tests = 1
        target_file = "target_file.json"
        mock_evg_api = MagicMock()
        split_config = build_mock_split_task_config()
        configure_dependencies(mock_evg_api, split_config)
        tests_by_task = create_tests_by_task_mock(n_tasks, n_tests)

        orchestrator = under_test.MultiversionBurnInOrchestrator()
        generated_config = orchestrator.generate_configuration(tests_by_task, target_file,
                                                               "build_variant")

        evg_config = [
            config for config in generated_config.file_list if config.file_name == target_file
        ]
        self.assertEqual(1, len(evg_config))
        evg_config = evg_config[0]
        evg_config_dict = json.loads(evg_config.content)

        self.assertEqual(0, len(evg_config_dict["tasks"]))

    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    @patch(
        "buildscripts.resmokelib.run.generate_multiversion_exclude_tags.get_backports_required_hash_for_shell_version"
    )
    def test_one_task_one_test(self, mock_hash):
        mock_hash.return_value = MONGO_4_2_HASH
        n_tasks = 1
        n_tests = 1
        target_file = "target_file.json"
        mock_evg_api = MagicMock()
        split_config = build_mock_split_task_config()
        configure_dependencies(mock_evg_api, split_config)
        tests_by_task = create_multiversion_tests_by_task_mock(n_tasks, n_tests)

        orchestrator = under_test.MultiversionBurnInOrchestrator()
        generated_config = orchestrator.generate_configuration(tests_by_task, target_file,
                                                               "build_variant")

        evg_config = [
            config for config in generated_config.file_list if config.file_name == target_file
        ]
        self.assertEqual(1, len(evg_config))
        evg_config = evg_config[0]
        evg_config_dict = json.loads(evg_config.content)
        tasks = evg_config_dict["tasks"]
        self.assertEqual(len(tasks), NUM_REPL_MIXED_VERSION_CONFIGS * n_tests)

    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    @patch(
        "buildscripts.resmokelib.run.generate_multiversion_exclude_tags.get_backports_required_hash_for_shell_version"
    )
    def test_n_task_one_test(self, mock_hash):
        mock_hash.return_value = MONGO_4_2_HASH
        n_tasks = 2
        n_tests = 1
        target_file = "target_file.json"
        mock_evg_api = MagicMock()
        split_config = build_mock_split_task_config()
        configure_dependencies(mock_evg_api, split_config)
        tests_by_task = create_multiversion_tests_by_task_mock(n_tasks, n_tests)

        orchestrator = under_test.MultiversionBurnInOrchestrator()
        generated_config = orchestrator.generate_configuration(tests_by_task, target_file,
                                                               "build_variant")

        evg_config = [
            config for config in generated_config.file_list if config.file_name == target_file
        ]
        self.assertEqual(1, len(evg_config))
        evg_config = evg_config[0]
        evg_config_dict = json.loads(evg_config.content)
        tasks = evg_config_dict["tasks"]
        self.assertEqual(
            len(tasks),
            (NUM_REPL_MIXED_VERSION_CONFIGS + NUM_SHARDED_MIXED_VERSION_CONFIGS) * n_tests)

    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    @patch(
        "buildscripts.resmokelib.run.generate_multiversion_exclude_tags.get_backports_required_hash_for_shell_version"
    )
    def test_one_task_n_test(self, mock_hash):
        mock_hash.return_value = MONGO_4_2_HASH
        n_tasks = 1
        n_tests = 2
        target_file = "target_file.json"
        mock_evg_api = MagicMock()
        split_config = build_mock_split_task_config()
        configure_dependencies(mock_evg_api, split_config)
        tests_by_task = create_multiversion_tests_by_task_mock(n_tasks, n_tests)

        orchestrator = under_test.MultiversionBurnInOrchestrator()
        generated_config = orchestrator.generate_configuration(tests_by_task, target_file,
                                                               "build_variant")

        evg_config = [
            config for config in generated_config.file_list if config.file_name == target_file
        ]
        self.assertEqual(1, len(evg_config))
        evg_config = evg_config[0]
        evg_config_dict = json.loads(evg_config.content)
        tasks = evg_config_dict["tasks"]
        self.assertEqual(len(tasks), NUM_REPL_MIXED_VERSION_CONFIGS * n_tests)

    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    @patch(
        "buildscripts.resmokelib.run.generate_multiversion_exclude_tags.get_backports_required_hash_for_shell_version"
    )
    def test_n_task_m_test(self, mock_hash):
        mock_hash.return_value = MONGO_4_2_HASH
        n_tasks = 2
        n_tests = 3
        target_file = "target_file.json"
        mock_evg_api = MagicMock()
        split_config = build_mock_split_task_config()
        configure_dependencies(mock_evg_api, split_config)
        tests_by_task = create_multiversion_tests_by_task_mock(n_tasks, n_tests)

        orchestrator = under_test.MultiversionBurnInOrchestrator()
        generated_config = orchestrator.generate_configuration(tests_by_task, target_file,
                                                               "build_variant")

        evg_config = [
            config for config in generated_config.file_list if config.file_name == target_file
        ]
        self.assertEqual(1, len(evg_config))
        evg_config = evg_config[0]
        evg_config_dict = json.loads(evg_config.content)
        tasks = evg_config_dict["tasks"]
        self.assertEqual(
            len(tasks),
            (NUM_REPL_MIXED_VERSION_CONFIGS + NUM_SHARDED_MIXED_VERSION_CONFIGS) * n_tests)


class TestGenerateConfig(unittest.TestCase):
    def test_validate_multiversion(self):
        evg_conf_mock = MagicMock()
        gen_config = under_test.GenerateConfig("build_variant", "project")
        gen_config.validate(evg_conf_mock)


class TestGatherTaskInfo(unittest.TestCase):
    def test_multiversion_task(self):
        suite_name = "suite_1"
        distro_name = "distro_1"
        variant = "build_variant"
        evg_conf_mock = MagicMock()
        evg_conf_mock.get_task.return_value.is_generate_resmoke_task = False
        task_mock = create_variant_task_mock("task 1", suite_name, distro_name)
        task_mock.require_multiversion = True
        test_list = [f"test{i}.js" for i in range(3)]
        tests_by_suite = {
            suite_name: test_list,
            "suite 2": [f"test{i}.js" for i in range(1)],
            "suite 3": [f"test{i}.js" for i in range(2)],
        }

        task_info = TaskInfo.from_task(task_mock, tests_by_suite, evg_conf_mock, variant)

        self.assertIn(suite_name, task_info.resmoke_args)
        for test in test_list:
            self.assertIn(test, task_info.tests)
        self.assertEqual(task_mock.require_multiversion, task_info.require_multiversion)
        self.assertEqual(distro_name, task_info.distro)

"""Unit tests for the selected_tests script."""
import json
import os
import unittest

from buildscripts import errorcodes

# Debugging
errorcodes.list_files = True

TESTDATA_DIR = './buildscripts/tests/data/errorcodes/'


class TestErrorcodes(unittest.TestCase):
    """Test errorcodes.py."""

    def setUp(self):
        # errorcodes.py keeps some global state.
        errorcodes.codes = []

    def test_regex_matching(self):
        """Test regex matching."""
        captured_error_codes = []

        def accumulate_files(code):
            captured_error_codes.append(code)

        errorcodes.parse_source_files(accumulate_files, TESTDATA_DIR + 'regex_matching/')
        self.assertEqual(26, len(captured_error_codes))

    def test_dup_checking(self):
        """Test dup checking."""
        assertions, errors, _ = errorcodes.read_error_codes(TESTDATA_DIR + 'dup_checking/')
        # `assertions` is every use of an error code. Duplicates are included.
        self.assertEqual(4, len(assertions))
        self.assertEqual([1, 2, 3, 2], list(map(lambda x: int(x.code), assertions)))

        # All assertions with the same error code are considered `errors`.
        self.assertEqual(2, len(errors))
        self.assertEqual(2, int(errors[0].code))
        self.assertEqual(2, int(errors[1].code))

    def test_generate_next_code(self):
        """Test `get_next_code`."""
        _, _, seen = errorcodes.read_error_codes(TESTDATA_DIR + 'generate_next_code/')
        generator = errorcodes.get_next_code(seen)
        self.assertEqual(21, next(generator))
        self.assertEqual(22, next(generator))

    def test_generate_next_server_code(self):
        """ This call to `read_error_codes` technically has no bearing on `get_next_code` when a
        `server_ticket` is passed in. But it maybe makes sense for the test to do so in case a
        future patch changes that relationship."""
        _, _, seen = errorcodes.read_error_codes(TESTDATA_DIR + 'generate_next_server_code/')
        print("Seen: " + str(seen))
        generator = errorcodes.get_next_code(seen, server_ticket=12301)
        self.assertEqual(1230101, next(generator))
        self.assertEqual(1230103, next(generator))

    def test_ticket_coersion(self):
        """Test `coerce_to_number`."""
        self.assertEqual(0, errorcodes.coerce_to_number(0))
        self.assertEqual(1234, errorcodes.coerce_to_number('1234'))
        self.assertEqual(1234, errorcodes.coerce_to_number('server-1234'))
        self.assertEqual(1234, errorcodes.coerce_to_number('SERVER-1234'))
        self.assertEqual(-1, errorcodes.coerce_to_number('not a ticket'))

"""Unit tests for the generate_resmoke_suite script."""
import unittest

from mock import MagicMock

from buildscripts import evergreen_activate_gen_tasks as under_test

# pylint: disable=missing-docstring,invalid-name,unused-argument,no-self-use,protected-access
# pylint: disable=too-many-locals,too-many-lines,too-many-public-methods,no-value-for-parameter


def build_mock_task(name, task_id):
    mock_task = MagicMock(display_name=name, task_id=task_id)
    return mock_task


def build_mock_evg_api(mock_task_list):
    mock_build = MagicMock()
    mock_build.get_tasks.return_value = mock_task_list
    mock_evg_api = MagicMock()
    mock_evg_api.build_by_id.return_value = mock_build
    return mock_evg_api


class TestActivateTask(unittest.TestCase):
    def test_task_with_display_name_is_activated(self):
        n_tasks = 5
        mock_task_list = [build_mock_task(f"task_{i}", f"id_{i}") for i in range(n_tasks)]
        mock_evg_api = build_mock_evg_api(mock_task_list)

        under_test.activate_task("build_id", "task_3", mock_evg_api)

        mock_evg_api.configure_task.assert_called_with("id_3", activated=True)

    def test_task_with_no_matching_name(self):
        n_tasks = 5
        mock_task_list = [build_mock_task(f"task_{i}", f"id_{i}") for i in range(n_tasks)]
        mock_evg_api = build_mock_evg_api(mock_task_list)

        under_test.activate_task("build_id", "not_an_existing_task", mock_evg_api)

        mock_evg_api.configure_task.assert_not_called()

"""Unit tests for buildscripts/burn_in_tests.py."""

from __future__ import absolute_import

import json
import os
import sys
import unittest
from datetime import datetime, timedelta
from math import ceil

import requests
from mock import patch, MagicMock
from shrub.v2 import BuildVariant, ShrubProject

import buildscripts.evergreen_burn_in_tests as under_test
from buildscripts.ciconfig.evergreen import parse_evergreen_file
import buildscripts.resmokelib.parser as _parser
import buildscripts.resmokelib.config as _config
import buildscripts.util.teststats as teststats_utils
_parser.set_run_options()

# pylint: disable=missing-docstring,invalid-name,unused-argument,no-self-use,protected-access

NS = "buildscripts.evergreen_burn_in_tests"


def ns(relative_name):  # pylint: disable=invalid-name
    """Return a full name from a name relative to the test module"s name space."""
    return NS + "." + relative_name


def mock_a_file(filename):
    change = MagicMock(a_path=filename)
    return change


def mock_git_diff(change_list):
    diff = MagicMock()
    diff.iter_change_type.return_value = change_list
    return diff


def mock_changed_git_files(add_files):
    repo = MagicMock()
    repo.index.diff.return_value = mock_git_diff([mock_a_file(f) for f in add_files])
    repo.working_dir = "."
    return repo


def get_evergreen_config(config_file_path):
    evergreen_home = os.path.expanduser(os.path.join("~", "evergreen"))
    if os.path.exists(evergreen_home):
        return parse_evergreen_file(config_file_path, evergreen_home)
    return parse_evergreen_file(config_file_path)


class TestAcceptance(unittest.TestCase):
    def tearDown(self):
        _parser.set_run_options()

    @patch(ns("write_file"))
    def test_no_tests_run_if_none_changed(self, write_json_mock):
        """
        Given a git repository with no changes,
        When burn_in_tests is run,
        Then no tests are discovered to run.
        """
        variant = "build_variant"
        repos = [mock_changed_git_files([])]
        repeat_config = under_test.RepeatConfig()
        gen_config = under_test.GenerateConfig(
            variant,
            "project",
        )  # yapf: disable
        mock_evg_conf = MagicMock()
        mock_evg_conf.get_task_names_by_tag.return_value = set()
        mock_evg_api = MagicMock()

        under_test.burn_in("task_id", variant, gen_config, repeat_config, mock_evg_api,
                           mock_evg_conf, repos, "testfile.json")

        write_json_mock.assert_called_once()
        written_config = json.loads(write_json_mock.call_args[0][1])
        display_task = written_config["buildvariants"][0]["display_tasks"][0]
        self.assertEqual(1, len(display_task["execution_tasks"]))
        self.assertEqual(under_test.BURN_IN_TESTS_GEN_TASK, display_task["execution_tasks"][0])

    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    @patch(ns("write_file"))
    def test_tests_generated_if_a_file_changed(self, write_json_mock):
        """
        Given a git repository with changes,
        When burn_in_tests is run,
        Then tests are discovered to run.
        """
        # Note: this test is using actual tests and suites. So changes to those suites could
        # introduce failures and require this test to be updated.
        # You can see the test file it is using below. This test is used in the 'auth' and
        # 'auth_audit' test suites. It needs to be in at least one of those for the test to pass.
        _config.NAMED_SUITES = None
        variant = "enterprise-rhel-80-64-bit"
        repos = [mock_changed_git_files(["jstests/auth/auth1.js"])]
        repeat_config = under_test.RepeatConfig()
        gen_config = under_test.GenerateConfig(
            variant,
            "project",
        )  # yapf: disable
        mock_evg_conf = get_evergreen_config("etc/evergreen.yml")
        mock_evg_api = MagicMock()

        under_test.burn_in("task_id", variant, gen_config, repeat_config, mock_evg_api,
                           mock_evg_conf, repos, "testfile.json")

        write_json_mock.assert_called_once()
        written_config = json.loads(write_json_mock.call_args[0][1])
        n_tasks = len(written_config["tasks"])
        # Ensure we are generating at least one task for the test.
        self.assertGreaterEqual(n_tasks, 1)

        written_build_variant = written_config["buildvariants"][0]
        self.assertEqual(variant, written_build_variant["name"])
        self.assertEqual(n_tasks, len(written_build_variant["tasks"]))

        display_task = written_build_variant["display_tasks"][0]
        # The display task should contain all the generated tasks as well as 1 extra task for
        # the burn_in_test_gen task.
        self.assertEqual(n_tasks + 1, len(display_task["execution_tasks"]))


class TestGenerateConfig(unittest.TestCase):
    def test_run_build_variant_with_no_run_build_variant(self):
        gen_config = under_test.GenerateConfig("build_variant", "project")

        self.assertEqual(gen_config.build_variant, gen_config.run_build_variant)

    def test_run_build_variant_with_run_build_variant(self):
        gen_config = under_test.GenerateConfig("build_variant", "project", "run_build_variant")

        self.assertNotEqual(gen_config.build_variant, gen_config.run_build_variant)
        self.assertEqual(gen_config.run_build_variant, "run_build_variant")

    def test_validate_non_existing_build_variant(self):
        evg_conf_mock = MagicMock()
        evg_conf_mock.get_variant.return_value = None

        gen_config = under_test.GenerateConfig("build_variant", "project", "run_build_variant")

        with self.assertRaises(ValueError):
            gen_config.validate(evg_conf_mock)

    def test_validate_existing_build_variant(self):
        evg_conf_mock = MagicMock()

        gen_config = under_test.GenerateConfig("build_variant", "project", "run_build_variant")
        gen_config.validate(evg_conf_mock)

    def test_validate_non_existing_run_build_variant(self):
        evg_conf_mock = MagicMock()

        gen_config = under_test.GenerateConfig("build_variant", "project")
        gen_config.validate(evg_conf_mock)


class TestParseAvgTestRuntime(unittest.TestCase):
    def test__parse_avg_test_runtime(self):
        task_avg_test_runtime_stats = [
            teststats_utils.TestRuntime(test_name="dir/test1.js", runtime=30.2),
            teststats_utils.TestRuntime(test_name="dir/test2.js", runtime=455.1)
        ]
        result = under_test._parse_avg_test_runtime("dir/test2.js", task_avg_test_runtime_stats)
        self.assertEqual(result, 455.1)


class TestCalculateTimeout(unittest.TestCase):
    def test__calculate_timeout(self):
        avg_test_runtime = 455.1
        expected_result = ceil(avg_test_runtime * under_test.AVG_TEST_TIME_MULTIPLIER)
        self.assertEqual(expected_result, under_test._calculate_timeout(avg_test_runtime))

    def test__calculate_timeout_avg_is_less_than_min(self):
        avg_test_runtime = 10
        self.assertEqual(under_test.MIN_AVG_TEST_TIME_SEC,
                         under_test._calculate_timeout(avg_test_runtime))


class TestCalculateExecTimeout(unittest.TestCase):
    def test__calculate_exec_timeout(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_secs=600)
        avg_test_runtime = 455.1

        exec_timeout = under_test._calculate_exec_timeout(repeat_config, avg_test_runtime)

        self.assertEqual(1771, exec_timeout)

    def test_average_timeout_greater_than_execution_time(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_secs=600, repeat_tests_min=2)
        avg_test_runtime = 750

        exec_timeout = under_test._calculate_exec_timeout(repeat_config, avg_test_runtime)

        # The timeout needs to be greater than the number of the test * the minimum number of runs.
        minimum_expected_timeout = avg_test_runtime * repeat_config.repeat_tests_min

        self.assertGreater(exec_timeout, minimum_expected_timeout)


class TestGenerateTimeouts(unittest.TestCase):
    def test__generate_timeouts(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_secs=600)
        runtime_stats = [teststats_utils.TestRuntime(test_name="dir/test2.js", runtime=455.1)]
        test_name = "dir/test2.js"

        task_generator = under_test.TaskGenerator(MagicMock(), repeat_config, MagicMock(),
                                                  runtime_stats)
        timeout_info = task_generator.generate_timeouts(test_name)

        self.assertEqual(timeout_info.exec_timeout, 1771)
        self.assertEqual(timeout_info.timeout, 1366)

    def test__generate_timeouts_no_results(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_secs=600)
        runtime_stats = []
        test_name = "dir/new_test.js"

        task_generator = under_test.TaskGenerator(MagicMock(), repeat_config, MagicMock(),
                                                  runtime_stats)
        timeout_info = task_generator.generate_timeouts(test_name)

        self.assertIsNone(timeout_info.cmd)

    def test__generate_timeouts_avg_runtime_is_zero(self):
        repeat_config = under_test.RepeatConfig(repeat_tests_secs=600)
        runtime_stats = [
            teststats_utils.TestRuntime(test_name="dir/test_with_zero_runtime.js", runtime=0)
        ]
        test_name = "dir/test_with_zero_runtime.js"

        task_generator = under_test.TaskGenerator(MagicMock(), repeat_config, MagicMock(),
                                                  runtime_stats)
        timeout_info = task_generator.generate_timeouts(test_name)

        self.assertIsNone(timeout_info.cmd)


class TestGetTaskRuntimeHistory(unittest.TestCase):
    def test_get_task_runtime_history(self):
        mock_evg_api = MagicMock()
        mock_evg_api.test_stats_by_project.return_value = [
            MagicMock(
                test_file="dir/test2.js",
                task_name="task1",
                variant="variant1",
                distro="distro1",
                date=datetime.utcnow().date(),
                num_pass=1,
                num_fail=0,
                avg_duration_pass=10.1,
            )
        ]
        analysis_duration = under_test.AVG_TEST_RUNTIME_ANALYSIS_DAYS
        end_date = datetime.utcnow().replace(microsecond=0)
        start_date = end_date - timedelta(days=analysis_duration)
        mock_gen_config = MagicMock(project="project1", build_variant="variant1")

        executor = under_test.GenerateBurnInExecutor(mock_gen_config, MagicMock(), mock_evg_api,
                                                     history_end_date=end_date)
        result = executor.get_task_runtime_history("task1")

        self.assertEqual(result, [("dir/test2.js", 10.1)])
        mock_evg_api.test_stats_by_project.assert_called_with(
            "project1", after_date=start_date, before_date=end_date, group_by="test",
            group_num_days=14, tasks=["task1"], variants=["variant1"])

    def test_get_task_runtime_history_evg_degraded_mode_error(self):
        mock_response = MagicMock(status_code=requests.codes.SERVICE_UNAVAILABLE)
        mock_evg_api = MagicMock()
        mock_evg_api.test_stats_by_project.side_effect = requests.HTTPError(response=mock_response)
        mock_gen_config = MagicMock(project="project1", build_variant="variant1")

        executor = under_test.GenerateBurnInExecutor(mock_gen_config, MagicMock(), mock_evg_api)
        result = executor.get_task_runtime_history("task1")

        self.assertEqual(result, [])


TESTS_BY_TASK = {
    "task1": {
        "resmoke_args": "--suites=suite1",
        "tests": ["jstests/test1.js", "jstests/test2.js"]},
    "task2": {
        "resmoke_args": "--suites=suite1",
        "tests": ["jstests/test1.js", "jstests/test3.js"]},
    "task3": {
        "resmoke_args": "--suites=suite3",
        "tests": ["jstests/test4.js", "jstests/test5.js"]},
    "task4": {
        "resmoke_args": "--suites=suite4", "tests": []},
}  # yapf: disable


def create_tests_by_task_mock(n_tasks, n_tests):
    return {
        f"task_{i}_gen":
        under_test.TaskInfo(display_task_name=f"task_{i}", resmoke_args=f"--suites=suite_{i}",
                            tests=[f"jstests/tests_{j}" for j in range(n_tests)],
                            require_multiversion=None, distro=f"distro_{i}")
        for i in range(n_tasks)
    }


class TestCreateGenerateTasksConfig(unittest.TestCase):
    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    def test_no_tasks_given(self):
        build_variant = BuildVariant("build variant")
        gen_config = MagicMock(run_build_variant="variant")
        repeat_config = MagicMock()
        mock_evg_api = MagicMock()

        executor = under_test.GenerateBurnInExecutor(gen_config, repeat_config, mock_evg_api)
        executor.add_config_for_build_variant(build_variant, {})

        evg_config_dict = build_variant.as_dict()
        self.assertEqual(0, len(evg_config_dict["tasks"]))

    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    def test_one_task_one_test(self):
        n_tasks = 1
        n_tests = 1
        resmoke_options = "options for resmoke"
        build_variant = BuildVariant("build variant")
        gen_config = MagicMock(run_build_variant="variant", distro=None)
        repeat_config = MagicMock()
        repeat_config.generate_resmoke_options.return_value = resmoke_options
        mock_evg_api = MagicMock()
        tests_by_task = create_tests_by_task_mock(n_tasks, n_tests)

        executor = under_test.GenerateBurnInExecutor(gen_config, repeat_config, mock_evg_api)
        executor.add_config_for_build_variant(build_variant, tests_by_task)

        shrub_config = ShrubProject.empty().add_build_variant(build_variant)
        evg_config_dict = shrub_config.as_dict()
        tasks = evg_config_dict["tasks"]
        self.assertEqual(n_tasks * n_tests, len(tasks))
        cmd = tasks[0]["commands"]
        self.assertIn(resmoke_options, cmd[1]["vars"]["resmoke_args"])
        self.assertIn("--suites=suite_0", cmd[1]["vars"]["resmoke_args"])
        self.assertIn("tests_0", cmd[1]["vars"]["resmoke_args"])

    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    def test_n_task_m_test(self):
        n_tasks = 3
        n_tests = 5
        build_variant = BuildVariant("build variant")
        gen_config = MagicMock(run_build_variant="variant", distro=None)
        repeat_config = MagicMock()
        tests_by_task = create_tests_by_task_mock(n_tasks, n_tests)
        mock_evg_api = MagicMock()

        executor = under_test.GenerateBurnInExecutor(gen_config, repeat_config, mock_evg_api)
        executor.add_config_for_build_variant(build_variant, tests_by_task)

        evg_config_dict = build_variant.as_dict()
        self.assertEqual(n_tasks * n_tests, len(evg_config_dict["tasks"]))


class TestCreateGenerateTasksFile(unittest.TestCase):
    @unittest.skipIf(sys.platform.startswith("win"), "not supported on windows")
    @patch(ns("sys.exit"))
    @patch(ns("validate_task_generation_limit"))
    def test_cap_on_task_generate(self, validate_mock, exit_mock):
        gen_config = MagicMock(require_multiversion=False)
        repeat_config = MagicMock()
        tests_by_task = MagicMock()
        mock_evg_api = MagicMock()

        validate_mock.return_value = False

        exit_mock.side_effect = ValueError("exiting")
        with self.assertRaises(ValueError):
            executor = under_test.GenerateBurnInExecutor(gen_config, repeat_config, mock_evg_api,
                                                         "gen_file.json")
            executor.execute(tests_by_task)

        exit_mock.assert_called_once()

"""Unit tests for the generate_resmoke_suite script."""
import unittest

from mock import MagicMock

from buildscripts import evergreen_gen_build_variant as under_test
from buildscripts.ciconfig.evergreen import Variant, Task

# pylint: disable=missing-docstring,invalid-name,unused-argument,no-self-use,protected-access
# pylint: disable=too-many-locals,too-many-lines,too-many-public-methods,no-value-for-parameter


def build_mock_build_variant(expansions=None, task_list=None):
    task_spec_list = [{"name": task.name} for task in task_list] if task_list else []
    config = {
        "tasks": task_spec_list,
    }
    if expansions:
        config["expansions"] = expansions

    if task_list is None:
        task_list = []
    task_map = {task.name: task for task in task_list}

    return Variant(config, task_map, {})


def build_mock_task(name, run_vars=None):
    config = {
        "name":
            name, "commands": [
                {"func": "do setup"},
                {
                    "func": "generate resmoke tasks",
                    "vars": run_vars if run_vars else {},
                },
            ]
    }
    return Task(config)


def build_mock_project_config(variant=None, task_defs=None):
    mock_project = MagicMock()
    if variant:
        mock_project.get_variant.return_value = variant

    if task_defs:
        mock_project.get_task.side_effect = task_defs

    return mock_project


def build_mock_expansions():
    mock_expansions = MagicMock()
    mock_expansions.config_location.return_value = "/path/to/config"
    return mock_expansions


def build_mock_evg_api(build_task_list):
    mock_evg_api = MagicMock()
    mock_evg_api.build_by_id.return_value.get_tasks.return_value = build_task_list
    return mock_evg_api


def build_mock_orchestrator(build_expansions=None, task_def_list=None, build_task_list=None):
    if build_expansions is None:
        build_expansions = {}
    if task_def_list is None:
        task_def_list = []
    if build_task_list is None:
        build_task_list = []

    mock_build_variant = build_mock_build_variant(build_expansions, task_def_list)
    mock_project = build_mock_project_config(mock_build_variant, task_def_list)
    mock_evg_expansions = build_mock_expansions()
    mock_evg_api = build_mock_evg_api(build_task_list)

    return under_test.GenerateBuildVariantOrchestrator(
        gen_task_validation=MagicMock(),
        gen_task_options=MagicMock(),
        evg_project_config=mock_project,
        evg_expansions=mock_evg_expansions,
        multiversion_util=MagicMock(),
        evg_api=mock_evg_api,
    )


class TestTranslateRunVar(unittest.TestCase):
    def test_normal_value_should_be_returned(self):
        run_var = "some value"
        mock_build_variant = build_mock_build_variant()
        self.assertEqual(run_var, under_test.translate_run_var(run_var, mock_build_variant))

    def test_expansion_should_be_returned_from_build_variant(self):
        run_var = "${my_expansion}"
        value = "my value"
        mock_build_variant = build_mock_build_variant(expansions={"my_expansion": value})
        self.assertEqual(value, under_test.translate_run_var(run_var, mock_build_variant))

    def test_expansion_not_found_should_return_none(self):
        run_var = "${my_expansion}"
        mock_build_variant = build_mock_build_variant(expansions={})
        self.assertIsNone(under_test.translate_run_var(run_var, mock_build_variant))

    def test_expansion_not_found_should_return_default(self):
        run_var = "${my_expansion|default}"
        mock_build_variant = build_mock_build_variant(expansions={})
        self.assertEqual("default", under_test.translate_run_var(run_var, mock_build_variant))

    def test_expansion_should_be_returned_from_build_variant_even_with_default(self):
        run_var = "${my_expansion|default}"
        value = "my value"
        mock_build_variant = build_mock_build_variant(expansions={"my_expansion": value})
        self.assertEqual(value, under_test.translate_run_var(run_var, mock_build_variant))


class TestTaskDefToSplitParams(unittest.TestCase):
    def test_params_should_be_generated(self):
        run_vars = {
            "resmoke_args": "run tests",
        }
        mock_task_def = build_mock_task("my_task", run_vars)
        mock_orchestrator = build_mock_orchestrator(task_def_list=[mock_task_def])

        split_param = mock_orchestrator.task_def_to_split_params(mock_task_def, "build_variant")

        self.assertEqual("build_variant", split_param.build_variant)
        self.assertEqual("my_task", split_param.task_name)
        self.assertEqual("my_task", split_param.suite_name)
        self.assertEqual("my_task", split_param.filename)

    def test_params_should_allow_suite_to_be_overridden(self):
        run_vars = {
            "resmoke_args": "run tests",
            "suite": "the suite",
        }
        mock_task_def = build_mock_task("my_task", run_vars)
        mock_orchestrator = build_mock_orchestrator(task_def_list=[mock_task_def])

        split_param = mock_orchestrator.task_def_to_split_params(mock_task_def, "build_variant")

        self.assertEqual("build_variant", split_param.build_variant)
        self.assertEqual("my_task", split_param.task_name)
        self.assertEqual("the suite", split_param.suite_name)
        self.assertEqual("the suite", split_param.filename)


class TestTaskDefToGenParams(unittest.TestCase):
    def test_params_should_be_generated(self):
        run_vars = {
            "resmoke_args": "run tests",
        }
        mock_task_def = build_mock_task("my_task", run_vars)
        mock_orchestrator = build_mock_orchestrator(task_def_list=[mock_task_def])

        gen_params = mock_orchestrator.task_def_to_gen_params(mock_task_def, "build_variant")

        self.assertIsNone(gen_params.require_multiversion)
        self.assertEqual("run tests", gen_params.resmoke_args)
        self.assertEqual(mock_orchestrator.evg_expansions.config_location.return_value,
                         gen_params.config_location)
        self.assertIsNone(gen_params.large_distro_name)
        self.assertFalse(gen_params.use_large_distro)

    def test_params_should_be_overwritable(self):
        run_vars = {
            "resmoke_args": "run tests",
            "use_large_distro": "true",
            "require_multiversion": True,
        }
        mock_task_def = build_mock_task("my_task", run_vars)
        build_expansions = {"large_distro_name": "my large distro"}
        mock_orchestrator = build_mock_orchestrator(build_expansions=build_expansions,
                                                    task_def_list=[mock_task_def])
        gen_params = mock_orchestrator.task_def_to_gen_params(mock_task_def, "build_variant")

        self.assertTrue(gen_params.require_multiversion)
        self.assertEqual("run tests", gen_params.resmoke_args)
        self.assertEqual(mock_orchestrator.evg_expansions.config_location.return_value,
                         gen_params.config_location)
        self.assertEqual("my large distro", gen_params.large_distro_name)
        self.assertTrue(gen_params.use_large_distro)


class TestTaskDefToFuzzerParams(unittest.TestCase):
    def test_params_should_be_generated(self):
        run_vars = {
            "name": "my_fuzzer",
            "num_files": "5",
            "num_tasks": "3",
        }
        mock_task_def = build_mock_task("my_task", run_vars)
        mock_orchestrator = build_mock_orchestrator(task_def_list=[mock_task_def])
        fuzzer_params = mock_orchestrator.task_def_to_fuzzer_params(mock_task_def, "build_variant")

        self.assertEqual("my_fuzzer", fuzzer_params.task_name)
        self.assertEqual(5, fuzzer_params.num_files)
        self.assertEqual(3, fuzzer_params.num_tasks)
        self.assertEqual("jstestfuzz", fuzzer_params.npm_command)
        self.assertEqual(mock_orchestrator.evg_expansions.config_location.return_value,
                         fuzzer_params.config_location)
        self.assertIsNone(fuzzer_params.large_distro_name)
        self.assertFalse(fuzzer_params.use_large_distro)

    def test_params_should_be_overwritable(self):
        run_vars = {
            "name": "my_fuzzer",
            "num_files": "${file_count|8}",
            "num_tasks": "3",
            "use_large_distro": "true",
            "npm_command": "aggfuzzer",
        }
        mock_task_def = build_mock_task("my_task", run_vars)
        build_expansions = {"large_distro_name": "my large distro"}
        mock_orchestrator = build_mock_orchestrator(build_expansions=build_expansions,
                                                    task_def_list=[mock_task_def])

        fuzzer_params = mock_orchestrator.task_def_to_fuzzer_params(mock_task_def, "build_variant")

        self.assertEqual("my_fuzzer", fuzzer_params.task_name)
        self.assertEqual(8, fuzzer_params.num_files)
        self.assertEqual(3, fuzzer_params.num_tasks)
        self.assertEqual("aggfuzzer", fuzzer_params.npm_command)
        self.assertEqual(mock_orchestrator.evg_expansions.config_location.return_value,
                         fuzzer_params.config_location)
        self.assertEqual("my large distro", fuzzer_params.large_distro_name)
        self.assertTrue(fuzzer_params.use_large_distro)


class TestGenerateBuildVariant(unittest.TestCase):
    def test_a_whole_build_variant(self):
        gen_run_vars = {
            "resmoke_args": "run tests",
        }
        mv_gen_run_vars = {
            "resmoke_args": "run tests",
            "suite": "multiversion suite",
            "implicit_multiversion": "True",
        }
        fuzz_run_vars = {
            "name": "my_fuzzer",
            "num_files": "5",
            "num_tasks": "3",
            "is_jstestfuzz": "True",
        }
        mv_fuzz_run_vars = {
            "name": "my_fuzzer",
            "num_files": "5",
            "num_tasks": "3",
            "is_jstestfuzz": "True",
            "suite": "aggfuzzer",
            "implicit_multiversion": "True",
        }
        mock_task_defs = [
            build_mock_task("my_gen_task", gen_run_vars),
            build_mock_task("my_fuzzer_task", fuzz_run_vars),
            build_mock_task("my_mv_fuzzer_task", mv_fuzz_run_vars),
            build_mock_task("my_mv_gen_task", mv_gen_run_vars),
        ]
        mock_orchestrator = build_mock_orchestrator(task_def_list=mock_task_defs)
        builder = MagicMock()

        builder = mock_orchestrator.generate_build_variant(builder, "build variant")

        self.assertEqual(builder.generate_suite.call_count, 1)
        self.assertEqual(builder.generate_fuzzer.call_count, 2)
        self.assertEqual(builder.add_multiversion_suite.call_count, 1)


class TestAdjustTaskPriority(unittest.TestCase):
    def test_task_is_updates(self):
        starting_priority = 42
        task_id = "task 314"
        mock_task = MagicMock(task_id=task_id, priority=starting_priority)
        mock_orchestrator = build_mock_orchestrator()

        mock_orchestrator.adjust_task_priority(mock_task)

        mock_orchestrator.evg_api.configure_task.assert_called_with(task_id,
                                                                    priority=starting_priority + 1)

    def test_task_should_only_reach_99(self):
        starting_priority = 99
        task_id = "task 314"
        mock_task = MagicMock(task_id=task_id, priority=starting_priority)
        mock_orchestrator = build_mock_orchestrator()

        mock_orchestrator.adjust_task_priority(mock_task)

        mock_orchestrator.evg_api.configure_task.assert_called_with(task_id,
                                                                    priority=starting_priority)


class TestAdjustGenTasksPriority(unittest.TestCase):
    def test_gen_tasks_in_task_list_are_adjusted(self):
        gen_tasks = {"task_3", "task_8", "task_13"}
        n_build_tasks = 25
        mock_task_list = [
            MagicMock(display_name=f"task_{i}", priority=0) for i in range(n_build_tasks)
        ]
        mock_orchestrator = build_mock_orchestrator(build_task_list=mock_task_list)

        n_tasks_adjusted = mock_orchestrator.adjust_gen_tasks_priority(gen_tasks)

        self.assertEqual(len(gen_tasks), n_tasks_adjusted)

"""Unit tests for the evergreen_resomke_job_count script."""

import unittest

import psutil

from buildscripts import evergreen_resmoke_job_count as under_test

# pylint: disable=missing-docstring,no-self-use


class DetermineJobsTest(unittest.TestCase):
    cpu_count = psutil.cpu_count()
    mytask = "mytask"
    regex = "regexthatmatches"
    mytask_factor = 0.5
    regex_factor = 0.25
    task_factors = [{"task": mytask, "factor": mytask_factor},
                    {"task": "regex.*", "factor": regex_factor}]

    def test_determine_jobs_no_matching_task(self):
        jobs = under_test.determine_jobs("_no_match_", "_no_variant_", "_no_distro_", 0, 1)
        self.assertEqual(self.cpu_count, jobs)

    def test_determine_jobs_matching_variant(self):
        under_test.VARIANT_TASK_FACTOR_OVERRIDES = {"myvariant": self.task_factors}
        jobs = under_test.determine_jobs(self.mytask, "myvariant", "mydistro", 0, 1)
        self.assertEqual(int(round(self.cpu_count * self.mytask_factor)), jobs)
        jobs = under_test.determine_jobs(self.regex, "myvariant", "mydistro", 0, 1)
        self.assertEqual(int(round(self.cpu_count * self.regex_factor)), jobs)

    def test_determine_jobs_matching_distro(self):
        old_multipliers = under_test.DISTRO_MULTIPLIERS
        try:
            under_test.DISTRO_MULTIPLIERS = {"mydistro": 3}
            under_test.VARIANT_TASK_FACTOR_OVERRIDES = {"myvariant": self.task_factors}
            jobs = under_test.determine_jobs(self.mytask, "myvariant", "mydistro", 0, 1)
            self.assertEqual(int(round(self.cpu_count * self.mytask_factor * 3)), jobs)
            jobs = under_test.determine_jobs(self.regex, "myvariant", "mydistro", 0, 1)
            self.assertEqual(int(round(self.cpu_count * self.regex_factor * 3)), jobs)
        finally:
            under_test.DISTRO_MULTIPLIERS = old_multipliers

    def test_determine_factor_matching_variant(self):
        under_test.VARIANT_TASK_FACTOR_OVERRIDES = {"myvariant": self.task_factors}
        factor = under_test.determine_factor(self.mytask, "myvariant", "mydistro", 1)
        self.assertEqual(self.mytask_factor, factor)
        factor = under_test.determine_factor(self.regex, "myvariant", "mydistro", 1)
        self.assertEqual(self.regex_factor, factor)

    def test_determine_jobs_matching_machine(self):
        under_test.PLATFORM_MACHINE = "mymachine"
        under_test.MACHINE_TASK_FACTOR_OVERRIDES = {"mymachine": self.task_factors}
        jobs = under_test.determine_jobs(self.mytask, "myvariant", "mydistro", 0, 1)
        self.assertEqual(int(round(self.cpu_count * self.mytask_factor)), jobs)
        jobs = under_test.determine_jobs(self.regex, "myvariant", "mydistro", 0, 1)
        self.assertEqual(int(round(self.cpu_count * self.regex_factor)), jobs)

    def test_determine_factor_matching_machine(self):
        under_test.PLATFORM_MACHINE = "mymachine"
        under_test.MACHINE_TASK_FACTOR_OVERRIDES = {"mymachine": self.task_factors}
        factor = under_test.determine_factor(self.mytask, "myvariant", "mydistro", 1)
        self.assertEqual(self.mytask_factor, factor)
        factor = under_test.determine_factor(self.regex, "myvariant", "mydistro", 1)
        self.assertEqual(self.regex_factor, factor)

    def test_determine_jobs_matching_platform(self):
        under_test.SYS_PLATFORM = "myplatform"
        under_test.PLATFORM_TASK_FACTOR_OVERRIDES = {"myplatform": self.task_factors}
        jobs = under_test.determine_jobs(self.mytask, "myvariant", "mydistro", 0, 1)
        self.assertEqual(int(round(self.cpu_count * self.mytask_factor)), jobs)
        jobs = under_test.determine_jobs(self.regex, "myvariant", "mydistro", 0, 1)
        self.assertEqual(int(round(self.cpu_count * self.regex_factor)), jobs)

    def test_determine_factor_matching_platform(self):
        under_test.SYS_PLATFORM = "myplatform"
        under_test.PLATFORM_TASK_FACTOR_OVERRIDES = {"mymachine": self.task_factors}
        factor = under_test.determine_factor(self.mytask, "myvariant", "mydistro", 1)
        self.assertEqual(self.mytask_factor, factor)
        factor = under_test.determine_factor(self.regex, "myvariant", "mydistro", 1)
        self.assertEqual(self.regex_factor, factor)

    def test_determine_jobs_min_factor(self):
        under_test.PLATFORM_MACHINE = "mymachine"
        under_test.SYS_PLATFORM = "myplatform"
        mytask_factor_min = 0.5
        regex_factor_min = 0.25
        task_factors1 = [{"task": "mytask", "factor": mytask_factor_min + .5},
                         {"task": "regex.*", "factor": regex_factor_min + .5}]
        task_factors2 = [{"task": "mytask", "factor": mytask_factor_min + .25},
                         {"task": "regex.*", "factor": regex_factor_min + .25}]
        task_factors3 = [{"task": "mytask", "factor": mytask_factor_min},
                         {"task": "regex.*", "factor": regex_factor_min}]
        under_test.VARIANT_TASK_FACTOR_OVERRIDES = {"myvariant": task_factors1}
        under_test.MACHINE_TASK_FACTOR_OVERRIDES = {"mymachine": task_factors2}
        under_test.PLATFORM_TASK_FACTOR_OVERRIDES = {"myplatform": task_factors3}
        jobs = under_test.determine_jobs(self.mytask, "myvariant", "mydistro", 0, 1)
        self.assertEqual(int(round(self.cpu_count * mytask_factor_min)), jobs)
        jobs = under_test.determine_jobs(self.regex, "myvariant", "mydistro", 0, 1)
        self.assertEqual(int(round(self.cpu_count * regex_factor_min)), jobs)

    def test_determine_jobs_factor(self):
        factor = 0.4
        jobs = under_test.determine_jobs("_no_match_", "_no_variant_", "_no_distro_", 0, factor)
        self.assertEqual(int(round(self.cpu_count * factor)), jobs)

    def test_determine_jobs_jobs_max(self):
        jobs_max = 3
        jobs = under_test.determine_jobs("_no_match_", "_no_variant_", "_no_distro_", jobs_max, 1)
        self.assertEqual(min(jobs_max, jobs), jobs)
        jobs_max = 30
        jobs = under_test.determine_jobs("_no_match_", "_no_variant_", "_no_distro_", jobs_max, 1)
        self.assertEqual(min(jobs_max, jobs), jobs)

    def test_determine_jobs_with_global_specification(self):
        task = "matching_task"
        target_factor = 0.25
        jobs_default = 8
        under_test.CPU_COUNT = jobs_default
        under_test.GLOBAL_TASK_FACTOR_OVERRIDES = {
            r"matching.*": target_factor,
        }
        variant = "a_build_variant"
        distro = "a_distro"
        job_count_matching = under_test.determine_jobs(task, variant, distro, jobs_max=jobs_default)
        self.assertEqual(jobs_default * target_factor, job_count_matching)

    def test_determine_jobs_without_global_specification(self):
        task = "another_task"
        target_factor = 0.25
        jobs_default = 8
        under_test.CPU_COUNT = jobs_default
        under_test.GLOBAL_TASK_FACTOR_OVERRIDES = {
            r"matching.*": target_factor,
        }
        variant = "a_build_variant"
        distro = "a_distro"

        job_count_matching = under_test.determine_jobs(task, variant, distro, jobs_max=jobs_default)
        self.assertEqual(jobs_default, job_count_matching)

"""Unit tests for the evergreen_task_tags script."""

from __future__ import absolute_import

import unittest

from mock import MagicMock

from buildscripts import evergreen_task_tags as ett

# pylint: disable=missing-docstring,no-self-use


def gen_tag_set(prefix, size):
    return {prefix + " " + str(index) for index in range(size)}


class TestGetAllTaskTags(unittest.TestCase):
    def test_with_no_tasks(self):
        evg_config_mock = MagicMock(tasks=[])
        self.assertEqual(0, len(ett.get_all_task_tags(evg_config_mock)))

    def test_with_no_tags(self):
        n_tasks = 5
        task_list_mock = [MagicMock(tags=set()) for _ in range(n_tasks)]
        evg_config_mock = MagicMock(tasks=task_list_mock)
        self.assertEqual(0, len(ett.get_all_task_tags(evg_config_mock)))

    def test_with_some_tags(self):
        task_prefixes = ["b", "a", "q", "v"]
        n_tags = 3
        task_list_mock = [MagicMock(tags=gen_tag_set(prefix, n_tags)) for prefix in task_prefixes]
        evg_config_mock = MagicMock(tasks=task_list_mock)

        tag_list = ett.get_all_task_tags(evg_config_mock)
        self.assertEqual(n_tags * len(task_prefixes), len(tag_list))
        self.assertEqual(sorted(tag_list), tag_list)


class TestGetTasksWithTag(unittest.TestCase):
    def test_with_no_tasks(self):
        evg_config_mock = MagicMock(tasks=[])
        self.assertEqual(0, len(ett.get_tasks_with_tag(evg_config_mock, ["tag"], None)))

    def test_with_no_tags(self):
        n_tasks = 5
        task_list_mock = [MagicMock(tags=set()) for _ in range(n_tasks)]
        evg_config_mock = MagicMock(tasks=task_list_mock)
        self.assertEqual(0, len(ett.get_tasks_with_tag(evg_config_mock, ["tag"], None)))

    def test_with_one_tag_each(self):
        task_prefixes = ["b", "a", "b", "v"]
        n_tags = 3
        task_list_mock = [MagicMock(tags=gen_tag_set(prefix, n_tags)) for prefix in task_prefixes]
        for index, task in enumerate(task_list_mock):
            task.name = "task " + str(index)
        evg_config_mock = MagicMock(tasks=task_list_mock)

        task_list = ett.get_tasks_with_tag(evg_config_mock, ["b 0"], None)
        self.assertIn("task 0", task_list)
        self.assertIn("task 2", task_list)
        self.assertEqual(2, len(task_list))
        self.assertEqual(sorted(task_list), task_list)

    def test_with_two_tags(self):
        task_prefixes = ["b", "a", "b", "v"]
        n_tags = 3
        task_list_mock = [MagicMock(tags=gen_tag_set(prefix, n_tags)) for prefix in task_prefixes]
        for index, task in enumerate(task_list_mock):
            task.name = "task " + str(index)
        evg_config_mock = MagicMock(tasks=task_list_mock)

        task_list = ett.get_tasks_with_tag(evg_config_mock, ["b 0", "b 1"], None)
        self.assertIn("task 0", task_list)
        self.assertIn("task 2", task_list)
        self.assertEqual(2, len(task_list))
        self.assertEqual(sorted(task_list), task_list)

    def test_with_two_tags_no_results(self):
        task_prefixes = ["b", "a", "b", "v"]
        n_tags = 3
        task_list_mock = [MagicMock(tags=gen_tag_set(prefix, n_tags)) for prefix in task_prefixes]
        for index, task in enumerate(task_list_mock):
            task.name = "task " + str(index)
        evg_config_mock = MagicMock(tasks=task_list_mock)

        task_list = ett.get_tasks_with_tag(evg_config_mock, ["b 0", "a 0"], None)
        self.assertEqual(0, len(task_list))

    def test_with_one_filter(self):
        task_prefixes = ["b", "a", "b", "v"]
        n_tags = 3
        task_list_mock = [MagicMock(tags=gen_tag_set(prefix, n_tags)) for prefix in task_prefixes]
        for index, task in enumerate(task_list_mock):
            task.name = "task " + str(index)
        task_list_mock[0].tags = ["b 0"]
        evg_config_mock = MagicMock(tasks=task_list_mock)

        task_list = ett.get_tasks_with_tag(evg_config_mock, ["b 0"], ["b 1"])
        self.assertEqual(1, len(task_list))
        self.assertIn(task_list_mock[0].name, task_list)

    def test_with_two_filter(self):
        task_prefixes = ["b", "a", "b", "v"]
        n_tags = 3
        task_list_mock = [MagicMock(tags=gen_tag_set(prefix, n_tags)) for prefix in task_prefixes]
        for index, task in enumerate(task_list_mock):
            task.name = "task " + str(index)
        task_list_mock[0].tags = ["b 0"]
        evg_config_mock = MagicMock(tasks=task_list_mock)

        task_list = ett.get_tasks_with_tag(evg_config_mock, ["b 0"], ["b 1", "b 0"])
        self.assertEqual(0, len(task_list))


class TestGetAllTasks(unittest.TestCase):
    def test_get_all_tasks_for_empty_variant(self):
        variant = "variant 0"
        evg_config = MagicMock()

        task_list = ett.get_all_tasks(evg_config, variant)
        self.assertEqual(0, len(task_list))

    def test_get_all_tasks_for_variant_with_tasks(self):
        variant = "variant 0"
        evg_config = MagicMock()

        task_list = ett.get_all_tasks(evg_config, variant)
        self.assertEqual(evg_config.get_variant.return_value.task_names, task_list)
        evg_config.get_variant.assert_called_with(variant)

"""Unit tests for the evergreen_task_timeout script."""
from datetime import timedelta
import unittest

import buildscripts.evergreen_task_timeout as under_test

# pylint: disable=missing-docstring,no-self-use


class DetermineTimeoutTest(unittest.TestCase):
    def test_timeout_used_if_specified(self):
        timeout = timedelta(seconds=42)
        self.assertEqual(
            under_test.determine_timeout("task_name", "variant", None, timeout), timeout)

    def test_default_is_returned_with_no_timeout(self):
        self.assertEqual(
            under_test.determine_timeout("task_name", "variant"),
            under_test.DEFAULT_NON_REQUIRED_BUILD_TIMEOUT)

    def test_default_is_returned_with_timeout_at_zero(self):
        self.assertEqual(
            under_test.determine_timeout("task_name", "variant", timedelta(seconds=0)),
            under_test.DEFAULT_NON_REQUIRED_BUILD_TIMEOUT)

    def test_default_required_returned_on_required_variants(self):
        self.assertEqual(
            under_test.determine_timeout("task_name", "variant-required"),
            under_test.DEFAULT_REQUIRED_BUILD_TIMEOUT)

    def test_task_specific_timeout(self):
        self.assertEqual(
            under_test.determine_timeout("auth", "linux-64-debug"), timedelta(minutes=60))

    def test_commit_queue_items_use_commit_queue_timeout(self):
        timeout = under_test.determine_timeout("auth", "variant",
                                               evg_alias=under_test.COMMIT_QUEUE_ALIAS)
        self.assertEqual(timeout, under_test.COMMIT_QUEUE_TIMEOUT)

    def test_use_idle_timeout_if_greater_than_exec_timeout(self):
        idle_timeout = timedelta(hours=2)
        exec_timeout = timedelta(minutes=10)
        timeout = under_test.determine_timeout("task_name", "variant", idle_timeout=idle_timeout,
                                               exec_timeout=exec_timeout)

        self.assertEqual(timeout, idle_timeout)

"""Unit tests for feature_flag_tags_check.py."""
# pylint: disable=missing-docstring
import unittest
from unittest.mock import patch

from buildscripts import feature_flag_tags_check


class TestFindTestsInGitDiff(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.requires_fcv_tag = "requires_fcv_51"
        cls.original_requires_fcv_tag = feature_flag_tags_check.REQUIRES_FCV_TAG_LATEST
        feature_flag_tags_check.REQUIRES_FCV_TAG_LATEST = cls.requires_fcv_tag

    @classmethod
    def tearDownClass(cls):
        feature_flag_tags_check.REQUIRES_FCV_TAG_LATEST = cls.original_requires_fcv_tag

    def test_get_tests_missing_fcv_tag_no_tag(self):
        tests = ["dummy_jstest_file.js"]
        with patch.object(feature_flag_tags_check.jscomment, "get_tags", return_value=[]):
            result = feature_flag_tags_check.get_tests_missing_fcv_tag(tests)
        self.assertCountEqual(tests, result)

    def test_get_tests_missing_fcv_tag_have_tag(self):
        tests = ["dummy_jstest_file.js"]
        with patch.object(feature_flag_tags_check.jscomment, "get_tags",
                          return_value=[self.requires_fcv_tag]):
            result = feature_flag_tags_check.get_tests_missing_fcv_tag(tests)
        self.assertCountEqual([], result)

    def test_get_tests_missing_fcv_tag_test_file_deleted(self):
        tests = ["some/non/existent/jstest_file.js"]
        result = feature_flag_tags_check.get_tests_missing_fcv_tag(tests)
        self.assertCountEqual([], result)

"""Unit tests for powercycle_sentinel.py."""
# pylint: disable=missing-docstring
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock

from evergreen import EvergreenApi, Task

from buildscripts.powercycle_sentinel import watch_tasks, POWERCYCLE_TASK_EXEC_TIMEOUT_SECS


def make_task_mock(evg_api, task_id, start_time, finish_time):
    return Task({
        "task_id": task_id,
        "start_time": start_time,
        "finish_time": finish_time,
    }, evg_api)


class TestWatchTasks(unittest.TestCase):
    """Test watch_tasks."""

    def test_no_long_running_tasks(self):
        evg_api = EvergreenApi()
        task_ids = ["1", "2"]
        now = datetime.now(timezone.utc).isoformat()
        task_1 = make_task_mock(evg_api, task_ids[0], now, now)
        task_2 = make_task_mock(evg_api, task_ids[1], now, now)
        evg_api.task_by_id = Mock(
            side_effect=(lambda task_id: {
                "1": task_1,
                "2": task_2,
            }[task_id]))
        long_running_task_ids = watch_tasks(task_ids, evg_api, 0)
        self.assertEqual([], long_running_task_ids)

    def test_found_long_running_tasks(self):
        evg_api = EvergreenApi()
        task_ids = ["1", "2"]
        exec_timeout_seconds_ago = (datetime.now(timezone.utc) -
                                    timedelta(hours=POWERCYCLE_TASK_EXEC_TIMEOUT_SECS)).isoformat()
        now = datetime.now(timezone.utc).isoformat()
        task_1 = make_task_mock(evg_api, task_ids[0], exec_timeout_seconds_ago, now)
        task_2 = make_task_mock(evg_api, task_ids[1], exec_timeout_seconds_ago, None)
        evg_api.task_by_id = Mock(
            side_effect=(lambda task_id: {
                "1": task_1,
                "2": task_2,
            }[task_id]))
        long_running_task_ids = watch_tasks(task_ids, evg_api, 0)
        self.assertEqual([task_2.task_id], long_running_task_ids)

#!/usr/bin/env python3
"""Unit test for buildscripts/remote_operations.py.

   Note - Tests require sshd to be enabled on localhost with paswordless login
   and can fail otherwise."""

import os
import shutil
import tempfile
import time
import unittest

from buildscripts import remote_operations as rop

# pylint: disable=invalid-name,missing-docstring,protected-access


class RemoteOperationsTestCase(unittest.TestCase):
    def setUp(self):
        self.temp_local_dir = tempfile.mkdtemp()
        self.temp_remote_dir = tempfile.mkdtemp()
        self.rop = rop.RemoteOperations(user_host="localhost")
        self.rop_use_shell = rop.RemoteOperations(user_host="localhost", use_shell=True)
        self.rop_sh_shell_binary = rop.RemoteOperations(user_host="localhost",
                                                        shell_binary="/bin/sh")
        self.rop_ssh_opts = rop.RemoteOperations(
            user_host="localhost",
            ssh_connection_options="-v -o ConnectTimeout=10 -o ConnectionAttempts=10")

    def tearDown(self):
        shutil.rmtree(self.temp_local_dir, ignore_errors=True)
        shutil.rmtree(self.temp_remote_dir, ignore_errors=True)


class RemoteOperationConnection(RemoteOperationsTestCase):
    @unittest.skip("Known broken. SERVER-48969 tracks re-enabling.")
    def runTest(self):

        self.assertTrue(self.rop.access_established())
        ret, buff = self.rop.access_info()
        self.assertEqual(0, ret)

        # Invalid host
        remote_op = rop.RemoteOperations(user_host="badhost")
        ret, buff = remote_op.access_info()
        self.assertFalse(remote_op.access_established())
        self.assertEqual(255, ret)
        self.assertIsNotNone(buff)

        # Invalid host with retries
        remote_op = rop.RemoteOperations(user_host="badhost2", retries=3)
        ret, buff = remote_op.access_info()
        self.assertFalse(remote_op.access_established())
        self.assertNotEqual(0, ret)
        self.assertIsNotNone(buff)

        # Invalid host with retries & retry_sleep
        remote_op = rop.RemoteOperations(user_host="badhost3", retries=3, retry_sleep=1)
        ret, buff = remote_op.access_info()
        self.assertFalse(remote_op.access_established())
        self.assertNotEqual(0, ret)
        self.assertIsNotNone(buff)

        # Valid host with invalid ssh options
        ssh_connection_options = "-o invalid"
        remote_op = rop.RemoteOperations(user_host="localhost",
                                         ssh_connection_options=ssh_connection_options)
        ret, buff = remote_op.access_info()
        self.assertFalse(remote_op.access_established())
        self.assertNotEqual(0, ret)
        self.assertIsNotNone(buff)

        ssh_options = "--invalid"
        remote_op = rop.RemoteOperations(user_host="localhost", ssh_options=ssh_options)
        ret, buff = remote_op.access_info()
        self.assertFalse(remote_op.access_established())
        self.assertNotEqual(0, ret)
        self.assertIsNotNone(buff)

        # Valid host with valid ssh options
        ssh_connection_options = "-v -o ConnectTimeout=10 -o ConnectionAttempts=10"
        remote_op = rop.RemoteOperations(user_host="localhost",
                                         ssh_connection_options=ssh_connection_options)
        ret, buff = remote_op.access_info()
        self.assertTrue(remote_op.access_established())
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ssh_options = "-v -o ConnectTimeout=10 -o ConnectionAttempts=10"
        remote_op = rop.RemoteOperations(user_host="localhost", ssh_options=ssh_options)
        ret, buff = remote_op.access_info()
        self.assertTrue(remote_op.access_established())
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ssh_connection_options = "-v -o ConnectTimeout=10 -o ConnectionAttempts=10"
        ssh_options = "-t"
        remote_op = rop.RemoteOperations(user_host="localhost",
                                         ssh_connection_options=ssh_connection_options,
                                         ssh_options=ssh_options)
        ret, buff = remote_op.access_info()
        self.assertTrue(remote_op.access_established())
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)


class RemoteOperationShell(RemoteOperationsTestCase):
    @unittest.skip("Known broken. SERVER-48969 tracks re-enabling.")
    def runTest(self):  # pylint: disable=too-many-statements

        # Shell connect
        ret, buff = self.rop.shell("uname")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ret, buff = self.rop_use_shell.shell("uname")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ret, buff = self.rop_sh_shell_binary.shell("uname")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ret, buff = self.rop.operation("shell", "uname")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        # Invalid command
        ret, buff = self.rop.shell("invalid_command")
        self.assertNotEqual(0, ret)
        self.assertIsNotNone(buff)

        # Multiple commands
        ret, buff = self.rop.shell("date; whoami; ls")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ret, buff = self.rop_use_shell.shell("date; whoami; ls")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ret, buff = self.rop_sh_shell_binary.shell("date; whoami; ls")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ret, buff = self.rop_ssh_opts.shell("date; whoami; ls")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        # Command with single quotes
        ret, buff = self.rop.shell("echo 'hello there' | grep 'hello'")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ret, buff = self.rop_use_shell.shell("echo 'hello there' | grep 'hello'")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        # Multiple commands with escaped single quotes
        ret, buff = self.rop.shell("echo \"hello \'dolly\'\"; pwd; echo \"goodbye \'charlie\'\"")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ret, buff = self.rop_use_shell.shell(
            "echo \"hello \'dolly\'\"; pwd; echo \"goodbye \'charlie\'\"")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        # Command with escaped double quotes
        ret, buff = self.rop.shell("echo \"hello there\" | grep \"hello\"")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ret, buff = self.rop_use_shell.shell("echo \"hello there\" | grep \"hello\"")
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        # Command with directory and pipe
        ret, buff = self.rop.shell("touch {dir}/{file}; ls {dir} | grep {file}".format(
            file=time.time(), dir="/tmp"))
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)

        ret, buff = self.rop_use_shell.shell("touch {dir}/{file}; ls {dir} | grep {file}".format(
            file=time.time(), dir="/tmp"))
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)


class RemoteOperationCopyTo(RemoteOperationsTestCase):
    @unittest.skip("Known broken. SERVER-48969 tracks re-enabling.")
    def runTest(self):  # pylint: disable=too-many-statements

        # Copy to remote
        l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir)[1]
        l_temp_file = os.path.basename(l_temp_path)
        ret, buff = self.rop.copy_to(l_temp_path, self.temp_remote_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        r_temp_path = os.path.join(self.temp_remote_dir, l_temp_file)
        self.assertTrue(os.path.isfile(r_temp_path))

        l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir)[1]
        l_temp_file = os.path.basename(l_temp_path)
        ret, buff = self.rop_use_shell.copy_to(l_temp_path, self.temp_remote_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        r_temp_path = os.path.join(self.temp_remote_dir, l_temp_file)
        self.assertTrue(os.path.isfile(r_temp_path))

        l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir)[1]
        l_temp_file = os.path.basename(l_temp_path)
        ret, buff = self.rop.operation("copy_to", l_temp_path, self.temp_remote_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        self.assertTrue(os.path.isfile(r_temp_path))

        l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir)[1]
        l_temp_file = os.path.basename(l_temp_path)
        ret, buff = self.rop_ssh_opts.operation("copy_to", l_temp_path, self.temp_remote_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        self.assertTrue(os.path.isfile(r_temp_path))

        # Copy multiple files to remote
        num_files = 3
        l_temp_files = []
        for i in range(num_files):
            l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir)[1]
            l_temp_file = os.path.basename(l_temp_path)
            l_temp_files.append(l_temp_path)
        ret, buff = self.rop.copy_to(" ".join(l_temp_files), self.temp_remote_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        for i in range(num_files):
            r_temp_path = os.path.join(self.temp_remote_dir, os.path.basename(l_temp_files[i]))
            self.assertTrue(os.path.isfile(r_temp_path))

        num_files = 3
        l_temp_files = []
        for i in range(num_files):
            l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir)[1]
            l_temp_file = os.path.basename(l_temp_path)
            l_temp_files.append(l_temp_path)
        ret, buff = self.rop_use_shell.copy_to(" ".join(l_temp_files), self.temp_remote_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        for i in range(num_files):
            r_temp_path = os.path.join(self.temp_remote_dir, os.path.basename(l_temp_files[i]))
            self.assertTrue(os.path.isfile(r_temp_path))

        # Copy to remote without directory
        l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir)[1]
        l_temp_file = os.path.basename(l_temp_path)
        ret, buff = self.rop.copy_to(l_temp_path)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        r_temp_path = os.path.join(os.environ["HOME"], l_temp_file)
        self.assertTrue(os.path.isfile(r_temp_path))
        os.remove(r_temp_path)

        l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir)[1]
        l_temp_file = os.path.basename(l_temp_path)
        ret, buff = self.rop_use_shell.copy_to(l_temp_path)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        r_temp_path = os.path.join(os.environ["HOME"], l_temp_file)
        self.assertTrue(os.path.isfile(r_temp_path))
        os.remove(r_temp_path)

        # Copy to remote with space in file name, note it must be quoted.
        l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir, prefix="filename with space")[1]
        l_temp_file = os.path.basename(l_temp_path)
        ret, buff = self.rop.copy_to("'{}'".format(l_temp_path))
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        r_temp_path = os.path.join(os.environ["HOME"], l_temp_file)
        self.assertTrue(os.path.isfile(r_temp_path))
        os.remove(r_temp_path)

        l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir, prefix="filename with space")[1]
        l_temp_file = os.path.basename(l_temp_path)
        ret, buff = self.rop_use_shell.copy_to("'{}'".format(l_temp_path))
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        r_temp_path = os.path.join(os.environ["HOME"], l_temp_file)
        self.assertTrue(os.path.isfile(r_temp_path))
        os.remove(r_temp_path)

        # Valid scp options
        l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir)[1]
        l_temp_file = os.path.basename(l_temp_path)
        scp_options = "-l 5000"
        remote_op = rop.RemoteOperations(user_host="localhost", scp_options=scp_options)
        ret, buff = remote_op.copy_to(l_temp_path, self.temp_remote_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        r_temp_path = os.path.join(self.temp_remote_dir, l_temp_file)
        self.assertTrue(os.path.isfile(r_temp_path))

        # Invalid scp options
        l_temp_path = tempfile.mkstemp(dir=self.temp_local_dir)[1]
        scp_options = "--invalid"
        remote_op = rop.RemoteOperations(user_host="localhost", scp_options=scp_options)
        ret, buff = remote_op.copy_to(l_temp_path, self.temp_remote_dir)
        self.assertNotEqual(0, ret)
        self.assertIsNotNone(buff)


class RemoteOperationCopyFrom(RemoteOperationsTestCase):
    @unittest.skip("Known broken. SERVER-48969 tracks re-enabling.")
    def runTest(self):  # pylint: disable=too-many-statements

        # Copy from remote
        r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir)[1]
        r_temp_file = os.path.basename(r_temp_path)
        ret, buff = self.rop.copy_from(r_temp_path, self.temp_local_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        l_temp_path = os.path.join(self.temp_local_dir, r_temp_file)
        self.assertTrue(os.path.isfile(l_temp_path))

        r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir)[1]
        r_temp_file = os.path.basename(r_temp_path)
        ret, buff = self.rop_use_shell.copy_from(r_temp_path, self.temp_local_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        l_temp_path = os.path.join(self.temp_local_dir, r_temp_file)
        self.assertTrue(os.path.isfile(l_temp_path))

        r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir)[1]
        r_temp_file = os.path.basename(r_temp_path)
        ret, buff = self.rop_ssh_opts.copy_from(r_temp_path, self.temp_local_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        l_temp_path = os.path.join(self.temp_local_dir, r_temp_file)
        self.assertTrue(os.path.isfile(l_temp_path))

        # Copy from remote without directory
        r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir)[1]
        r_temp_file = os.path.basename(r_temp_path)
        ret, buff = self.rop.copy_from(r_temp_path)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        self.assertTrue(os.path.isfile(r_temp_file))
        os.remove(r_temp_file)

        r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir)[1]
        r_temp_file = os.path.basename(r_temp_path)
        ret, buff = self.rop_use_shell.copy_from(r_temp_path)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        self.assertTrue(os.path.isfile(r_temp_file))
        os.remove(r_temp_file)

        # Copy from remote with space in file name, note it must be quoted.
        r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir, prefix="filename with space")[1]
        r_temp_file = os.path.basename(r_temp_path)
        ret, buff = self.rop.copy_from("'{}'".format(r_temp_path))
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        self.assertTrue(os.path.isfile(r_temp_file))
        os.remove(r_temp_file)

        # Copy multiple files from remote
        num_files = 3
        r_temp_files = []
        for i in range(num_files):
            r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir)[1]
            r_temp_file = os.path.basename(r_temp_path)
            r_temp_files.append(r_temp_path)
        ret, buff = self.rop.copy_from(" ".join(r_temp_files), self.temp_local_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        for i in range(num_files):
            basefile_name = os.path.basename(r_temp_files[i])
            l_temp_path = os.path.join(self.temp_local_dir, basefile_name)
            self.assertTrue(os.path.isfile(l_temp_path))

        num_files = 3
        r_temp_files = []
        for i in range(num_files):
            r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir)[1]
            r_temp_file = os.path.basename(r_temp_path)
            r_temp_files.append(r_temp_path)
        ret, buff = self.rop_use_shell.copy_from(" ".join(r_temp_files), self.temp_local_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        for i in range(num_files):
            basefile_name = os.path.basename(r_temp_files[i])
            l_temp_path = os.path.join(self.temp_local_dir, basefile_name)
            self.assertTrue(os.path.isfile(l_temp_path))

        # Copy files from remote with wilcard
        num_files = 3
        r_temp_files = []
        for i in range(num_files):
            r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir, prefix="wild1")[1]
            r_temp_file = os.path.basename(r_temp_path)
            r_temp_files.append(r_temp_path)
        r_temp_path = os.path.join(self.temp_remote_dir, "wild1*")
        ret, buff = self.rop.copy_from(r_temp_path, self.temp_local_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        for i in range(num_files):
            l_temp_path = os.path.join(self.temp_local_dir, os.path.basename(r_temp_files[i]))
            self.assertTrue(os.path.isfile(l_temp_path))

        num_files = 3
        r_temp_files = []
        for i in range(num_files):
            r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir, prefix="wild2")[1]
            r_temp_file = os.path.basename(r_temp_path)
            r_temp_files.append(r_temp_path)
        r_temp_path = os.path.join(self.temp_remote_dir, "wild2*")
        ret, buff = self.rop_use_shell.copy_from(r_temp_path, self.temp_local_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        for i in range(num_files):
            l_temp_path = os.path.join(self.temp_local_dir, os.path.basename(r_temp_files[i]))
            self.assertTrue(os.path.isfile(l_temp_path))

        # Local directory does not exist.
        self.assertRaises(ValueError, lambda: self.rop_use_shell.copy_from(r_temp_path, "bad_dir"))

        # Valid scp options
        r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir)[1]
        r_temp_file = os.path.basename(r_temp_path)
        scp_options = "-l 5000"
        remote_op = rop.RemoteOperations(user_host="localhost", scp_options=scp_options)
        ret, buff = remote_op.copy_from(r_temp_path, self.temp_local_dir)
        self.assertEqual(0, ret)
        self.assertIsNotNone(buff)
        l_temp_path = os.path.join(self.temp_local_dir, r_temp_file)
        self.assertTrue(os.path.isfile(l_temp_path))

        # Invalid scp options
        r_temp_path = tempfile.mkstemp(dir=self.temp_remote_dir)[1]
        scp_options = "--invalid"
        remote_op = rop.RemoteOperations(user_host="localhost", scp_options=scp_options)
        ret, buff = remote_op.copy_from(r_temp_path, self.temp_local_dir)
        self.assertNotEqual(0, ret)
        self.assertIsNotNone(buff)


class RemoteOperation(RemoteOperationsTestCase):
    @unittest.skip("Known broken. SERVER-48969 tracks re-enabling.")
    def runTest(self):
        # Invalid operation
        self.assertRaises(ValueError, lambda: self.rop.operation("invalid", None))
