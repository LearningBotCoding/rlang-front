<?xml version="1.0" encoding="utf-8"?>

<!-- header for use with make_vcxproj.py

     Note that once you generate the vcxproj file, if you change it in visual studio, when it
     writes it back out you will use the comments and semi-neat formatting below.
-->

<Project DefaultTargets="Build" ToolsVersion="%ToolsVersion%" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <ItemGroup Label="ProjectConfigurations">
    <ProjectConfiguration Include="Debug|x64">
      <Configuration>Debug</Configuration>
      <Platform>x64</Platform>
    </ProjectConfiguration>
    <ProjectConfiguration Include="Release|x64">
      <Configuration>Release</Configuration>
      <Platform>x64</Platform>
    </ProjectConfiguration>
  </ItemGroup>

  <PropertyGroup Label="Globals">
    <VCProjectVersion>%ToolsVersion%</VCProjectVersion>
    <ProjectName>%_TARGET_%</ProjectName>
    <RootNamespace>mongod</RootNamespace>
    <PlatformToolset>%PlatformToolset%</PlatformToolset>
    <UseNativeEnvironment>true</UseNativeEnvironment>
    <!-- <ProjectGuid>{215B2D68-0A70-4D10-8E75-B31010C62A91}</ProjectGuid> -->
    <Keyword>MakeFileProj</Keyword>
    <RootNamespace>%_TARGET_%</RootNamespace>
    <WindowsTargetPlatformVersion>%WindowsTargetPlatformVersion%</WindowsTargetPlatformVersion>
  </PropertyGroup>

  <Import Project="$(VCTargetsPath)\Microsoft.Cpp.Default.props" />

  <PropertyGroup Label="Configuration">
    <ConfigurationType>Makefile</ConfigurationType>
    <CharacterSet>Unicode</CharacterSet>
  </PropertyGroup>

  <Import Project="$(VCTargetsPath)\Microsoft.Cpp.props" />
  <ImportGroup Label="PropertySheets">
    <Import Project="$(UserRootDir)\Microsoft.Cpp.$(Platform).user.props" Condition="exists('$(UserRootDir)\Microsoft.Cpp.$(Platform).user.props')" Label="LocalAppDataPlatform" />
  </ImportGroup>
  <PropertyGroup Label="UserMacros" />

  <PropertyGroup>
    <_ProjectFileVersion>10.0.30319.1</_ProjectFileVersion>
    <OutDir>$(ProjectDir)$(Platform)\$(Configuration)\</OutDir>
    <LinkIncremental Condition="'$(Configuration)|$(Platform)'=='Debug|x64'">true</LinkIncremental>
    <LinkIncremental Condition="'$(Configuration)|$(Platform)'=='Release|x64'">false</LinkIncremental>
    <CodeAnalysisRuleSet >AllRules.ruleset</CodeAnalysisRuleSet>
  </PropertyGroup>

  <!-- GLOBAL SETTINGS ALL BUILD STYLES -->
  <ItemDefinitionGroup>
    <ClCompile>
      <ObjectFileName>Build/%(RelativeDir)/</ObjectFileName>
      <AdditionalIncludeDirectories>%AdditionalIncludeDirectories%</AdditionalIncludeDirectories>
      <PreprocessorDefinitions>%(PreprocessorDefinitions)</PreprocessorDefinitions>
      <DisableSpecificWarnings>4290;4355;4800;4267;4244;4351</DisableSpecificWarnings>
      <MultiProcessorCompilation>true</MultiProcessorCompilation>
      <MinimalRebuild>No</MinimalRebuild>
      <WarningLevel>Level3</WarningLevel>
      <PrecompiledHeaderFile>pch.h</PrecompiledHeaderFile>
      <DebugInformationFormat>ProgramDatabase</DebugInformationFormat>
      <IntrinsicFunctions>true</IntrinsicFunctions>
    </ClCompile>
    <Link>
      <SubSystem>Console</SubSystem>
      <GenerateDebugInformation>true</GenerateDebugInformation>
      <IgnoreAllDefaultLibraries>false</IgnoreAllDefaultLibraries>
      <AdditionalDependencies>winmm.lib;ws2_32.lib;psapi.lib;dbghelp.lib;%(AdditionalDependencies)</AdditionalDependencies>
    </Link>
  </ItemDefinitionGroup>

  <!-- DEBUG -->
  <ItemDefinitionGroup Condition="'$(Configuration)'=='Debug'">
    <ClCompile>
      <Optimization>Disabled</Optimization>
      <BasicRuntimeChecks>EnableFastChecks</BasicRuntimeChecks>
      <RuntimeLibrary>MultiThreadedDebug</RuntimeLibrary>
      <PreprocessorDefinitions>_DEBUG;DEBUG;OBJECT_PRINT;ENABLE_DISASSEMBLER;%(PreprocessorDefinitions)</PreprocessorDefinitions>
      <!--<DebugInformationFormat>EditAndContinue</DebugInformationFormat>-->
    </ClCompile>
  </ItemDefinitionGroup>

 <!-- RELEASE -->
  <ItemDefinitionGroup Condition="'$(Configuration)'=='Release'">
    <ClCompile>
      <Optimization>MaxSpeed</Optimization>
      <FunctionLevelLinking>true</FunctionLevelLinking>
      <RuntimeLibrary>MultiThreaded</RuntimeLibrary>
    </ClCompile>
  </ItemDefinitionGroup>

  <!-- X64 -->
  <ItemDefinitionGroup Condition="'$(Platform)'=='x64'">
    <ClCompile>
      <PreprocessorDefinitions>V8_TARGET_ARCH_X64;%(PreprocessorDefinitions)</PreprocessorDefinitions>
    </ClCompile>
  </ItemDefinitionGroup>

  <!-- 4.0 - change to a Makefile project to allow invocation of external build system -->
  <PropertyGroup Condition="'$(Configuration)'=='Debug'">
    <NMakeBuildCommandLine>ninja.exe -k 0 core</NMakeBuildCommandLine>
    <NMakeOutput>mongod.exe</NMakeOutput>
    <NMakeCleanCommandLine>ninja.exe -t clean</NMakeCleanCommandLine>
    <NMakeReBuildCommandLine>not_supported</NMakeReBuildCommandLine>
    <NMakePreprocessorDefinitions>MONGO_SSL;WIN32;_DEBUG;$(NMakePreprocessorDefinitions)</NMakePreprocessorDefinitions>
    <NMakeIncludeSearchPath>$(NMakeIncludeSearchPath)</NMakeIncludeSearchPath>
    <AdditionalOptions>/std:c++latest</AdditionalOptions>
  </PropertyGroup>
  <PropertyGroup Condition="'$(Configuration)'=='Release'">
    <NMakeBuildCommandLine>ninja.exe -k 0 core</NMakeBuildCommandLine>
    <NMakeOutput>mongod.exe</NMakeOutput>
    <NMakeCleanCommandLine>ninja.exe -t clean</NMakeCleanCommandLine>
    <NMakeReBuildCommandLine>not_supported</NMakeReBuildCommandLine>
    <NMakePreprocessorDefinitions>not_supported;WIN32;NDEBUG;$(NMakePreprocessorDefinitions)</NMakePreprocessorDefinitions>
    <NMakeIncludeSearchPath>$(NMakeIncludeSearchPath)</NMakeIncludeSearchPath>
    <AdditionalOptions>/std:c++latest</AdditionalOptions>
  </PropertyGroup>

   <!-- SPECIFICS -->
  <ItemDefinitionGroup Condition="'$(Configuration)|$(Platform)'=='Debug|x64'">
  </ItemDefinitionGroup>
  <ItemDefinitionGroup Condition="'$(Configuration)|$(Platform)'=='Release|x64'">
  </ItemDefinitionGroup>

#!/usr/bin/env python3
"""Utility to return YAML value from key in YAML file."""

import optparse

import yaml


def get_yaml_value(yaml_file, yaml_key):
    """Return string value for 'yaml_key' from 'yaml_file'."""
    with open(yaml_file, "r") as ystream:
        yaml_dict = yaml.safe_load(ystream)
    return str(yaml_dict.get(yaml_key, ""))


def main():
    """Execute Main program."""

    parser = optparse.OptionParser(description=__doc__)
    parser.add_option("--yamlFile", dest="yaml_file", default=None, help="YAML file to read")
    parser.add_option("--yamlKey", dest="yaml_key", default=None,
                      help="Top level YAML key to provide the value")

    (options, _) = parser.parse_args()
    if not options.yaml_file:
        parser.error("Must specifiy '--yamlFile'")
    if not options.yaml_key:
        parser.error("Must specifiy '--yamlKey'")

    print(get_yaml_value(options.yaml_file, options.yaml_key))


if __name__ == "__main__":
    main()

set -o errexit

BASEDIR=$(dirname "$0")
cd "$BASEDIR/../"

find buildscripts etc jstests -name '*.y*ml' -exec yamllint -c etc/yamllint_config.yml {} +
python -m evergreen_lint -c ./etc/evergreen_lint.yml lint

"""Empty."""

"""Aggregate_tracefiles module.

This script aggregates several tracefiles into one tracefile.
All but the last argument are input tracefiles or .txt files which list tracefiles.
The last argument is the tracefile to which the output will be written.
"""

import subprocess
import os
import sys
from optparse import OptionParser


def aggregate(inputs, output):
    """Aggregate the tracefiles given in inputs to a tracefile given by output."""
    args = ['lcov']

    for name in inputs:
        args += ['-a', name]

    args += ['-o', output]

    print(' '.join(args))

    return subprocess.call(args)


def getfilesize(path):
    """Return file size of 'path'."""
    if not os.path.isfile(path):
        return 0
    return os.path.getsize(path)


def main():
    """Execute Main entry."""
    inputs = []

    usage = "usage: %prog input1.info input2.info ... output.info"
    parser = OptionParser(usage=usage)

    (_, args) = parser.parse_args()
    if len(args) < 2:
        return "must supply input files"

    for path in args[:-1]:
        _, ext = os.path.splitext(path)

        if ext == '.info':
            if getfilesize(path) > 0:
                inputs.append(path)

        elif ext == '.txt':
            inputs += [line.strip() for line in open(path) if getfilesize(line.strip()) > 0]
        else:
            return "unrecognized file type"

    return aggregate(inputs, args[-1])


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python3
"""Utility script to run Black Duck scans and query Black Duck database."""
#pylint: disable=too-many-lines

import argparse
import functools
import io
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import warnings

from abc import ABCMeta, abstractmethod
from typing import Dict, List, Optional

import urllib3.util.retry as urllib3_retry
import requests
import yaml

from blackduck.HubRestApi import HubInstance

try:
    import requests.packages.urllib3.exceptions as urllib3_exceptions  #pylint: disable=ungrouped-imports
except ImportError:
    # Versions of the requests package prior to 1.2.0 did not vendor the urllib3 package.
    urllib3_exceptions = None

LOGGER = logging.getLogger(__name__)

############################################################################

# Name of project to upload to and query about
BLACKDUCK_PROJECT = "mongodb/mongo"

# Version of project to query about
# Black Duck automatically determines the version based on branch
BLACKDUCK_PROJECT_VERSION = "master"

# Timeout to wait for a Black Duck scan to complete
BLACKDUCK_TIMEOUT_SECS = 600

# Black Duck hub api uses this file to get settings
BLACKDUCK_RESTCONFIG = ".restconfig.json"

# Wiki page where we document more information about Black Duck
BLACKDUCK_WIKI_PAGE = "https://wiki.corp.mongodb.com/display/KERNEL/Black+Duck"

# Black Duck failed report prefix
BLACKDUCK_FAILED_PREFIX = "A Black Duck scan was run and failed"

# Black Duck default teaam
BLACKDUCK_DEFAULT_TEAM = "Service Development Platform"

############################################################################

# Globals
BLACKDUCK_PROJECT_URL = None

############################################################################

# Build Logger constants

BUILD_LOGGER_CREATE_BUILD_ENDPOINT = "/build"
BUILD_LOGGER_APPEND_GLOBAL_LOGS_ENDPOINT = "/build/%(build_id)s"
BUILD_LOGGER_CREATE_TEST_ENDPOINT = "/build/%(build_id)s/test"
BUILD_LOGGER_APPEND_TEST_LOGS_ENDPOINT = "/build/%(build_id)s/test/%(test_id)s"

BUILD_LOGGER_DEFAULT_URL = "https://logkeeper.mongodb.org"
BUILD_LOGGER_TIMEOUT_SECS = 65

LOCAL_REPORTS_DIR = "bd_reports"

############################################################################

THIRD_PARTY_DIRECTORIES = [
    'src/third_party/wiredtiger/test/3rdparty',
    'src/third_party',
]

THIRD_PARTY_COMPONENTS_FILE = "etc/third_party_components.yml"

############################################################################

RE_LETTERS = re.compile("[A-Za-z]{2,}")


def default_if_none(value, default):
    """Set default if value is 'None'."""
    return value if value is not None else default


# Derived from buildscripts/resmokelib/logging/handlers.py
class HTTPHandler(object):
    """A class which sends data to a web server using POST requests."""

    def __init__(self, url_root, username, password, should_retry=False):
        """Initialize the handler with the necessary authentication credentials."""

        self.auth_handler = requests.auth.HTTPBasicAuth(username, password)

        self.session = requests.Session()

        if should_retry:
            retry_status = [500, 502, 503, 504]  # Retry for these statuses.
            retry = urllib3_retry.Retry(
                backoff_factor=0.1,  # Enable backoff starting at 0.1s.
                allowed_methods=False,  # Support all HTTP verbs.
                status_forcelist=retry_status)

            adapter = requests.adapters.HTTPAdapter(max_retries=retry)
            self.session.mount('http://', adapter)
            self.session.mount('https://', adapter)

        self.url_root = url_root

    def make_url(self, endpoint):
        """Generate a url to post to."""
        return "%s/%s/" % (self.url_root.rstrip("/"), endpoint.strip("/"))

    def post(self, endpoint, data=None, headers=None, timeout_secs=BUILD_LOGGER_TIMEOUT_SECS):
        """
        Send a POST request to the specified endpoint with the supplied data.

        Return the response, either as a string or a JSON object based
        on the content type.
        """

        data = default_if_none(data, [])
        data = json.dumps(data)

        headers = default_if_none(headers, {})
        headers["Content-Type"] = "application/json; charset=utf-8"

        url = self.make_url(endpoint)

        LOGGER.info("POSTING to %s", url)

        with warnings.catch_warnings():
            if urllib3_exceptions is not None:
                try:
                    warnings.simplefilter("ignore", urllib3_exceptions.InsecurePlatformWarning)
                except AttributeError:
                    # Versions of urllib3 prior to 1.10.3 didn't define InsecurePlatformWarning.
                    # Versions of requests prior to 2.6.0 didn't have a vendored copy of urllib3
                    # that defined InsecurePlatformWarning.
                    pass

                try:
                    warnings.simplefilter("ignore", urllib3_exceptions.InsecureRequestWarning)
                except AttributeError:
                    # Versions of urllib3 prior to 1.9 didn't define InsecureRequestWarning.
                    # Versions of requests prior to 2.4.0 didn't have a vendored copy of urllib3
                    # that defined InsecureRequestWarning.
                    pass

            response = self.session.post(url, data=data, headers=headers, timeout=timeout_secs,
                                         auth=self.auth_handler, verify=True)

        response.raise_for_status()

        if not response.encoding:
            response.encoding = "utf-8"

        headers = response.headers

        if headers["Content-Type"].startswith("application/json"):
            return response.json()

        return response.text


# Derived from buildscripts/resmokelib/logging/buildlogger.py
class BuildloggerServer(object):
    # pylint: disable=too-many-instance-attributes
    """
    A remote server to which build logs can be sent.

    It is used to retrieve handlers that can then be added to logger
    instances to send the log to the servers.
    """

    def __init__(self, username, password, task_id, builder, build_num, build_phase, url):
        # pylint: disable=too-many-arguments
        """Initialize BuildloggerServer."""
        self.username = username
        self.password = password
        self.builder = builder
        self.build_num = build_num
        self.build_phase = build_phase
        self.url = url
        self.task_id = task_id

        self.handler = HTTPHandler(url_root=self.url, username=self.username,
                                   password=self.password, should_retry=True)

    def new_build_id(self, suffix):
        """Return a new build id for sending global logs to."""
        builder = "%s_%s" % (self.builder, suffix)
        build_num = int(self.build_num)

        response = self.handler.post(
            BUILD_LOGGER_CREATE_BUILD_ENDPOINT, data={
                "builder": builder,
                "buildnum": build_num,
                "task_id": self.task_id,
            })

        return response["id"]

    def new_test_id(self, build_id, test_filename, test_command):
        """Return a new test id for sending test logs to."""
        endpoint = BUILD_LOGGER_CREATE_TEST_ENDPOINT % {"build_id": build_id}

        response = self.handler.post(
            endpoint, data={
                "test_filename": test_filename,
                "command": test_command,
                "phase": self.build_phase,
                "task_id": self.task_id,
            })

        return response["id"]

    def post_new_file(self, build_id, test_name, lines):
        """Post a new file to the build logger server."""
        test_id = self.new_test_id(build_id, test_name, "foo")
        endpoint = BUILD_LOGGER_APPEND_TEST_LOGS_ENDPOINT % {
            "build_id": build_id,
            "test_id": test_id,
        }

        dt = time.time()

        dlines = [(dt, line) for line in lines]

        try:
            self.handler.post(endpoint, data=dlines)
        except requests.HTTPError as err:
            # Handle the "Request Entity Too Large" error, set the max size and retry.
            raise ValueError("Encountered an HTTP error: %s" % (err))
        except requests.RequestException as err:
            raise ValueError("Encountered a network error: %s" % (err))
        except:  # pylint: disable=bare-except
            raise ValueError("Encountered an error.")

        return self.handler.make_url(endpoint)


def _to_dict(items, func):
    dm = {}

    for i in items:
        tuple1 = func(i)
        dm[tuple1[0]] = tuple1[1]

    return dm


def _compute_security_risk(security_risk_profile):
    counts = security_risk_profile["counts"]

    cm = _to_dict(counts, lambda i: (i["countType"], int(i["count"])))

    priorities = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'OK', 'UNKNOWN']

    for priority in priorities:
        if cm[priority] > 0:
            return priority

    return "OK"


@functools.total_ordering
class VersionInfo:
    """Parse and break apart version strings so they can be compared."""

    def __init__(self, ver_str):
        """Parse a version string input a tuple of ints or mark it as a beta release."""
        try:

            self.ver_str = ver_str
            self.production_version = True

            # Abseil has an empty string for one version
            if self.ver_str == "":
                self.production_version = False
                return

            # Special case Intel's Decimal library since it is just too weird
            if ver_str == "v2.0 U1":
                self.ver_array = [2, 0]
                return

            # BlackDuck thinks boost 1.70.0 was released on 2007 which means we have to check hundreds of versions
            bad_keywords = ["(", "+b", "-b", "b1", ".0a"]
            if [bad for bad in bad_keywords if bad in self.ver_str]:
                self.production_version = False
                return

            # Clean the version information
            # Some versions start with 'v'. Some components have a mix of 'v' and not 'v' prefixed versions so trim the 'v'
            # MongoDB versions start with 'r'
            if ver_str[0] == 'v' or ver_str[0] == 'r':
                self.ver_str = ver_str[1:]

            # Git hashes are not valid versions
            if len(self.ver_str) == 40 and bytes.fromhex(self.ver_str):
                self.production_version = False
                return

            # Clean out Mozilla's suffix
            self.ver_str = self.ver_str.replace("esr", "")

            # Clean out GPerfTool's prefix
            self.ver_str = self.ver_str.replace("gperftools-", "")

            # Clean out Yaml Cpp's prefix
            self.ver_str = self.ver_str.replace("yaml-cpp-", "")

            # Clean out Boosts's prefix
            self.ver_str = self.ver_str.replace("boost-", "")
            self.ver_str = self.ver_str.replace("asio-", "")

            if self.ver_str.endswith('-'):
                self.ver_str = self.ver_str[0:-1]

            # Boost keeps varying the version strings so filter for anything with 2 or more ascii charaters
            if RE_LETTERS.search(self.ver_str):
                self.production_version = False
                return

            # Some versions end with "-\d", change the "-" since it just means a patch release from a debian/rpm package
            # yaml-cpp has this problem where Black Duck sourced the wrong version information
            self.ver_str = self.ver_str.replace("-", ".")

            # If we trimmed the string to nothing, treat it as a beta version
            if self.ver_str == '':
                self.production_version = False
                return

            # Versions are generally a multi-part integer tuple
            self.ver_array = [int(part) for part in self.ver_str.split(".")]

        except:
            LOGGER.error("Failed to parse version '%s' as '%s', exception", ver_str, self.ver_str)
            raise

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return ".".join([str(val) for val in self.ver_array])

    def __eq__(self, other):
        return (self.production_version, self.ver_array) == (other.production_version,
                                                             other.ver_array)

    def __gt__(self, other):
        if self.production_version != other.production_version:
            return self.production_version

        return self.ver_array > other.ver_array


def _test_version_info():
    VersionInfo("v2.0 U1")
    VersionInfo("60.7.0-esr")
    VersionInfo("v1.1")
    VersionInfo("0.4.2-1")
    VersionInfo("7.0.2")
    VersionInfo("gperftools-2.8")
    VersionInfo("v1.5-rc2")
    VersionInfo("r4.7.0-alpha")
    VersionInfo("r4.2.10")
    VersionInfo("2.0.0.1")
    VersionInfo("7.0.2-2")
    VersionInfo("git")
    VersionInfo("20200225.2")
    VersionInfo('release-68-alpha')
    VersionInfo('cldr/2020-09-22')
    VersionInfo('release-67-rc')
    VersionInfo('66.1~rc')
    VersionInfo('release-66-rc')
    VersionInfo('release-66-preview')
    VersionInfo('65.1')
    VersionInfo('release-65-rc')
    VersionInfo('64.2-rc')
    VersionInfo('release-64-rc2')
    VersionInfo('release-63-rc')
    VersionInfo('last-cvs-commit')
    VersionInfo('last-svn-commit')
    VersionInfo('release-62-rc')
    VersionInfo('cldr-32-beta2')
    VersionInfo('release-60-rc')
    VersionInfo('milestone-60-0-1')
    VersionInfo('release-59-rc')
    VersionInfo('milestone-59-0-1')
    VersionInfo('release-58-2-eclipse-20170118')
    VersionInfo('tools-release-58')
    VersionInfo('icu-latest')
    VersionInfo('icu4j-latest')
    VersionInfo('icu4j-release-58-1')
    VersionInfo('icu4j-release-58-rc')
    VersionInfo('icu-release-58-rc')
    VersionInfo('icu-milestone-58-0-1')
    VersionInfo('icu4j-milestone-58-0-1')

    VersionInfo('yaml-cpp-0.6.3')

    VersionInfo('gb-c8-task188949.100')
    VersionInfo('1.2.8-alt1.M80C.1')
    VersionInfo('1.2.8-alt2')

    assert VersionInfo('7.0.2.2') > VersionInfo('7.0.0.1')
    assert VersionInfo('7.0.2.2') > VersionInfo('7.0.2')
    assert VersionInfo('7.0.2.2') > VersionInfo('3.1')
    assert VersionInfo('7.0.2.2') <= VersionInfo('8.0.2')


def _retry_on_except(count, func):
    # Retry func() COUNT times until func() does not raise an exception
    # pylint: disable=bare-except
    retry = 0
    while retry < count:

        try:
            return func()
        except:
            exception_info = sys.exc_info()[0]
            LOGGER.error("Failed to execute retriable function (%s), retrying", exception_info)

        retry += 1

    raise ValueError("Failed to run query after retries %s" % (count))


class Component:
    """
    Black Duck Component description.

    Contains a subset of information about a component extracted from Black Duck for a given project and version
    """

    def __init__(self, name, version, licenses, policy_status, security_risk, newest_release,
                 is_manually_added):
        # pylint: disable=too-many-arguments
        """Initialize Black Duck component."""
        self.name = name
        self.version = version
        self.licenses = licenses
        self.policy_status = policy_status
        self.security_risk = security_risk
        self.newest_release = newest_release
        self.is_manually_added = is_manually_added

    @staticmethod
    def parse(hub, component):
        # pylint: disable=too-many-locals
        """Parse a Black Duck component from a dictionary."""
        name = component["componentName"]
        cversion = component.get("componentVersionName", "unknown_version")
        licenses = ",".join([a.get("spdxId", a["licenseDisplay"]) for a in component["licenses"]])

        policy_status = component["policyStatus"]
        security_risk = _compute_security_risk(component['securityRiskProfile'])

        newer_releases = component["activityData"].get("newerReleases", 0)

        is_manually_added = 'MANUAL_BOM_COMPONENT' in component['matchTypes']

        LOGGER.info("Retrievinng version information for Comp %s - %s  Releases %s", name, cversion,
                    newer_releases)
        cver = VersionInfo(cversion)
        newest_release = None

        # Blackduck's newerReleases is based on "releasedOn" date. This means that if a upstream component releases a beta or rc,
        # it counts as newer but we do not consider those newer for our purposes
        # Missing newerReleases means we do not have to upgrade
        if newer_releases > 0:
            limit = newer_releases + 1
            versions_url = component["component"] + f"/versions?sort=releasedon%20desc&limit={limit}"

            LOGGER.info("Retrieving version information via %s", versions_url)

            def get_version_info():
                vjson = hub.execute_get(versions_url).json()

                if "items" not in vjson:
                    LOGGER.warn("Missing items in response: %s", vjson)
                    raise ValueError("Missing items in response for " + versions_url)

                return vjson

            vjson = _retry_on_except(5, get_version_info)

            versions = [(ver["versionName"], ver["releasedOn"]) for ver in vjson["items"]]

            LOGGER.info("Known versions: %s ", versions)

            versions = [ver["versionName"] for ver in vjson["items"]]

            # For Firefox, only examine Extended Service Releases (i.e. esr), their long term support releases
            if name == "Mozilla Firefox":
                versions = [ver for ver in versions if "esr" in ver]

            # For yaml-cpp, we need to clean the list of versions a little
            # yaml-cpp uses #.#.# but there are some entires with #.#.#.# so the later needs to
            # be filtered out.
            if name == "jbeder/yaml-cpp":
                ver_regex = re.compile(r"\d+\.\d+\.\d+$")
                versions = [ver for ver in versions if ver_regex.match(ver)]

            # For Boost C++ Libraries - boost, we need to clean the list of versions a little
            # All boost versions for the last 10 years start with 1.x.x. Black Duck thinks some
            # versions are 4.x.x which are bogus and throw off the sorting.
            # Also, boost uses #.#.# but there are some entires with #.#.#.# so the later needs to
            # be filtered out.
            if name == "Boost C++ Libraries - boost":
                ver_regex = re.compile(r"\d+\.\d+\.\d+$")
                versions = [ver for ver in versions if ver.startswith("1") and ver_regex.match(ver)]

            ver_info = [VersionInfo(ver) for ver in versions]
            ver_info = [ver for ver in ver_info if ver.production_version]
            LOGGER.info("Filtered versions: %s ", ver_info)

            ver_info = sorted([ver for ver in ver_info if ver.production_version and ver > cver],
                              reverse=True)

            LOGGER.info("Sorted versions: %s ", ver_info)

            if ver_info:
                newest_release = ver_info[0]

        return Component(name, cversion, licenses, policy_status, security_risk, newest_release,
                         is_manually_added)


class BlackDuckConfig:
    """
    Black Duck configuration settings.

    Format is defined by Black Duck Python hub API.
    """

    def __init__(self):
        """Init Black Duck config from disk."""
        if not os.path.exists(BLACKDUCK_RESTCONFIG):
            raise ValueError("Cannot find %s for blackduck configuration" % (BLACKDUCK_RESTCONFIG))

        with open(BLACKDUCK_RESTCONFIG, "r") as rfh:
            rc = json.loads(rfh.read())

        self.url = rc["baseurl"]
        self.username = rc["username"]
        self.password = rc["password"]


def _run_scan():
    # Get user name and password from .restconfig.json
    bdc = BlackDuckConfig()

    with tempfile.NamedTemporaryFile() as fp:
        fp.write(f"""#/!bin/sh
curl --retry 5 -s -L https://detect.synopsys.com/detect.sh  | bash -s -- --blackduck.url={bdc.url} --blackduck.username={bdc.username} --blackduck.password={bdc.password} --detect.report.timeout={BLACKDUCK_TIMEOUT_SECS} --snippet-matching --upload-source --detect.wait.for.results=true
""".encode())
        fp.flush()

        subprocess.call(["/bin/sh", fp.name])


def _scan_cmd_args(args):
    # pylint: disable=unused-argument
    LOGGER.info("Running Black Duck Scan")

    _run_scan()


def _query_blackduck():
    # pylint: disable=global-statement
    global BLACKDUCK_PROJECT_URL

    hub = HubInstance()

    LOGGER.info("Getting version from blackduck")
    version = hub.execute_get(hub.get_urlbase() + "/api/current-version").json()
    LOGGER.info("Version: %s", version)

    # Get a list of all projects, this is a privileged call and will fail if we do not have a valid license
    LOGGER.info("Get All Projects")
    projects = hub.get_projects()
    LOGGER.info("Projects: %s", projects)

    LOGGER.info("Fetching project %s from blackduck", BLACKDUCK_PROJECT)
    project = hub.get_project_by_name(BLACKDUCK_PROJECT)

    LOGGER.info("Fetching project version %s from blackduck", BLACKDUCK_PROJECT_VERSION)
    version = hub.get_version_by_name(project, BLACKDUCK_PROJECT_VERSION)

    LOGGER.info("Getting version components from blackduck")
    bom_components = hub.get_version_components(version)

    components = [
        Component.parse(hub, comp) for comp in bom_components["items"] if comp['ignored'] is False
    ]

    BLACKDUCK_PROJECT_URL = version["_meta"]["href"]

    return components


class TestResultEncoder(json.JSONEncoder):
    """JSONEncoder for TestResults."""

    def default(self, o):
        """Serialize objects by default as a dictionary."""
        # pylint: disable=method-hidden
        return o.__dict__


class TestResult:
    """A single test result in the Evergreen report.json format."""

    def __init__(self, name, status, url):
        """Init test result."""
        # This matches the report.json schema
        # See https://github.com/evergreen-ci/evergreen/blob/789bee107d3ffb9f0f82ae344d72502945bdc914/model/task/task.go#L264-L284
        assert status in ["pass", "fail"]

        self.test_file = name
        self.status = status
        self.exit_code = 1

        if url:
            self.url = url
            self.url_raw = url + "?raw=1"

        if status == "pass":
            self.exit_code = 0


class TestResults:
    """Evergreen TestResult format for report.json."""

    def __init__(self):
        """Init test results."""
        self.results = []

    def add_result(self, result: TestResult):
        """Add a test result."""
        self.results.append(result)

    def write(self, filename: str):
        """Write the test results to disk."""

        with open(filename, "w") as wfh:
            wfh.write(json.dumps(self, cls=TestResultEncoder))


class ReportLogger(object, metaclass=ABCMeta):
    """Base Class for all report loggers."""

    @abstractmethod
    def log_report(self, name: str, content: str) -> Optional[str]:
        """Get the command to run a linter."""
        pass


class LocalReportLogger(ReportLogger):
    """Write reports to local directory as a set of files."""

    def __init__(self):
        """Init logger and create directory."""
        if not os.path.exists(LOCAL_REPORTS_DIR):
            os.mkdir(LOCAL_REPORTS_DIR)

    def log_report(self, name: str, content: str) -> Optional[str]:
        """Log report to a local file."""
        file_name = os.path.join(LOCAL_REPORTS_DIR, name + ".log")

        with open(file_name, "w") as wfh:
            wfh.write(content)


class BuildLoggerReportLogger(ReportLogger):
    """Write reports to a build logger server."""

    def __init__(self, build_logger):
        """Init logger."""
        self.build_logger = build_logger

        self.build_id = self.build_logger.new_build_id("bdh")

    def log_report(self, name: str, content: str) -> Optional[str]:
        """Log report to a build logger."""

        content = content.split("\n")

        return self.build_logger.post_new_file(self.build_id, name, content)


def _get_default(list1, idx, default):
    if (idx + 1) < len(list1):
        return list1[idx]

    return default


class TableWriter:
    """Generate an ASCII table that summarizes the results of all the reports generated."""

    def __init__(self, headers: List[str]):
        """Init writer."""
        self._headers = headers
        self._rows = []

    def add_row(self, row: List[str]):
        """Add a row to the table."""
        self._rows.append(row)

    @staticmethod
    def _write_row(col_sizes: List[int], row: List[str], writer: io.StringIO):
        writer.write("|")
        for idx, row_value in enumerate(row):
            writer.write(" ")
            writer.write(row_value)
            writer.write(" " * (col_sizes[idx] - len(row_value)))
            writer.write(" |")
        writer.write("\n")

    def print(self, writer: io.StringIO):
        """Print the final table to the string stream."""
        cols = max([len(r) for r in self._rows])

        assert cols == len(self._headers)

        col_sizes = []
        for col in range(0, cols):
            col_sizes.append(
                max([len(_get_default(row, col, []))
                     for row in self._rows] + [len(self._headers[col])]))

        TableWriter._write_row(col_sizes, self._headers, writer)

        TableWriter._write_row(col_sizes, ["-" * c for c in col_sizes], writer)

        for row in self._rows:
            TableWriter._write_row(col_sizes, row, writer)


class TableData:
    """Store scalar values in a two-dimensional matrix indexed by the first column's value."""

    def __init__(self):
        """Init table data."""
        self._rows = {}

    def add_value(self, col: str, value: str):
        """Add a value for a given column. Order sensitive."""
        if col not in self._rows:
            self._rows[col] = []

        self._rows[col].append(value)

    def write(self, headers: List[str], writer: io.StringIO):
        """Write table data as nice prettty table to writer."""
        tw = TableWriter(headers)

        for row in self._rows:
            tw.add_row([row] + self._rows[row])

        tw.print(writer)


class ReportManager:
    """Manage logging reports to ReportLogger and generate summary report."""

    def __init__(self, logger: ReportLogger):
        """Init report manager."""
        self._logger = logger
        self._results = TestResults()
        self._data = TableData()

    @staticmethod
    def _get_norm_comp_name(comp_name: str):
        return comp_name.replace(" ", "_").replace("/", "_").lower()

    def write_report(self, comp_name: str, report_name: str, status: str, content: str):
        """
        Write a report about a test to the build logger.

        status is a string of "pass" or "fail"
        """
        comp_name = ReportManager._get_norm_comp_name(comp_name)

        name = comp_name + "_" + report_name

        LOGGER.info("Writing Report %s - %s", name, status)

        self._data.add_value(comp_name, status)

        url = self._logger.log_report(name, content)

        self._results.add_result(TestResult(name, status, url))

    def add_report_metric(self, comp_name: str, metric: str):
        """Add a column to be included in the pretty table."""
        comp_name = ReportManager._get_norm_comp_name(comp_name)

        self._data.add_value(comp_name, metric)

    def finish(self, reports_file: Optional[str], vulnerabilties_only: bool):
        """Generate final summary of all reports run."""

        if reports_file:
            self._results.write(reports_file)

        stream = io.StringIO()

        if vulnerabilties_only:
            self._data.write(["Component", "Vulnerability"], stream)
        else:
            self._data.write(
                ["Component", "Vulnerability", "Upgrade", "Current Version", "Newest Version"],
                stream)

        print(stream.getvalue())


class ThirdPartyComponent:
    """MongoDB Third Party component from third_party_components.yml."""

    def __init__(self, name, homepage_url, local_path, team_owner):
        """Init class."""
        # Required fields
        self.name = name
        self.homepage_url = homepage_url
        self.local_path = local_path
        self.team_owner = team_owner

        # optional fields
        self.is_test_only = False
        self.vulnerability_suppression = None
        self.upgrade_suppression = None


def _get_field(name, ymap, field: str):
    if field not in ymap:
        raise ValueError("Missing field %s for component %s" % (field, name))

    return ymap[field]


def _get_supression_field(ymap, field: str):
    if field not in ymap:
        return None

    value = ymap[field].lower()

    if not "todo" in value:
        raise ValueError(
            "Invalid suppression, a suppression must include the word 'TODO' so that the TODO scanner finds resolved tickets."
        )

    return value


def _read_third_party_components():
    with open(THIRD_PARTY_COMPONENTS_FILE) as rfh:
        yaml_file = yaml.load(rfh.read())

    third_party = []
    components = yaml_file["components"]
    for comp in components:
        cmap = components[comp]

        tp = ThirdPartyComponent(comp, _get_field(comp, cmap, 'homepage_url'),
                                 _get_field(comp, cmap, 'local_directory_path'),
                                 _get_field(comp, cmap, 'team_owner'))

        tp.is_test_only = cmap.get("is_test_only", False)
        tp.vulnerability_suppression = _get_supression_field(cmap, "vulnerability_suppression")
        tp.upgrade_suppression = _get_supression_field(cmap, "upgrade_suppression")

        third_party.append(tp)

    return third_party


def _generate_report_missing_blackduck_component(mgr: ReportManager, mcomp: ThirdPartyComponent):
    mgr.write_report(
        mcomp.name, "missing_blackduck_component", "fail", f"""{BLACKDUCK_FAILED_PREFIX}

The {mcomp.name} library was found in {THIRD_PARTY_COMPONENTS_FILE} but not detected by Black Duck.

This is caused by one of two issues:
1. The {THIRD_PARTY_COMPONENTS_FILE} file is out of date and the entry needs to be removed for "{mcomp.name}".
or
2. A entry to the component needs to be manually added to Black Duck.

Next Steps:

Build Baron:
A BF ticket should be generated and assigned to "{BLACKDUCK_DEFAULT_TEAM}" with
this text.

Developer:
To address this build failure, the next steps are as follows:
1. Verify that the component is correct in {THIRD_PARTY_COMPONENTS_FILE}.

2. If the component is incorrect, add a comment to the component in Black Duck and mark it as "Ignored".
or
2. If the component is correct, add the correct information to {THIRD_PARTY_COMPONENTS_FILE}.

If the "{BLACKDUCK_DEFAULT_TEAM}" cannot do this work for any reason, the BF should be assigned to
the component owner team "{mcomp.team_owner}".

For more information, see {BLACKDUCK_WIKI_PAGE}.
""")


def _generate_report_blackduck_missing_directory(mgr: ReportManager, directory: str):
    mgr.write_report(
        directory, "missing_directory", "fail", f"""{BLACKDUCK_FAILED_PREFIX}

The directory "{directory}" was found in a known MongoDB third_party directory but is not known to
Black Duck or {THIRD_PARTY_COMPONENTS_FILE}. This directory likely needs to be added to Black Duck
manually.

Next Steps:

Build Baron:
A BF ticket should be generated and assigned to "{BLACKDUCK_DEFAULT_TEAM}" with
this text.

Developer:
To address this build failure, the next steps are as follows:
1. Verify that the component is correct.
2. Add the component manually to the Black Duck project at
   {BLACKDUCK_PROJECT_URL}.
3. Once the component has been accepted by Black Duck, add the correct information to
   {THIRD_PARTY_COMPONENTS_FILE}.

For more information, see {BLACKDUCK_WIKI_PAGE}.
""")


def _generate_report_missing_yaml_component(mgr: ReportManager, comp: Component):
    mgr.write_report(
        comp.name, "missing_yaml_component", "fail", f"""{BLACKDUCK_FAILED_PREFIX}

The {comp.name} library with version "{comp.version}" was detected by Black Duck but not found in
{THIRD_PARTY_COMPONENTS_FILE}.

This is caused by one of two issues:
1. Black Duck has made an error and the software is not being vendored by MongoDB.
2. Black Duck is correct and {THIRD_PARTY_COMPONENTS_FILE} must be updated.

Next Steps:

Build Baron:
A BF ticket should be generated and assigned to "{BLACKDUCK_DEFAULT_TEAM}" with
this text.

Developer:
To address this build failure, the next steps are as follows:
1. Verify that the component is correct at {BLACKDUCK_PROJECT_URL}.

2. If the component is incorrect, add a comment to the component and mark it as "Ignored".
or
3. If the component is correct, add the correct information to {THIRD_PARTY_COMPONENTS_FILE}.

For more information, see {BLACKDUCK_WIKI_PAGE}.
""")


def _generate_report_upgrade(mgr: ReportManager, comp: Component, mcomp: ThirdPartyComponent,
                             fail: bool):
    if not fail:
        mgr.write_report(comp.name, "upgrade_check", "pass", "Blackduck run passed")
    else:

        if comp.is_manually_added:
            component_explanation = f"""This component requires a manual update in the Black Duck Database because it was added to
Black Duck manually.  After the update to the third-party library is committed, please update the
version information for this component at {BLACKDUCK_PROJECT_URL}. Click on the down arrow on the
far right of the component, choose edit and specify the new version."""
        else:
            component_explanation = """This commponent was automatically detected by Black Duck. Black Duck should automatically detect
the new version after the library is updated and the daily scanner task runs again."""

        mgr.write_report(
            comp.name, "upgrade_check", "fail", f"""{BLACKDUCK_FAILED_PREFIX}

The {comp.name} library at {mcomp.local_path} is out of date. The current version is
"{comp.version}" and the newest version is "{comp.newest_release}" according to Black Duck.

MongoDB policy requires all third-party software to be updated to the latest version on the master
branch.

Next Steps:

Build Baron:
A BF ticket should be generated and assigned to "{BLACKDUCK_DEFAULT_TEAM}" with
this text.

Developer:
To address this build failure, the next steps are as follows:
1. File a SERVER ticket to update the software if one already does not exist.
2. Add a “upgrade_supression” to {THIRD_PARTY_COMPONENTS_FILE} with the SERVER ticket to acknowledge
   this report. Note that you do not need to immediately update the library, just file a ticket.

{component_explanation}

If the "{BLACKDUCK_DEFAULT_TEAM}" cannot do this work for any reason, the BF should be assigned to
the component owner team "{mcomp.team_owner}".

For more information, see {BLACKDUCK_WIKI_PAGE}.
""")

    mgr.add_report_metric(comp.name, str(comp.version))
    mgr.add_report_metric(comp.name, str(comp.newest_release))


def _generate_report_vulnerability(mgr: ReportManager, comp: Component, mcomp: ThirdPartyComponent,
                                   fail: bool):
    if not fail:
        mgr.write_report(comp.name, "vulnerability_check", "pass", "Blackduck run passed")
        return

    mgr.write_report(
        comp.name, "vulnerability_check", "fail", f"""{BLACKDUCK_FAILED_PREFIX}

The {comp.name} library at {mcomp.local_path} had HIGH and/or CRITICAL security issues. The current
version in Black Duck is "{comp.version}".

MongoDB policy requires all third-party software to be updated to a version clean of HIGH and
CRITICAL vulnerabilities on the master branch.

Next Steps:

Build Baron:
A BF ticket should be generated and assigned to "{BLACKDUCK_DEFAULT_TEAM}" with
this text.

Developer:
To address this build failure, the next steps are as follows:
1. File a SERVER ticket to update the software if one already does not exist. Note that you do not
   need to immediately update the library, just file a ticket.
2. Add a “vulnerability_supression” to {THIRD_PARTY_COMPONENTS_FILE} with the SERVER ticket to
   acknowledge this report.

If you believe the library is already up-to-date but Black Duck has the wrong version, please update
version information for this component at {BLACKDUCK_PROJECT_URL}.

If the "{BLACKDUCK_DEFAULT_TEAM}" cannot do this work for any reason, the BF should be assigned to
the component owner team "{mcomp.team_owner}".

For more information, see {BLACKDUCK_WIKI_PAGE}.
""")


def _get_third_party_directories():
    third_party = []
    for tp in THIRD_PARTY_DIRECTORIES:
        for entry in os.scandir(tp):
            if entry.name not in ["scripts"] and entry.is_dir():
                third_party.append(entry.path)

    return sorted(third_party)


class Analyzer:
    """
    Analyze the MongoDB source code for software maintence issues.

    Queries Black Duck for out of date software
    Consults a local yaml file for detailed information about third party components included in the MongoDB source code.
    """

    def __init__(self):
        """Init analyzer."""
        self.third_party_components = None
        self.third_party_directories = None
        self.black_duck_components = None
        self.mgr = None

    def _do_reports(self, vulnerabilties_only: bool):
        for comp in self.black_duck_components:
            # 1. Validate if this is in the YAML file
            if self._verify_yaml_contains_component(comp, vulnerabilties_only):

                # 2. Validate there are no security issues
                self._verify_vulnerability_status(comp)

                # 3. Check for upgrade issue
                if not vulnerabilties_only:
                    self._verify_upgrade_status(comp)

        if vulnerabilties_only:
            return

        # 4. Validate that each third_party directory is in the YAML file
        self._verify_directories_in_yaml()

        # 5. Verify the YAML file has all the entries in Black Duck
        self._verify_components_in_yaml()

    def _verify_yaml_contains_component(self, comp: Component, vulnerabilties_only: bool):
        # It should be rare that Black Duck detects something that is not in the YAML file
        # As a result, we do not generate a "pass" report for simply be consistent between Black Duck and the yaml file
        if comp.name not in [c.name for c in self.third_party_components]:
            if not vulnerabilties_only:
                _generate_report_missing_yaml_component(self.mgr, comp)
            return False

        return True

    def _verify_directories_in_yaml(self):

        comp_dirs = [c.local_path for c in self.third_party_components]
        for cdir in self.third_party_directories:
            # Ignore WiredTiger since it is not a third-party library but made by MongoDB, Inc.
            if cdir in ["src/third_party/wiredtiger"]:
                continue

            if cdir not in comp_dirs:
                _generate_report_blackduck_missing_directory(self.mgr, cdir)

    def _verify_components_in_yaml(self):

        comp_names = [c.name for c in self.black_duck_components]
        for mcomp in self.third_party_components:
            # These components are known to be missing from Black Duck
            # Aladdin MD5 is a pair of C files for MD5 computation
            # timelib is simply missing
            # Unicode is not code
            if mcomp.name in ["Aladdin MD5", "timelib", "unicode"]:
                continue

            if mcomp.name not in comp_names:
                _generate_report_missing_blackduck_component(self.mgr, mcomp)

    def _verify_upgrade_status(self, comp: Component):
        mcomp = self._get_mongo_component(comp)

        if comp.newest_release and not mcomp.upgrade_suppression and not mcomp.is_test_only:
            _generate_report_upgrade(self.mgr, comp, mcomp, True)
        else:
            _generate_report_upgrade(self.mgr, comp, mcomp, False)

    def _verify_vulnerability_status(self, comp: Component):
        mcomp = self._get_mongo_component(comp)

        if comp.security_risk in [
                "HIGH", "CRITICAL"
        ] and not mcomp.vulnerability_suppression and not mcomp.is_test_only:
            _generate_report_vulnerability(self.mgr, comp, mcomp, True)
        else:
            _generate_report_vulnerability(self.mgr, comp, mcomp, False)

    def _get_mongo_component(self, comp: Component):
        mcomp = next((x for x in self.third_party_components if x.name == comp.name), None)

        if not mcomp:
            raise ValueError(
                "Cannot find third party component for Black Duck Component '%s'. Please update '%s'. "
                % (comp.name, THIRD_PARTY_COMPONENTS_FILE))

        return mcomp

    def run(self, logger: ReportLogger, report_file: Optional[str], vulnerabilties_only: bool):
        """Run analysis of Black Duck scan and local files."""

        self.third_party_directories = _get_third_party_directories()

        LOGGER.info("Found the following third party directories: %s", self.third_party_directories)

        self.third_party_components = _read_third_party_components()

        self.black_duck_components = _query_blackduck()

        # Black Duck detects ourself everytime we release a new version
        # Rather then constantly have to supress this in Black Duck itself which will generate false positives
        # We filter ourself our of the list of components.
        self.black_duck_components = [
            comp for comp in self.black_duck_components
            if not (comp.name == "MongoDB" or comp.name == "WiredTiger")
        ]

        # Remove duplicate Black Duck components. We only care about the component with highest version number
        # Black Duck can detect different versions of the same component for instance if an upgrade of a component happens
        bd_names = {comp.name for comp in self.black_duck_components}
        if len(bd_names) != len(self.black_duck_components):
            LOGGER.warning("Found duplicate Black Duck components")
            bd_unique = {}
            for comp in self.black_duck_components:
                if comp.name in bd_unique:
                    LOGGER.warning("Found duplicate Black Duck component: %s", comp.name)
                    first = bd_unique[comp.name]
                    if comp.version > first.version:
                        bd_unique[comp.name] = comp
                else:
                    bd_unique[comp.name] = comp

            self.black_duck_components = list(bd_unique.values())

        self.mgr = ReportManager(logger)

        self._do_reports(vulnerabilties_only)

        self.mgr.finish(report_file, vulnerabilties_only)


# Derived from buildscripts/resmokelib/logging/buildlogger.py
def _get_build_logger_from_file(filename, build_logger_url, task_id):
    tmp_globals = {}
    config = {}

    # The build logger config file is actually python
    # It is a mix of quoted strings and ints
    exec(compile(open(filename, "rb").read(), filename, 'exec'), tmp_globals, config)

    # Rename "slavename" to "username" if present.
    if "slavename" in config and "username" not in config:
        config["username"] = config["slavename"]
        del config["slavename"]

    # Rename "passwd" to "password" if present.
    if "passwd" in config and "password" not in config:
        config["password"] = config["passwd"]
        del config["passwd"]

    return BuildloggerServer(config["username"], config["password"], task_id, config["builder"],
                             config["build_num"], config["build_phase"], build_logger_url)


def _generate_reports_args(args):
    LOGGER.info("Generating Reports")

    # Log to LOCAL_REPORTS_DIR directory unless build logger is explicitly chosen
    logger = LocalReportLogger()

    if args.build_logger_local:
        build_logger = BuildloggerServer("fake_user", "fake_pass", "fake_task", "fake_builder", 1,
                                         "fake_build_phase", "http://localhost:8080")
        logger = BuildLoggerReportLogger(build_logger)
    elif args.build_logger:
        if not args.build_logger_task_id:
            raise ValueError("Must set build_logger_task_id if using build logger")

        build_logger = _get_build_logger_from_file(args.build_logger, args.build_logger_url,
                                                   args.build_logger_task_id)
        logger = BuildLoggerReportLogger(build_logger)

    analyzer = Analyzer()
    analyzer.run(logger, args.report_file, args.vulnerabilities_only)


def _scan_and_report_args(args):
    LOGGER.info("Running Black Duck Scan And Generating Reports")

    _run_scan()

    _generate_reports_args(args)


def main() -> None:
    """Execute Main entry point."""

    parser = argparse.ArgumentParser(description='Black Duck hub controller.')

    parser.add_argument('-v', "--verbose", action='store_true', help="Enable verbose logging")
    parser.add_argument('-d', "--debug", action='store_true', help="Enable debug logging")

    sub = parser.add_subparsers(title="Hub subcommands", help="sub-command help")
    generate_reports_cmd = sub.add_parser('generate_reports',
                                          help='Generate reports from Black Duck')

    generate_reports_cmd.add_argument("--report_file", type=str,
                                      help="report json file to write to")
    generate_reports_cmd.add_argument(
        "--build_logger", type=str, help="Log to build logger with credentials from specified file")
    generate_reports_cmd.add_argument("--build_logger_url", type=str,
                                      default=BUILD_LOGGER_DEFAULT_URL,
                                      help="build logger url to log to")
    generate_reports_cmd.add_argument("--build_logger_task_id", type=str,
                                      help="build logger task id")
    generate_reports_cmd.add_argument("--build_logger_local", action='store_true',
                                      help="Log to local build logger, logs to disk by default")
    generate_reports_cmd.add_argument("--vulnerabilities_only", action='store_true',
                                      help="Only check for security vulnerabilities")
    generate_reports_cmd.set_defaults(func=_generate_reports_args)

    scan_cmd = sub.add_parser('scan', help='Do Black Duck Scan')
    scan_cmd.set_defaults(func=_scan_cmd_args)

    scan_and_report_cmd = sub.add_parser('scan_and_report',
                                         help='Run scan and then generate reports')
    scan_and_report_cmd.add_argument("--report_file", type=str, help="report json file to write to")

    scan_and_report_cmd.add_argument(
        "--build_logger", type=str, help="Log to build logger with credentials from specified file")
    scan_and_report_cmd.add_argument("--build_logger_url", type=str,
                                     default=BUILD_LOGGER_DEFAULT_URL,
                                     help="build logger url to log to")
    scan_and_report_cmd.add_argument("--build_logger_task_id", type=str,
                                     help="build logger task id")
    scan_and_report_cmd.add_argument("--build_logger_local", action='store_true',
                                     help="Log to local build logger, logs to disk by default")
    scan_and_report_cmd.add_argument("--vulnerabilities_only", action='store_true',
                                     help="Only check for security vulnerabilities")
    scan_and_report_cmd.set_defaults(func=_scan_and_report_args)

    args = parser.parse_args()

    _test_version_info()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO)

    args.func(args)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Generate burn in tests to run on certain build variants."""
from collections import namedtuple
import logging
import os
import sys
from typing import Any, Dict, List

import click

from git import Repo
from shrub.v2 import ShrubProject, BuildVariant, ExistingTask
from evergreen.api import RetryingEvergreenApi, EvergreenApi

# Get relative imports to work when the package is not installed on the PYTHONPATH.
from buildscripts.patch_builds.task_generation import validate_task_generation_limit

if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pylint: disable=wrong-import-position
from buildscripts.evergreen_burn_in_tests import GenerateBurnInExecutor, GenerateConfig, \
    EvergreenFileChangeDetector
from buildscripts.util.fileops import write_file_to_dir
import buildscripts.util.read_config as read_config
from buildscripts.ciconfig import evergreen
from buildscripts.ciconfig.evergreen import EvergreenProjectConfig, Variant
from buildscripts.burn_in_tests import create_tests_by_task, RepeatConfig, DEFAULT_REPO_LOCATIONS
# pylint: enable=wrong-import-position

EXTERNAL_LOGGERS = {
    "evergreen",
    "git",
    "urllib3",
}
CONFIG_DIRECTORY = "generated_burn_in_tags_config"
CONFIG_FILE = "burn_in_tags_gen.json"
EVERGREEN_FILE = "etc/evergreen.yml"
EVG_CONFIG_FILE = ".evergreen.yml"
COMPILE_TASK = "compile_and_archive_dist_test_TG"
TASK_ID_EXPANSION = "task_id"

ConfigOptions = namedtuple("ConfigOptions", [
    "build_variant",
    "run_build_variant",
    "base_commit",
    "max_revisions",
    "branch",
    "check_evergreen",
    "distro",
    "repeat_tests_secs",
    "repeat_tests_min",
    "repeat_tests_max",
    "project",
])


def _get_config_options(expansions_file_data, build_variant, run_build_variant):
    """
    Get the configuration to use.

    :param expansions_file_data: Config data file to use.
    :param build_variant: The buildvariant the current patch should be compared against to figure
        out which tests have changed.
    :param run_build_variant: The buildvariant the generated task should be run on.
    :return: ConfigOptions for the generated task to use.
    """
    base_commit = expansions_file_data["revision"]
    max_revisions = int(expansions_file_data["max_revisions"])
    branch = expansions_file_data["branch_name"]
    is_patch = expansions_file_data.get("is_patch", False)
    check_evergreen = is_patch != "true"
    distro = expansions_file_data["distro_id"]
    repeat_tests_min = int(expansions_file_data["repeat_tests_min"])
    repeat_tests_max = int(expansions_file_data["repeat_tests_max"])
    repeat_tests_secs = int(expansions_file_data["repeat_tests_secs"])
    project = expansions_file_data["project"]

    return ConfigOptions(build_variant, run_build_variant, base_commit, max_revisions, branch,
                         check_evergreen, distro, repeat_tests_secs, repeat_tests_min,
                         repeat_tests_max, project)


def _create_evg_build_variant_map(expansions_file_data, evergreen_conf):
    """
    Generate relationship of base buildvariant to generated buildvariant.

    :param expansions_file_data: Config data file to use.
    :param evergreen_conf: Evergreen configuration.
    :return: Map of base buildvariants to their generated buildvariants.
    """
    burn_in_tags_gen_variant = expansions_file_data["build_variant"]
    burn_in_tags_gen_variant_config = evergreen_conf.get_variant(burn_in_tags_gen_variant)
    burn_in_tag_build_variants = burn_in_tags_gen_variant_config.expansions.get(
        "burn_in_tag_buildvariants")

    if burn_in_tag_build_variants:
        return {
            base_variant: f"{base_variant}-required"
            for base_variant in burn_in_tag_build_variants.split(" ")
        }

    return {}


def _generate_evg_build_variant(
        source_build_variant: Variant,
        run_build_variant: str,
        bypass_build_variant: str,
) -> BuildVariant:
    """
    Generate a shrub build variant for the given run build variant.

    :param source_build_variant: The build variant to base configuration on.
    :param run_build_variant: The build variant to generate.
    :param bypass_build_variant: The build variant to get compile artifacts from.
    :return: Shrub build variant configuration.
    """
    display_name = f"! {source_build_variant.display_name}"
    run_on = source_build_variant.run_on
    modules = source_build_variant.modules

    expansions = source_build_variant.expansions
    expansions["burn_in_bypass"] = bypass_build_variant

    build_variant = BuildVariant(run_build_variant, display_name, expansions=expansions,
                                 modules=modules, run_on=run_on)
    build_variant.add_existing_task(ExistingTask(COMPILE_TASK))
    return build_variant


# pylint: disable=too-many-arguments,too-many-locals
def _generate_evg_tasks(evergreen_api: EvergreenApi, shrub_project: ShrubProject,
                        task_expansions: Dict[str, Any], build_variant_map: Dict[str, str],
                        repos: List[Repo], evg_conf: EvergreenProjectConfig) -> None:
    """
    Generate burn in tests tasks for a given shrub config and group of build variants.

    :param evergreen_api: Evergreen.py object.
    :param shrub_project: Shrub config object that the build variants will be built upon.
    :param task_expansions: Dictionary of expansions for the running task.
    :param build_variant_map: Map of base buildvariants to their generated buildvariant.
    :param repos: Git repositories.
    """
    for build_variant, run_build_variant in build_variant_map.items():
        config_options = _get_config_options(task_expansions, build_variant, run_build_variant)
        task_id = task_expansions[TASK_ID_EXPANSION]
        change_detector = EvergreenFileChangeDetector(task_id, evergreen_api)
        changed_tests = change_detector.find_changed_tests(repos)
        tests_by_task = create_tests_by_task(build_variant, evg_conf, changed_tests)
        if tests_by_task:
            shrub_build_variant = _generate_evg_build_variant(
                evg_conf.get_variant(build_variant), run_build_variant,
                task_expansions["build_variant"])
            gen_config = GenerateConfig(build_variant, config_options.project, run_build_variant,
                                        config_options.distro,
                                        include_gen_task=False).validate(evg_conf)
            repeat_config = RepeatConfig(repeat_tests_min=config_options.repeat_tests_min,
                                         repeat_tests_max=config_options.repeat_tests_max,
                                         repeat_tests_secs=config_options.repeat_tests_secs)

            burn_in_generator = GenerateBurnInExecutor(gen_config, repeat_config, evergreen_api)
            burn_in_generator.add_config_for_build_variant(shrub_build_variant, tests_by_task)
            shrub_project.add_build_variant(shrub_build_variant)


def burn_in(task_expansions: Dict[str, Any], evg_conf: EvergreenProjectConfig,
            evergreen_api: RetryingEvergreenApi, repos: List[Repo]):
    """
    Execute main program.

    :param task_expansions: Dictionary of expansions for the running task.
    :param evg_conf: Evergreen configuration.
    :param evergreen_api: Evergreen.py object.
    :param repos: Git repositories.
    """
    shrub_project = ShrubProject.empty()
    build_variant_map = _create_evg_build_variant_map(task_expansions, evg_conf)
    _generate_evg_tasks(evergreen_api, shrub_project, task_expansions, build_variant_map, repos,
                        evg_conf)

    if not validate_task_generation_limit(shrub_project):
        sys.exit(1)
    write_file_to_dir(CONFIG_DIRECTORY, CONFIG_FILE, shrub_project.json())


def _configure_logging(verbose: bool):
    """
    Configure logging for the application.

    :param verbose: If True set log level to DEBUG.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="[%(asctime)s - %(name)s - %(levelname)s] %(message)s",
        level=level,
        stream=sys.stdout,
    )
    for log_name in EXTERNAL_LOGGERS:
        logging.getLogger(log_name).setLevel(logging.WARNING)


@click.command()
@click.option("--expansion-file", "expansion_file", required=True,
              help="Location of expansions file generated by evergreen.")
@click.option("--verbose", is_flag=True)
def main(expansion_file: str, verbose: bool):
    """
    Run new or changed tests in repeated mode to validate their stability.

    burn_in_tags detects jstests that are new or changed since the last git command and then
    runs those tests in a loop to validate their reliability.

    \f
    :param expansion_file: The expansion file containing the configuration params.
    """
    _configure_logging(verbose)
    evg_api = RetryingEvergreenApi.get_api(config_file=EVG_CONFIG_FILE)
    repos = [Repo(x) for x in DEFAULT_REPO_LOCATIONS if os.path.isdir(x)]
    expansions_file_data = read_config.read_config_file(expansion_file)
    evg_conf = evergreen.parse_evergreen_file(EVERGREEN_FILE)

    burn_in(expansions_file_data, evg_conf, evg_api, repos)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

#!/usr/bin/env python3
"""Command line utility for determining what jstests have been added or modified."""
import copy
import logging
import os.path
import shlex
import subprocess
import sys
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Optional, Set, Tuple, List, Dict, NamedTuple

import click
import yaml
from git import Repo
import structlog
from structlog.stdlib import LoggerFactory

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pylint: disable=wrong-import-position
from buildscripts.patch_builds.change_data import generate_revision_map, \
    RevisionMap, find_changed_files_in_repos
import buildscripts.resmokelib.parser
from buildscripts.resmokelib.suitesconfig import create_test_membership_map, get_suites
from buildscripts.resmokelib.utils import default_if_none, globstar
from buildscripts.ciconfig.evergreen import parse_evergreen_file, ResmokeArgs, \
    EvergreenProjectConfig, Variant, VariantTask
# pylint: enable=wrong-import-position

structlog.configure(logger_factory=LoggerFactory())
LOGGER = structlog.getLogger(__name__)
EXTERNAL_LOGGERS = {
    "evergreen",
    "git",
    "urllib3",
}

DEFAULT_VARIANT = "enterprise-rhel-80-64-bit-dynamic-required"
ENTERPRISE_MODULE_PATH = "src/mongo/db/modules/enterprise"
DEFAULT_REPO_LOCATIONS = [".", f"./{ENTERPRISE_MODULE_PATH}"]
REPEAT_SUITES = 2
EVERGREEN_FILE = "etc/evergreen.yml"
# The executor_file and suite_files defaults are required to make the suite resolver work
# correctly.
SELECTOR_FILE = "etc/burn_in_tests.yml"
SUITE_FILES = ["with_server"]

SUPPORTED_TEST_KINDS = ("fsm_workload_test", "js_test", "json_schema_test",
                        "multi_stmt_txn_passthrough", "parallel_fsm_workload_test")


class RepeatConfig(object):
    """Configuration for how tests should be repeated."""

    def __init__(self, repeat_tests_secs: Optional[int] = None,
                 repeat_tests_min: Optional[int] = None, repeat_tests_max: Optional[int] = None,
                 repeat_tests_num: Optional[int] = None):
        """
        Create a Repeat Config.

        :param repeat_tests_secs: Repeat test for this number of seconds.
        :param repeat_tests_min: Repeat the test at least this many times.
        :param repeat_tests_max: At most repeat the test this many times.
        :param repeat_tests_num: Repeat the test exactly this many times.
        """
        self.repeat_tests_secs = repeat_tests_secs
        self.repeat_tests_min = repeat_tests_min
        self.repeat_tests_max = repeat_tests_max
        self.repeat_tests_num = repeat_tests_num

    def validate(self):
        """
        Raise an exception if this configuration is invalid.

        :return: self.
        """
        if self.repeat_tests_num and self.repeat_tests_secs:
            raise ValueError("Cannot specify --repeat-tests and --repeat-tests-secs")

        if self.repeat_tests_max:
            if not self.repeat_tests_secs:
                raise ValueError("Must specify --repeat-tests-secs with --repeat-tests-max")

            if self.repeat_tests_min and self.repeat_tests_min > self.repeat_tests_max:
                raise ValueError("--repeat-tests-secs-min is greater than --repeat-tests-max")

        if self.repeat_tests_min and not self.repeat_tests_secs:
            raise ValueError("Must specify --repeat-tests-secs with --repeat-tests-min")
        return self

    def generate_resmoke_options(self) -> str:
        """
        Generate the resmoke options to repeat a test.

        :return: Resmoke options to repeat a test.
        """
        if self.repeat_tests_secs:
            repeat_options = f" --repeatTestsSecs={self.repeat_tests_secs} "
            if self.repeat_tests_min:
                repeat_options += f" --repeatTestsMin={self.repeat_tests_min} "
            if self.repeat_tests_max:
                repeat_options += f" --repeatTestsMax={self.repeat_tests_max} "
            return repeat_options

        repeat_suites = self.repeat_tests_num if self.repeat_tests_num else REPEAT_SUITES
        return f" --repeatSuites={repeat_suites} "

    def __repr__(self):
        """Build string representation of object for debugging."""
        return "".join([
            f"RepeatConfig[num={self.repeat_tests_num}, secs={self.repeat_tests_secs}, ",
            f"min={self.repeat_tests_min}, max={self.repeat_tests_max}]",
        ])


def is_file_a_test_file(file_path: str) -> bool:
    """
    Check if the given path points to a test file.

    :param file_path: path to file.
    :return: True if path points to test.
    """
    # Check that the file exists because it may have been moved or deleted in the patch.
    if os.path.splitext(file_path)[1] != ".js" or not os.path.isfile(file_path):
        return False

    if "jstests" not in file_path:
        return False

    return True


def find_excludes(selector_file: str) -> Tuple[List, List, List]:
    """Parse etc/burn_in_tests.yml. Returns lists of excluded suites, tasks & tests."""

    if not selector_file:
        return [], [], []

    LOGGER.debug("reading configuration", config_file=selector_file)
    with open(selector_file, "r") as fstream:
        yml = yaml.safe_load(fstream)

    try:
        js_test = yml["selector"]["js_test"]
    except KeyError:
        raise Exception(f"The selector file {selector_file} is missing the 'selector.js_test' key")

    return (default_if_none(js_test.get("exclude_suites"), []),
            default_if_none(js_test.get("exclude_tasks"), []),
            default_if_none(js_test.get("exclude_tests"), []))


def filter_tests(tests: Set[str], exclude_tests: List[str]) -> Set[str]:
    """
    Exclude tests which have been denylisted.

    :param tests: Set of tests to filter.
    :param exclude_tests: Tests to filter out.
    :return: Set of tests with exclude_tests filtered out.
    """
    if not exclude_tests or not tests:
        return tests

    # The exclude_tests can be specified using * and ** to specify directory and file patterns.
    excluded_globbed = set()
    for exclude_test_pattern in exclude_tests:
        excluded_globbed.update(globstar.iglob(exclude_test_pattern))

    LOGGER.debug("Excluding test pattern", excluded=excluded_globbed)
    return tests - excluded_globbed


def create_executor_list(suites, exclude_suites):
    """Create the executor list.

    Looks up what other resmoke suites run the tests specified in the suites
    parameter. Returns a dict keyed by suite name / executor, value is tests
    to run under that executor.
    """
    test_membership = create_test_membership_map(test_kind=SUPPORTED_TEST_KINDS)

    memberships = defaultdict(list)
    for suite in suites:
        LOGGER.debug("Adding tests for suite", suite=suite, tests=suite.tests)
        for test in suite.tests:
            LOGGER.debug("membership for test", test=test, membership=test_membership[test])
            for executor in set(test_membership[test]) - set(exclude_suites):
                if test not in memberships[executor]:
                    memberships[executor].append(test)
    return memberships


def _get_task_name(task):
    """
    Return the task var from a "generate resmoke task" instead of the task name.

    :param task: task to get name of.
    """

    if task.is_generate_resmoke_task:
        return task.generated_task_name

    return task.name


def _set_resmoke_args(task):
    """
    Set the resmoke args to include the --suites option.

    The suite name from "generate resmoke tasks" can be specified as a var or directly in the
    resmoke_args.
    """

    resmoke_args = task.combined_resmoke_args
    suite_name = ResmokeArgs.get_arg(resmoke_args, "suites")
    if task.is_generate_resmoke_task:
        suite_name = task.get_vars_suite_name(task.generate_resmoke_tasks_command["vars"])

    return ResmokeArgs.set_updated_arg(resmoke_args, "suites", suite_name)


def _distro_to_run_task_on(task: VariantTask, evg_proj_config: EvergreenProjectConfig,
                           build_variant: str) -> str:
    """
    Determine what distro an task should be run on.

    For normal tasks, the distro will be the default for the build variant unless the task spec
    specifies a particular distro to run on.

    For generated tasks, the distro will be the default for the build variant unless (1) the
    "use_large_distro" flag is set as a "var" in the "generate resmoke tasks" command of the
    task definition and (2) the build variant defines the "large_distro_name" in its expansions.

    :param task: Task being run.
    :param evg_proj_config: Evergreen project configuration.
    :param build_variant: Build Variant task is being run on.
    :return: Distro task should be run on.
    """
    task_def = evg_proj_config.get_task(task.name)
    if task_def.is_generate_resmoke_task:
        resmoke_vars = task_def.generate_resmoke_tasks_command["vars"]
        if "use_large_distro" in resmoke_vars:
            evg_build_variant = _get_evg_build_variant_by_name(evg_proj_config, build_variant)
            if "large_distro_name" in evg_build_variant.raw["expansions"]:
                return evg_build_variant.raw["expansions"]["large_distro_name"]

    return task.run_on[0]


class TaskInfo(NamedTuple):
    """
    Information about tests to run under a specific Task.

    display_task_name: Display name of task.
    resmoke_args: Arguments to provide to resmoke on task invocation.
    tests: List of tests to run as part of task.
    require_multiversion: Requires downloading Multiversion binaries.
    distro: Evergreen distro task runs on.
    """

    display_task_name: str
    resmoke_args: str
    tests: List[str]
    require_multiversion: Optional[bool]
    distro: str

    @classmethod
    def from_task(cls, task: VariantTask, tests_by_suite: Dict[str, List[str]],
                  evg_proj_config: EvergreenProjectConfig, build_variant: str) -> "TaskInfo":
        """
        Gather the information needed to run the given task.

        :param task: Task to be run.
        :param tests_by_suite: Dict of suites.
        :param evg_proj_config: Evergreen project configuration.
        :param build_variant: Build variant task will be run on.
        :return: Dictionary of information needed to run task.
        """
        return cls(
            display_task_name=_get_task_name(task), resmoke_args=_set_resmoke_args(task),
            tests=tests_by_suite[task.resmoke_suite],
            require_multiversion=task.require_multiversion, distro=_distro_to_run_task_on(
                task, evg_proj_config, build_variant))


def create_task_list(evergreen_conf: EvergreenProjectConfig, build_variant: str,
                     tests_by_suite: Dict[str, List[str]],
                     exclude_tasks: [str]) -> Dict[str, TaskInfo]:
    """
    Find associated tasks for the specified build_variant and suites.

    :param evergreen_conf: Evergreen configuration for project.
    :param build_variant: Build variant to select tasks from.
    :param tests_by_suite: Suites to be run.
    :param exclude_tasks: Tasks to exclude.
    :return: Dict of tasks to run with run configuration.
    """
    log = LOGGER.bind(build_variant=build_variant)

    log.debug("creating task list for suites", suites=tests_by_suite, exclude_tasks=exclude_tasks)
    evg_build_variant = _get_evg_build_variant_by_name(evergreen_conf, build_variant)

    # Find all the build variant tasks.
    exclude_tasks_set = set(exclude_tasks)
    all_variant_tasks = {
        task.name: task
        for task in evg_build_variant.tasks
        if task.name not in exclude_tasks_set and task.combined_resmoke_args
    }

    # Return the list of tasks to run for the specified suite.
    task_list = {
        task_name: TaskInfo.from_task(task, tests_by_suite, evergreen_conf, build_variant)
        for task_name, task in all_variant_tasks.items() if task.resmoke_suite in tests_by_suite
    }

    log.debug("Found task list", task_list=task_list)
    return task_list


def _set_resmoke_cmd(repeat_config: RepeatConfig, resmoke_args: [str]) -> [str]:
    """Build the resmoke command, if a resmoke.py command wasn't passed in."""
    new_args = [sys.executable, "buildscripts/resmoke.py", "run"]
    if resmoke_args:
        new_args = copy.deepcopy(resmoke_args)

    new_args += repeat_config.generate_resmoke_options().split()
    LOGGER.debug("set resmoke command", new_args=new_args)
    return new_args


def create_task_list_for_tests(changed_tests: Set[str], build_variant: str,
                               evg_conf: EvergreenProjectConfig,
                               exclude_suites: Optional[List] = None,
                               exclude_tasks: Optional[List] = None) -> Dict[str, TaskInfo]:
    """
    Create a list of tests by task for the given tests.

    :param changed_tests: Set of test that have changed.
    :param build_variant: Build variant to collect tasks from.
    :param evg_conf: Evergreen configuration.
    :param exclude_suites: Suites to exclude.
    :param exclude_tasks: Tasks to exclude.
    :return: Tests by task.
    """
    if not exclude_suites:
        exclude_suites = []
    if not exclude_tasks:
        exclude_tasks = []

    suites = get_suites(suite_files=SUITE_FILES, test_files=changed_tests)
    LOGGER.debug("Found suites to run", suites=suites)

    tests_by_executor = create_executor_list(suites, exclude_suites)
    LOGGER.debug("tests_by_executor", tests_by_executor=tests_by_executor)

    return create_task_list(evg_conf, build_variant, tests_by_executor, exclude_tasks)


def create_tests_by_task(build_variant: str, evg_conf: EvergreenProjectConfig,
                         changed_tests: Set[str]) -> Dict[str, TaskInfo]:
    """
    Create a list of tests by task.

    :param build_variant: Build variant to collect tasks from.
    :param evg_conf: Evergreen configuration.
    :param changed_tests: Set of changed test files.
    :return: Tests by task.
    """
    exclude_suites, exclude_tasks, exclude_tests = find_excludes(SELECTOR_FILE)
    evg_build_variant = _get_evg_build_variant_by_name(evg_conf, build_variant)
    if not evg_build_variant.is_enterprise_build():
        exclude_tests.append(f"{ENTERPRISE_MODULE_PATH}/**/*")
    changed_tests = filter_tests(changed_tests, exclude_tests)

    buildscripts.resmokelib.parser.set_run_options()
    if changed_tests:
        return create_task_list_for_tests(changed_tests, build_variant, evg_conf, exclude_suites,
                                          exclude_tasks)

    LOGGER.info("No new or modified tests found.")
    return {}


def run_tests(tests_by_task: Dict[str, TaskInfo], resmoke_cmd: [str]) -> None:
    """
    Run the given tests locally.

    This function will exit with a non-zero return code on test failure.

    :param tests_by_task: Dictionary of tests to run.
    :param resmoke_cmd: Parameter to use when calling resmoke.
    """
    for task in sorted(tests_by_task):
        log = LOGGER.bind(task=task)
        new_resmoke_cmd = copy.deepcopy(resmoke_cmd)
        new_resmoke_cmd.extend(shlex.split(tests_by_task[task].resmoke_args))
        new_resmoke_cmd.extend(tests_by_task[task].tests)
        log.debug("starting execution of task")
        try:
            subprocess.check_call(new_resmoke_cmd, shell=False)
        except subprocess.CalledProcessError as err:
            log.warning("Resmoke returned an error with task", error=err.returncode)
            sys.exit(err.returncode)


def _configure_logging(verbose: bool):
    """
    Configure logging for the application.

    :param verbose: If True set log level to DEBUG.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="[%(asctime)s - %(name)s - %(levelname)s] %(message)s",
        level=level,
        stream=sys.stdout,
    )
    for log_name in EXTERNAL_LOGGERS:
        logging.getLogger(log_name).setLevel(logging.WARNING)


def _get_evg_build_variant_by_name(evergreen_conf: EvergreenProjectConfig, name: str) -> Variant:
    """
    Get the evergreen build variant by name from the evergreen config file.

    :param evergreen_conf: The evergreen config file.
    :param name: The build variant name to find.
    :return: The evergreen build variant.
    """
    evg_build_variant = evergreen_conf.get_variant(name)
    if not evg_build_variant:
        LOGGER.warning("Build variant not found in evergreen config")
        raise ValueError(f"Build variant ({name} not found in evergreen configuration")

    return evg_build_variant


class FileChangeDetector(ABC):
    """Interface to detect changes to files."""

    @abstractmethod
    def create_revision_map(self, repos: List[Repo]) -> RevisionMap:
        """
        Create a map of the repos and the given revisions to diff against.

        :param repos: List of repos being tracked.
        :return: Map of repositories and revisions to diff against.
        """
        raise NotImplementedError()

    def find_changed_tests(self, repos: List[Repo]) -> Set[str]:
        """
        Find the changed tests.

        Use git to find which test files have changed in this patch.
        The returned file paths are in normalized form (see os.path.normpath(path)).

        :param repos: List of repos containing changed files.
        :return: Set of changed tests.
        """
        revision_map = self.create_revision_map(repos)
        LOGGER.info("Calculated revision map", revision_map=revision_map)

        changed_files = find_changed_files_in_repos(repos, revision_map)
        return {os.path.normpath(path) for path in changed_files if is_file_a_test_file(path)}


class LocalFileChangeDetector(FileChangeDetector):
    """A change detector for detecting changes in a local repository."""

    def __init__(self, origin_rev: Optional[str]) -> None:
        """
        Create a local file change detector.

        :param origin_rev: Git revision to diff against.
        """
        self.origin_rev = origin_rev

    def create_revision_map(self, repos: List[Repo]) -> RevisionMap:
        """
        Create a map of the repos and the given revisions to diff against.

        :param repos: List of repos being tracked.
        :return: Map of repositories and revisions to diff against.
        """
        if self.origin_rev:
            return generate_revision_map(repos, {"mongo": self.origin_rev})

        return {}


class BurnInExecutor(ABC):
    """An interface to execute discovered tests."""

    @abstractmethod
    def execute(self, tests_by_task: Dict[str, TaskInfo]) -> None:
        """
        Execute the given tests in the given tasks.

        :param tests_by_task: Dictionary of tasks to run with tests to run in each.
        """
        raise NotImplementedError()


class NopBurnInExecutor(BurnInExecutor):
    """A burn-in executor that displays results, but doesn't execute."""

    def execute(self, tests_by_task: Dict[str, TaskInfo]) -> None:
        """
        Execute the given tests in the given tasks.

        :param tests_by_task: Dictionary of tasks to run with tests to run in each.
        """
        LOGGER.info("Not running tests due to 'no_exec' option.")
        for task_name, task_info in tests_by_task.items():
            print(task_name)
            for test_name in task_info.tests:
                print(f"- {test_name}")


class LocalBurnInExecutor(BurnInExecutor):
    """A burn-in executor that runs tests on the local machine."""

    def __init__(self, resmoke_args: str, repeat_config: RepeatConfig) -> None:
        """
        Create a new local burn-in executor.

        :param resmoke_args: Resmoke arguments to use for execution.
        :param repeat_config: How tests should be repeated.
        """
        self.resmoke_args = resmoke_args
        self.repeat_config = repeat_config

    def execute(self, tests_by_task: Dict[str, TaskInfo]) -> None:
        """
        Execute the given tests in the given tasks.

        :param tests_by_task: Dictionary of tasks to run with tests to run in each.
        """
        # Populate the config values in order to use the helpers from resmokelib.suitesconfig.
        resmoke_cmd = _set_resmoke_cmd(self.repeat_config, list(self.resmoke_args))
        run_tests(tests_by_task, resmoke_cmd)


class BurnInOrchestrator:
    """Orchestrate the execution of burn_in_tests."""

    def __init__(self, change_detector: FileChangeDetector, burn_in_executor: BurnInExecutor,
                 evg_conf: EvergreenProjectConfig) -> None:
        """
        Create a new orchestrator.

        :param change_detector: Component to use to detect test changes.
        :param burn_in_executor: Components to execute tests.
        :param evg_conf: Evergreen project configuration.
        """
        self.change_detector = change_detector
        self.burn_in_executor = burn_in_executor
        self.evg_conf = evg_conf

    def burn_in(self, repos: List[Repo], build_variant: str) -> None:
        """
        Execute burn in tests for the given git repositories.

        :param repos: Repositories to check for changes.
        :param build_variant: Build variant to use for task definitions.
        """
        changed_tests = self.change_detector.find_changed_tests(repos)
        LOGGER.info("Found changed tests", files=changed_tests)

        tests_by_task = create_tests_by_task(build_variant, self.evg_conf, changed_tests)
        LOGGER.debug("tests and tasks found", tests_by_task=tests_by_task)

        self.burn_in_executor.execute(tests_by_task)


@click.command()
@click.option("--no-exec", "no_exec", default=False, is_flag=True,
              help="Do not execute the found tests.")
@click.option("--build-variant", "build_variant", default=DEFAULT_VARIANT, metavar='BUILD_VARIANT',
              help="Tasks to run will be selected from this build variant.")
@click.option("--repeat-tests", "repeat_tests_num", default=None, type=int,
              help="Number of times to repeat tests.")
@click.option("--repeat-tests-min", "repeat_tests_min", default=None, type=int,
              help="The minimum number of times to repeat tests if time option is specified.")
@click.option("--repeat-tests-max", "repeat_tests_max", default=None, type=int,
              help="The maximum number of times to repeat tests if time option is specified.")
@click.option("--repeat-tests-secs", "repeat_tests_secs", default=None, type=int, metavar="SECONDS",
              help="Repeat tests for the given time (in secs).")
@click.option("--verbose", "verbose", default=False, is_flag=True, help="Enable extra logging.")
@click.option(
    "--origin-rev", "origin_rev", default=None,
    help="The revision in the mongo repo that changes will be compared against if specified.")
@click.argument("resmoke_args", nargs=-1, type=click.UNPROCESSED)
# pylint: disable=too-many-arguments,too-many-locals
def main(build_variant: str, no_exec: bool, repeat_tests_num: Optional[int],
         repeat_tests_min: Optional[int], repeat_tests_max: Optional[int],
         repeat_tests_secs: Optional[int], resmoke_args: str, verbose: bool,
         origin_rev: Optional[str]) -> None:
    """
    Run new or changed tests in repeated mode to validate their stability.

    burn_in_tests detects jstests that are new or changed since the last git command and then
    runs those tests in a loop to validate their reliability.

    The `--origin-rev` argument allows users to specify which revision should be used as the last
    git command to compare against to find changed files. If the `--origin-rev` argument is provided,
    we find changed files by comparing your latest changes to this revision. If not provided, we
    find changed test files by comparing your latest changes to HEAD. The revision provided must
    be a revision that exists in the mongodb repository.

    The `--repeat-*` arguments allow configuration of how burn_in_tests repeats tests. Tests can
    either be repeated a specified number of times with the `--repeat-tests` option, or they can
    be repeated for a certain time period with the `--repeat-tests-secs` option.
    \f

    :param build_variant: Build variant to query tasks from.
    :param no_exec: Just perform test discover, do not execute the tests.
    :param repeat_tests_num: Repeat each test this number of times.
    :param repeat_tests_min: Repeat each test at least this number of times.
    :param repeat_tests_max: Once this number of repetitions has been reached, stop repeating.
    :param repeat_tests_secs: Continue repeating tests for this number of seconds.
    :param resmoke_args: Arguments to pass through to resmoke.
    :param verbose: Log extra debug information.
    :param origin_rev: The revision that local changes will be compared against.
    """
    _configure_logging(verbose)

    repeat_config = RepeatConfig(repeat_tests_secs=repeat_tests_secs,
                                 repeat_tests_min=repeat_tests_min,
                                 repeat_tests_max=repeat_tests_max,
                                 repeat_tests_num=repeat_tests_num)  # yapf: disable

    repos = [Repo(x) for x in DEFAULT_REPO_LOCATIONS if os.path.isdir(x)]
    evg_conf = parse_evergreen_file(EVERGREEN_FILE)

    change_detector = LocalFileChangeDetector(origin_rev)
    executor = LocalBurnInExecutor(resmoke_args, repeat_config)
    if no_exec:
        executor = NopBurnInExecutor()

    burn_in_orchestrator = BurnInOrchestrator(change_detector, executor, evg_conf)
    burn_in_orchestrator.burn_in(repos, build_variant)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

#!/usr/bin/env python3
"""Command line utility for running newly added or modified jstests under the appropriate multiversion passthrough suites."""

import os
from datetime import datetime
from functools import partial
from typing import List, Dict, NamedTuple

import click
import inject
from git import Repo
import structlog
from structlog.stdlib import LoggerFactory
from evergreen.api import EvergreenApi, RetryingEvergreenApi

from buildscripts.burn_in_tests import EVERGREEN_FILE, \
    DEFAULT_REPO_LOCATIONS, create_tests_by_task, TaskInfo
from buildscripts.ciconfig.evergreen import parse_evergreen_file, EvergreenProjectConfig
from buildscripts.evergreen_burn_in_tests import GenerateConfig, DEFAULT_PROJECT, EvergreenFileChangeDetector
from buildscripts.task_generation.constants import CONFIG_FILE
from buildscripts.resmokelib.suitesconfig import get_named_suites_with_root_level_key
from buildscripts.task_generation.evg_config_builder import EvgConfigBuilder
from buildscripts.task_generation.gen_config import GenerationConfiguration
from buildscripts.task_generation.generated_config import GeneratedConfiguration
from buildscripts.task_generation.multiversion_util import MultiversionUtilService
from buildscripts.task_generation.resmoke_proxy import ResmokeProxyConfig
from buildscripts.task_generation.suite_split import SuiteSplitConfig, SuiteSplitParameters
from buildscripts.task_generation.suite_split_strategies import SplitStrategy, greedy_division, \
    FallbackStrategy, round_robin_fallback
from buildscripts.task_generation.task_types.gentask_options import GenTaskOptions
from buildscripts.task_generation.task_types.multiversion_tasks import MultiversionGenTaskParams
from buildscripts.util.cmdutils import enable_logging

structlog.configure(logger_factory=LoggerFactory())
LOGGER = structlog.getLogger(__name__)

MULTIVERSION_CONFIG_KEY = "use_in_multiversion"
MULTIVERSION_PASSTHROUGH_TAG = "multiversion_passthrough"
BURN_IN_MULTIVERSION_TASK = "burn_in_tests_multiversion"
DEFAULT_CONFIG_DIR = "generated_resmoke_config"
DEFAULT_TEST_SUITE_DIR = os.path.join("buildscripts", "resmokeconfig", "suites")


def filter_list(item: str, input_list: List[str]) -> bool:
    """
    Filter to determine if the given item is in the given list.

    :param item: Item to search for.
    :param input_list: List to search.
    :return: True if the item is contained in the list.
    """
    return item in input_list


class BurnInConfig(NamedTuple):
    """Configuration for generating build in."""

    build_id: str
    build_variant: str
    revision: str

    def build_config_location(self) -> str:
        """Build the configuration location for the generated configuration."""
        return f"{self.build_variant}/{self.revision}/generate_tasks/burn_in_tests_multiversion_gen-{self.build_id}.tgz"


class MultiversionBurnInOrchestrator:
    """Orchestrator for generating multiversion burn_in_tests."""

    @inject.autoparams()
    def __init__(self, change_detector: EvergreenFileChangeDetector,
                 evg_conf: EvergreenProjectConfig, multiversion_util: MultiversionUtilService,
                 burn_in_config: BurnInConfig) -> None:
        """
        Initialize the orchestrator.

        :param change_detector: Service to find changed files.
        :param evg_conf: Evergreen project configuration.
        :param multiversion_util: Multiversion utilities.
        :param burn_in_config: Configuration for generating burn in.
        """
        self.change_detector = change_detector
        self.evg_config = evg_conf
        self.multiversion_util = multiversion_util
        self.burn_in_config = burn_in_config

    def generate_tests(self, repos: List[Repo], generate_config: GenerateConfig,
                       target_file: str) -> None:
        """
        Generate evergreen configuration to run any changed tests and save them to disk.

        :param repos: List of repos to check for changed tests.
        :param generate_config: Configuration for how to generate tasks.
        :param target_file: File to write configuration to.
        """
        tests_by_task = self.find_changes(repos, generate_config)
        generated_config = self.generate_configuration(tests_by_task, target_file,
                                                       generate_config.build_variant)
        generated_config.write_all_to_dir(DEFAULT_CONFIG_DIR)

    def find_changes(self, repos: List[Repo],
                     generate_config: GenerateConfig) -> Dict[str, TaskInfo]:
        """
        Find tests and tasks to run based on test changes.

        :param repos: List of repos to check for changed tests.
        :param generate_config: Configuration for how to generate tasks.
        :return: Dictionary of tasks with tests to run in them.
        """
        changed_tests = self.change_detector.find_changed_tests(repos)
        tests_by_task = create_tests_by_task(generate_config.build_variant, self.evg_config,
                                             changed_tests)
        LOGGER.debug("tests and tasks found", tests_by_task=tests_by_task)
        return tests_by_task

    # pylint: disable=too-many-locals
    def generate_configuration(self, tests_by_task: Dict[str, TaskInfo], target_file: str,
                               build_variant: str) -> GeneratedConfiguration:
        """
        Generate configuration for the given tasks and tests.

        :param tests_by_task: Map of what to generate.
        :param target_file: Location to write generated configuration.
        :param build_variant: Name of build variant being generated on.
        :return: Generated configuration to create requested tasks and tests.
        """
        builder = EvgConfigBuilder()  # pylint: disable=no-value-for-parameter
        build_variant_config = self.evg_config.get_variant(build_variant)
        is_asan = build_variant_config.is_asan_build()
        tasks = set()
        if tests_by_task:
            # Get the multiversion suites that will run in as part of burn_in_multiversion.
            multiversion_suites = get_named_suites_with_root_level_key(MULTIVERSION_CONFIG_KEY)
            for suite in multiversion_suites:
                task_name = suite["origin"]
                if task_name not in tests_by_task.keys():
                    # Only generate burn in multiversion tasks for suites that would run the
                    # detected changed tests.
                    continue

                LOGGER.debug("Generating multiversion suite", suite=suite["multiversion_name"])
                test_list = tests_by_task[task_name].tests
                split_params = SuiteSplitParameters(
                    task_name=suite["multiversion_name"], suite_name=task_name, filename=task_name,
                    test_file_filter=partial(filter_list, input_list=test_list),
                    build_variant=build_variant, is_asan=is_asan)
                version_configs = self.multiversion_util.get_version_configs_for_suite(task_name)
                gen_params = MultiversionGenTaskParams(
                    mixed_version_configs=version_configs,
                    is_sharded=self.multiversion_util.is_suite_sharded(task_name),
                    resmoke_args="",
                    parent_task_name="burn_in_tests_multiversion",
                    origin_suite=task_name,
                    use_large_distro=False,
                    large_distro_name=None,
                    name_prefix="burn_in_multiversion",
                    create_misc_suite=False,
                    add_to_display_task=False,
                    config_location=self.burn_in_config.build_config_location(),
                )

                tasks = tasks.union(builder.add_multiversion_burn_in_test(split_params, gen_params))

        if len(tasks) == 0:
            builder.get_build_variant(build_variant)

        executions_tasks = {task.name for task in tasks}
        executions_tasks.add("burn_in_tests_multiversion_gen")
        builder.add_display_task(display_task_name="burn_in_multiversion",
                                 execution_task_names=executions_tasks, build_variant=build_variant)

        return builder.build(target_file)


@click.command()
@click.option("--generate-tasks-file", "generate_tasks_file", required=True, metavar='FILE',
              help="Run in 'generate.tasks' mode. Store task config to given file.")
@click.option("--build-variant", "build_variant", default=None, metavar='BUILD_VARIANT',
              help="Tasks to run will be selected from this build variant.")
@click.option("--run-build-variant", "run_build_variant", default=None, metavar='BUILD_VARIANT',
              help="Burn in tasks will be generated on this build variant.")
@click.option("--distro", "distro", default=None, metavar='DISTRO',
              help="The distro the tasks will execute on.")
@click.option("--project", "project", default=DEFAULT_PROJECT, metavar='PROJECT',
              help="The evergreen project the tasks will execute on.")
@click.option("--evg-api-config", "evg_api_config", default=CONFIG_FILE, metavar="FILE",
              help="Configuration file with connection info for Evergreen API.")
@click.option("--revision", required=True, help="Revision generation is being run against.")
@click.option("--build-id", required=True, help="ID of build being run against.")
@click.option("--verbose", "verbose", default=False, is_flag=True, help="Enable extra logging.")
@click.option("--task_id", "task_id", default=None, metavar='TASK_ID',
              help="The evergreen task id.")
# pylint: disable=too-many-arguments,too-many-locals
def main(build_variant, run_build_variant, distro, project, generate_tasks_file, evg_api_config,
         verbose, task_id, revision, build_id):
    """
    Run new or changed tests in repeated mode to validate their stability.

    Running burn_in_tests_multiversion will run new or changed tests against the appropriate generated multiversion
    suites. The purpose of these tests are to signal bugs in the generated multiversion suites as these tasks are
    excluded from the required build variants and are only run in certain daily build variants. As such, we only expect
    the burn-in multiversion tests to be run once for each binary version configuration, and `--repeat-*` arguments
    should be None when executing this script.

    There are two modes that burn_in_tests_multiversion can run in:

    (1) Normal mode: by default burn_in_tests will attempt to run all detected tests the
    configured number of times. This is useful if you have a test or tests you would like to
    check before submitting a patch to evergreen.

    (2) By specifying the `--generate-tasks-file`, burn_in_tests will run generate a configuration
    file that can then be sent to the Evergreen 'generate.tasks' command to create evergreen tasks
    to do all the test executions. This is the mode used to run tests in patch builds.

    NOTE: There is currently a limit of the number of tasks burn_in_tests will attempt to generate
    in evergreen. The limit is 1000. If you change enough tests that more than 1000 tasks would
    be generated, burn_in_test will fail. This is to avoid generating more tasks than evergreen
    can handle.
    \f

    :param build_variant: Build variant to query tasks from.
    :param run_build_variant:Build variant to actually run against.
    :param distro: Distro to run tests on.
    :param project: Project to run tests on.
    :param generate_tasks_file: Create a generate tasks configuration in this file.
    :param evg_api_config: Location of configuration file to connect to evergreen.
    :param verbose: Log extra debug information.
    """
    enable_logging(verbose)

    burn_in_config = BurnInConfig(build_variant=build_variant, build_id=build_id, revision=revision)
    evg_conf = parse_evergreen_file(EVERGREEN_FILE)
    generate_config = GenerateConfig(build_variant=build_variant,
                                     run_build_variant=run_build_variant,
                                     distro=distro,
                                     project=project,
                                     task_id=task_id)  # yapf: disable
    generate_config.validate(evg_conf)

    gen_task_options = GenTaskOptions(
        create_misc_suite=False,
        is_patch=True,
        generated_config_dir=DEFAULT_CONFIG_DIR,
        use_default_timeouts=False,
    )
    split_task_options = SuiteSplitConfig(
        evg_project=project,
        target_resmoke_time=60,
        max_sub_suites=100,
        max_tests_per_suite=1,
        start_date=datetime.utcnow(),
        end_date=datetime.utcnow(),
        default_to_fallback=True,
    )

    repos = [Repo(x) for x in DEFAULT_REPO_LOCATIONS if os.path.isdir(x)]

    def dependencies(binder: inject.Binder) -> None:
        evg_api = RetryingEvergreenApi.get_api(config_file=evg_api_config)
        binder.bind(SuiteSplitConfig, split_task_options)
        binder.bind(SplitStrategy, greedy_division)
        binder.bind(FallbackStrategy, round_robin_fallback)
        binder.bind(EvergreenProjectConfig, evg_conf)
        binder.bind(GenTaskOptions, gen_task_options)
        binder.bind(EvergreenApi, evg_api)
        binder.bind(GenerationConfiguration, GenerationConfiguration.from_yaml_file())
        binder.bind(ResmokeProxyConfig,
                    ResmokeProxyConfig(resmoke_suite_dir=DEFAULT_TEST_SUITE_DIR))
        binder.bind(EvergreenFileChangeDetector, EvergreenFileChangeDetector(task_id, evg_api))
        binder.bind(BurnInConfig, burn_in_config)

    inject.configure(dependencies)

    burn_in_orchestrator = MultiversionBurnInOrchestrator()  # pylint: disable=no-value-for-parameter
    burn_in_orchestrator.generate_tests(repos, generate_config, generate_tasks_file)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

#!/usr/bin/env python3
#
# Copyright (C) 2021-present MongoDB, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the Server Side Public License, version 1,
# as published by MongoDB, Inc.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# Server Side Public License for more details.
#
# You should have received a copy of the Server Side Public License
# along with this program. If not, see
# <http://www.mongodb.com/licensing/server-side-public-license>.
#
# As a special exception, the copyright holders give permission to link the
# code of portions of this program with the OpenSSL library under certain
# conditions as described in each individual source file and distribute
# linked combinations including the program with the OpenSSL library. You
# must comply with the Server Side Public License in all respects for
# all of the code used other than as permitted herein. If you modify file(s)
# with this exception, you may extend this exception to your version of the
# file(s), but you are not obligated to do so. If you do not wish to do so,
# delete this exception statement from your version. If you delete this
# exception statement from all source files in the program, then also delete
# it in the license file.
"""Feed command line arguments to a Cheetah template to generate a source file."""

import argparse
import sys

from Cheetah.Template import Template


def main():
    """Generate a source file by passing command line arguments to a Cheetah template.

    The Cheetah template will be expanded with an `$args` in its namespace, containing
    the trailing command line arguments of this program.
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-o', nargs='?', type=argparse.FileType('w'), default=sys.stdout,
                        help='output file (default sys.stdout)')
    parser.add_argument('template_file', help='Cheetah template file')
    parser.add_argument('template_arg', nargs='*', default=[], help='Cheetah template args')
    opts = parser.parse_args()

    opts.o.write(str(Template(file=opts.template_file, namespaces=[{'args': opts.template_arg}])))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Clang format script that provides the following.

1. Ability to grab binaries where possible from LLVM.
2. Ability to download binaries from MongoDB cache for clang-format.
3. Validates clang-format is the right version.
4. Has support for checking which files are to be checked.
5. Supports validating and updating a set of files to the right coding style.
"""

import difflib
import glob
import logging
import os
import re
import shutil
import string
import subprocess
import sys
import tarfile
import tempfile
import threading
import urllib.error
import urllib.parse
import urllib.request

from optparse import OptionParser
import structlog

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.realpath(__file__)))))

# pylint: disable=wrong-import-position
from buildscripts.linter.filediff import gather_changed_files_for_lint
from buildscripts.linter import git, parallel
# pylint: enable=wrong-import-position

##############################################################################
#
# Constants for clang-format
#
#

# Expected version of clang-format
CLANG_FORMAT_VERSION = "7.0.1"
CLANG_FORMAT_SHORT_VERSION = "7.0"
CLANG_FORMAT_SHORTER_VERSION = "70"

# Name of clang-format as a binary
CLANG_FORMAT_PROGNAME = "clang-format"

# URL location of the "cached" copy of clang-format to download
# for users which do not have clang-format installed
CLANG_FORMAT_HTTP_LINUX_CACHE = "https://s3.amazonaws.com/boxes.10gen.com/build/clang-format-7.0.1-rhel70.tar.gz"

CLANG_FORMAT_HTTP_DARWIN_CACHE = "https://s3.amazonaws.com/boxes.10gen.com/build/clang-format-7.0.1-x86_64-apple-darwin.tar.gz"

CLANG_FORMAT_TOOLCHAIN_PATH = "/opt/mongodbtoolchain/v3/bin/clang-format"

# Path in the tarball to the clang-format binary
CLANG_FORMAT_SOURCE_TAR_BASE = string.Template("clang+llvm-$version-$tar_path/bin/" +
                                               CLANG_FORMAT_PROGNAME)


##############################################################################
def callo(args, **kwargs):
    """Call a program, and capture its output."""
    return subprocess.check_output(args, **kwargs).decode('utf-8')


def get_tar_path(version, tar_path):
    """Return the path to clang-format in the llvm tarball."""
    # pylint: disable=too-many-function-args
    return CLANG_FORMAT_SOURCE_TAR_BASE.substitute(version=version, tar_path=tar_path)


def extract_clang_format(tar_path):
    """Extract the clang_format tar file."""
    # Extract just the clang-format binary
    # On OSX, we shell out to tar because tarfile doesn't support xz compression
    if sys.platform == 'darwin':
        subprocess.call(['tar', '-xzf', tar_path, '*clang-format*'])
    # Otherwise we use tarfile because some versions of tar don't support wildcards without
    # a special flag
    else:
        tarfp = tarfile.open(tar_path)
        for name in tarfp.getnames():
            if name.endswith('clang-format'):
                tarfp.extract(name)
        tarfp.close()


def get_clang_format_from_cache_and_extract(url, tarball_ext):
    """Get clang-format from mongodb's cache and extract the tarball."""
    dest_dir = tempfile.gettempdir()
    temp_tar_file = os.path.join(dest_dir, "temp.tar" + tarball_ext)

    # Download from file
    print("Downloading clang-format %s from %s, saving to %s" % (CLANG_FORMAT_VERSION, url,
                                                                 temp_tar_file))

    # Retry download up to 5 times.
    num_tries = 5
    for attempt in range(num_tries):
        try:
            resp = urllib.request.urlopen(url)
            with open(temp_tar_file, 'wb') as fh:
                fh.write(resp.read())
            break
        except urllib.error.URLError:
            if attempt == num_tries - 1:
                raise
            continue

    extract_clang_format(temp_tar_file)


def get_clang_format_from_darwin_cache(dest_file):
    """Download clang-format from llvm.org, unpack the tarball to dest_file."""
    get_clang_format_from_cache_and_extract(CLANG_FORMAT_HTTP_DARWIN_CACHE, ".xz")

    # Destination Path
    shutil.move(get_tar_path(CLANG_FORMAT_VERSION, "x86_64-apple-darwin"), dest_file)


def get_clang_format_from_linux_cache(dest_file):
    """Get clang-format from mongodb's cache."""
    get_clang_format_from_cache_and_extract(CLANG_FORMAT_HTTP_LINUX_CACHE, ".gz")

    # Destination Path
    shutil.move("build/bin/clang-format", dest_file)


class ClangFormat(object):
    """ClangFormat class."""

    def __init__(self, path, cache_dir):
        # pylint: disable=too-many-branches,too-many-statements
        """Initialize ClangFormat."""
        self.path = None

        # Check the clang-format the user specified
        if path is not None:
            self.path = path
            if not self._validate_version():
                print("WARNING: Could not find clang-format in the user specified path %s" %
                      (self.path))
                self.path = None

        # Check the environment variable
        if self.path is None:
            if "MONGO_CLANG_FORMAT" in os.environ:
                self.path = os.environ["MONGO_CLANG_FORMAT"]
                if not self._validate_version():
                    self.path = None

        # Check for the binary in the expected toolchain directory on non-windows systems
        if self.path is None:
            if sys.platform != "win32":
                if os.path.exists(CLANG_FORMAT_TOOLCHAIN_PATH):
                    self.path = CLANG_FORMAT_TOOLCHAIN_PATH
                    if not self._validate_version():
                        self.path = None

        # Check the users' PATH environment variable now
        if self.path is None:
            # Check for various versions staring with binaries with version specific suffixes in the
            # user's path
            programs = list(
                map(lambda program: program + ".exe" if sys.platform == "win32" else program, [
                    CLANG_FORMAT_PROGNAME + "-" + CLANG_FORMAT_VERSION,
                    CLANG_FORMAT_PROGNAME + "-" + CLANG_FORMAT_SHORT_VERSION,
                    CLANG_FORMAT_PROGNAME + CLANG_FORMAT_SHORTER_VERSION,
                    CLANG_FORMAT_PROGNAME,
                ]))

            directories_to_check = os.environ["PATH"].split(os.pathsep)

            # If Windows, try to grab it from Program Files
            # Check both native Program Files and WOW64 version
            if sys.platform == "win32":
                programfiles = [
                    os.environ["ProgramFiles"],
                    os.environ["ProgramFiles(x86)"],
                ]

                directories_to_check += [os.path.join(p, "LLVM\\bin\\") for p in programfiles]

            for ospath in directories_to_check:
                for program in programs:
                    self.path = os.path.join(ospath, program)
                    if os.path.exists(self.path) and self._validate_version():
                        break
                    self.path = None
                    continue
                else:
                    continue
                break

        # Have not found it yet, download it from the web
        if self.path is None:
            if not os.path.isdir(cache_dir):
                os.makedirs(cache_dir)

            clang_format_progname_ext = ".exe" if sys.platform == "win32" else ""
            self.path = os.path.join(
                cache_dir,
                CLANG_FORMAT_PROGNAME + "-" + CLANG_FORMAT_VERSION + clang_format_progname_ext)

            # Download a new version if the cache is empty or stale
            if not os.path.isfile(self.path) or not self._validate_version():
                if sys.platform.startswith("linux"):
                    get_clang_format_from_linux_cache(self.path)
                elif sys.platform == "darwin":
                    get_clang_format_from_darwin_cache(self.path)
                else:
                    print("ERROR: clang_format.py does not support downloading clang-format " +
                          "on this platform, please install clang-format " + CLANG_FORMAT_VERSION)

        # Validate we have the correct version
        # We only can fail here if the user specified a clang-format binary and it is the wrong
        # version
        if not self._validate_version():
            print("ERROR: exiting because of previous warning.")
            sys.exit(1)

        self.print_lock = threading.Lock()

    def _validate_version(self):
        """Validate clang-format is the expected version."""
        cf_version = callo([self.path, "--version"])

        if CLANG_FORMAT_VERSION in cf_version:
            return True

        print("WARNING: clang-format with incorrect version found at " + self.path + " version: " +
              cf_version)

        return False

    def _lint(self, file_name, print_diff):
        """Check the specified file has the correct format."""
        with open(file_name, 'rb') as original_text:
            original_file = original_text.read().decode('utf-8')

            original_text.seek(0)

            # Get formatted file as clang-format would format the file
            formatted_file = callo([
                self.path, "--assume-filename=" +
                (file_name if not file_name.endswith(".h") else file_name + "pp"), "--style=file"
            ], stdin=original_text)

        if original_file != formatted_file:
            if print_diff:
                original_lines = original_file.splitlines()
                formatted_lines = formatted_file.splitlines()
                result = difflib.unified_diff(original_lines, formatted_lines)

                # Take a lock to ensure diffs do not get mixed when printed to the screen
                with self.print_lock:
                    print("ERROR: Found diff for " + file_name)
                    print("To fix formatting errors, run `buildscripts/clang_format.py format`")
                    for line in result:
                        print(line.rstrip())

            return False

        return True

    def lint(self, file_name):
        """Check the specified file has the correct format."""
        return self._lint(file_name, print_diff=True)

    def format(self, file_name):
        """Update the format of the specified file."""
        if self._lint(file_name, print_diff=False):
            return True

        # Update the file with clang-format
        # We have to tell `clang-format` to format on standard input due to its file type
        # determiner.  `--assume-filename` doesn't work directly on files, but only on standard
        # input.  Thus we have to open the file as the subprocess's standard input. Then we record
        # that formatted standard output back into the file.  We can't use the `-i` option, due to
        # the fact that `clang-format` believes that many of our C++ headers are Objective-C code.
        formatted = True
        with open(file_name, 'rb') as source_stream:
            try:
                reformatted_text = subprocess.check_output([
                    self.path, "--assume-filename=" +
                    (file_name if not file_name.endswith(".h") else file_name + "pp"),
                    "--style=file"
                ], stdin=source_stream)
            except subprocess.CalledProcessError:
                formatted = False

        if formatted:
            with open(file_name, "wb") as output_stream:
                output_stream.write(reformatted_text)

        # Version 3.8 generates files like foo.cpp~RF83372177.TMP when it formats foo.cpp
        # on Windows, we must clean these up
        if sys.platform == "win32":
            glob_pattern = file_name + "*.TMP"
            for fglob in glob.glob(glob_pattern):
                os.unlink(fglob)

        return formatted


FILES_RE = re.compile('\\.(h|hpp|ipp|cpp|js)$')


def is_interesting_file(file_name):
    """Return true if this file should be checked."""
    return (file_name.startswith("jstests")
            or file_name.startswith("src") and not file_name.startswith("src/third_party/")
            and not file_name.startswith("src/mongo/gotools/")) and FILES_RE.search(file_name)


def get_list_from_lines(lines):
    """Convert a string containing a series of lines into a list of strings."""
    return [line.rstrip() for line in lines.splitlines()]


def _get_build_dir():
    """Return the location of the scons' build directory."""
    return os.path.join(git.get_base_dir(), "build")


def _lint_files(clang_format, files):
    """Lint a list of files with clang-format."""
    clang_format = ClangFormat(clang_format, _get_build_dir())

    lint_clean = parallel.parallel_process([os.path.abspath(f) for f in files], clang_format.lint)

    if not lint_clean:
        print("ERROR: Source code does not match required source formatting style")
        sys.exit(1)


def lint_patch(clang_format, infile):
    """Lint patch command entry point."""
    files = git.get_files_to_check_from_patch(infile, is_interesting_file)

    # Patch may have files that we do not want to check which is fine
    if files:
        _lint_files(clang_format, files)


def lint_git_diff(clang_format):
    """
    Lint the files that have changes since the last git commit.

    :param clang_format: Path to clang_format command.
    """
    files = gather_changed_files_for_lint(is_interesting_file)

    if files:
        _lint_files(clang_format, files)


def lint(clang_format):
    """Lint files command entry point."""
    files = git.get_files_to_check([], is_interesting_file)

    _lint_files(clang_format, files)

    return True


def lint_all(clang_format):
    """Lint files command entry point based on working tree."""
    files = git.get_files_to_check_working_tree(is_interesting_file)

    _lint_files(clang_format, files)

    return True


def _format_files(clang_format, files):
    """Format a list of files with clang-format."""
    clang_format = ClangFormat(clang_format, _get_build_dir())

    format_clean = parallel.parallel_process([os.path.abspath(f) for f in files],
                                             clang_format.format)

    if not format_clean:
        print("ERROR: failed to format files")
        sys.exit(1)


def format_func(clang_format):
    """Format files command entry point."""
    files = git.get_files_to_check([], is_interesting_file)

    _format_files(clang_format, files)


def format_my_func(clang_format, origin_branch):
    """My Format files command entry point."""
    files = git.get_my_files_to_check(is_interesting_file, origin_branch)
    files = [f for f in files if os.path.exists(f)]

    _format_files(clang_format, files)


def reformat_branch(  # pylint: disable=too-many-branches,too-many-locals,too-many-statements
        clang_format, commit_prior_to_reformat, commit_after_reformat):
    """Reformat a branch made before a clang-format run."""
    clang_format = ClangFormat(clang_format, _get_build_dir())

    if os.getcwd() != git.get_base_dir():
        raise ValueError("reformat-branch must be run from the repo root")

    if not os.path.exists("buildscripts/clang_format.py"):
        raise ValueError("reformat-branch is only supported in the mongo repo")

    repo = git.Repo(git.get_base_dir())

    # Validate that user passes valid commits
    if not repo.is_commit(commit_prior_to_reformat):
        raise ValueError("Commit Prior to Reformat '%s' is not a valid commit in this repo" %
                         commit_prior_to_reformat)

    if not repo.is_commit(commit_after_reformat):
        raise ValueError(
            "Commit After Reformat '%s' is not a valid commit in this repo" % commit_after_reformat)

    if not repo.is_ancestor(commit_prior_to_reformat, commit_after_reformat):
        raise ValueError(
            ("Commit Prior to Reformat '%s' is not a valid ancestor of Commit After" +
             " Reformat '%s' in this repo") % (commit_prior_to_reformat, commit_after_reformat))

    # Validate the user is on a local branch that has the right merge base
    if repo.is_detached():
        raise ValueError("You must not run this script in a detached HEAD state")

    # Validate the user has no pending changes
    if repo.is_working_tree_dirty():
        raise ValueError(
            "Your working tree has pending changes. You must have a clean working tree before proceeding."
        )

    merge_base = repo.get_merge_base(["HEAD", commit_prior_to_reformat])

    if not merge_base == commit_prior_to_reformat:
        raise ValueError(
            "Please rebase to '%s' and resolve all conflicts before running this script" %
            (commit_prior_to_reformat))

    # We assume the target branch is master, it could be a different branch if needed for testing
    merge_base = repo.get_merge_base(["HEAD", "master"])

    if not merge_base == commit_prior_to_reformat:
        raise ValueError(
            "This branch appears to already have advanced too far through the merge process")

    # Everything looks good so lets start going through all the commits
    branch_name = repo.get_branch_name()
    new_branch = "%s-reformatted" % branch_name

    if repo.does_branch_exist(new_branch):
        raise ValueError(
            "The branch '%s' already exists. Please delete the branch '%s', or rename the current branch."
            % (new_branch, new_branch))

    commits = get_list_from_lines(
        repo.git_log([
            "--reverse", "--no-show-signature", "--pretty=format:%H",
            "%s..HEAD" % commit_prior_to_reformat
        ]))

    previous_commit_base = commit_after_reformat

    # Go through all the commits the user made on the local branch and migrate to a new branch
    # that is based on post_reformat commits instead
    for commit_hash in commits:
        repo.git_checkout(["--quiet", commit_hash])

        deleted_files = []

        # Format each of the files by checking out just a single commit from the user's branch
        commit_files = get_list_from_lines(repo.git_diff(["HEAD~", "--name-only"]))

        for commit_file in commit_files:

            # Format each file needed if it was not deleted
            if not os.path.exists(commit_file):
                print("Skipping file '%s' since it has been deleted in commit '%s'" % (commit_file,
                                                                                       commit_hash))
                deleted_files.append(commit_file)
                continue

            if is_interesting_file(commit_file):
                clang_format.format(commit_file)
            else:
                print("Skipping file '%s' since it is not a file clang_format should format" %
                      commit_file)

        # Check if anything needed reformatting, and if so amend the commit
        if not repo.is_working_tree_dirty():
            print("Commit %s needed no reformatting" % commit_hash)
        else:
            repo.git_commit(["--all", "--amend", "--no-edit"])

        # Rebase our new commit on top the post-reformat commit
        previous_commit = repo.git_rev_parse(["HEAD"])

        # Checkout the new branch with the reformatted commits
        # Note: we will not name as a branch until we are done with all commits on the local branch
        repo.git_checkout(["--quiet", previous_commit_base])

        # Copy each file from the reformatted commit on top of the post reformat
        diff_files = get_list_from_lines(
            repo.git_diff(["%s~..%s" % (previous_commit, previous_commit), "--name-only"]))

        for diff_file in diff_files:
            # If the file was deleted in the commit we are reformatting, we need to delete it again
            if diff_file in deleted_files:
                repo.git_rm(["--ignore-unmatch", diff_file])
                continue

            # The file has been added or modified, continue as normal
            file_contents = repo.git_show(["%s:%s" % (previous_commit, diff_file)])

            root_dir = os.path.dirname(diff_file)
            if root_dir and not os.path.exists(root_dir):
                os.makedirs(root_dir)

            with open(diff_file, "w+", encoding="utf-8") as new_file:
                new_file.write(file_contents)

            repo.git_add([diff_file])

        # Create a new commit onto clang-formatted branch
        repo.git_commit(["--reuse-message=%s" % previous_commit, "--no-gpg-sign", "--allow-empty"])

        previous_commit_base = repo.git_rev_parse(["HEAD"])

    # Create a new branch to mark the hashes we have been using
    repo.git_checkout(["-b", new_branch])

    print("reformat-branch is done running.\n")
    print("A copy of your branch has been made named '%s', and formatted with clang-format.\n" %
          new_branch)
    print("The original branch has been left unchanged.")
    print("The next step is to rebase the new branch on 'master'.")


def usage():
    """Print usage."""
    print(
        "clang_format.py supports 6 commands (lint, lint-all, lint-patch, format, format-my, reformat-branch)."
    )
    print("\nformat-my <origin branch>")
    print("   <origin branch>  - upstream branch to compare against")


def main():
    """Execute Main entry point."""
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    structlog.configure(logger_factory=structlog.stdlib.LoggerFactory())

    parser = OptionParser()
    parser.add_option("-c", "--clang-format", type="string", dest="clang_format")

    (options, args) = parser.parse_args(args=sys.argv)

    if len(args) > 1:
        command = args[1]

        if command == "lint":
            lint(options.clang_format)
        elif command == "lint-all":
            lint_all(options.clang_format)
        elif command == "lint-patch":
            lint_patch(options.clang_format, args[2:])
        elif command == "lint-git-diff":
            lint_git_diff(options.clang_format)
        elif command == "format":
            format_func(options.clang_format)
        elif command == "format-my":
            format_my_func(options.clang_format, args[2] if len(args) > 2 else "origin/master")
        elif command == "reformat-branch":

            if len(args) < 3:
                print(
                    "ERROR: reformat-branch takes two parameters: commit_prior_to_reformat commit_after_reformat"
                )
                return

            reformat_branch(options.clang_format, args[2], args[3])
        else:
            usage()
    else:
        usage()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Collect system resource information on processes running in Evergreen on a given interval."""

from datetime import datetime
import optparse
import os
import sys
import time

from bson.json_util import dumps
import requests

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buildscripts.resmokelib import utils  # pylint: disable=wrong-import-position


def main():
    """Main."""
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(description=__doc__, usage=usage)
    parser.add_option(
        "-i", "--interval", dest="interval", default=5, type="int",
        help="Collect system resource information every <interval> seconds. "
        "Default is every 5 seconds.")
    parser.add_option(
        "-o", "--output-file", dest="outfile", default="-",
        help="If '-', then the file is written to stdout."
        " Any other value is treated as the output file name. By default,"
        " output is written to stdout.")

    (options, _) = parser.parse_args()

    with utils.open_or_use_stdout(options.outfile) as fp:
        while True:
            try:
                # Requires the Evergreen agent to be running on port 2285.
                response = requests.get("http://localhost:2285/status")
                if response.status_code != requests.codes.ok:
                    print(
                        "Received a {} HTTP response: {}".format(response.status_code,
                                                                 response.text), file=sys.stderr)
                    time.sleep(options.interval)
                    continue

                timestamp = datetime.now()
                try:
                    res_json = response.json()
                except ValueError:
                    print("Invalid JSON object returned with response: {}".format(response.text),
                          file=sys.stderr)
                    time.sleep(options.interval)
                    continue

                sys_res_dict = {}
                sys_res_dict["timestamp"] = timestamp
                sys_info = res_json["sys_info"]
                sys_res_dict["num_cpus"] = sys_info["num_cpus"]
                sys_res_dict["mem_total"] = sys_info["vmstat"]["total"]
                sys_res_dict["mem_avail"] = sys_info["vmstat"]["available"]
                ps_info = res_json["ps_info"]
                for process in ps_info:
                    try:
                        sys_res_dict["pid"] = process["pid"]
                        sys_res_dict["ppid"] = process["parentPid"]
                        sys_res_dict["num_threads"] = process["numThreads"]
                        sys_res_dict["command"] = process.get("command", "")
                        sys_res_dict["cpu_user"] = process["cpu"]["user"]
                        sys_res_dict["cpu_sys"] = process["cpu"]["system"]
                        sys_res_dict["io_wait"] = process["cpu"]["iowait"]
                        sys_res_dict["io_write"] = process["io"]["writeBytes"]
                        sys_res_dict["io_read"] = process["io"]["readBytes"]
                        sys_res_dict["mem_used"] = process["mem"]["rss"]
                    except KeyError:
                        # KeyError may occur as a result of file missing from /proc, likely due to
                        # process exiting.
                        continue

                    print(dumps(sys_res_dict, sort_keys=True), file=fp)

                    if fp.fileno() != sys.stdout.fileno():
                        # Flush internal buffers associated with file to disk.
                        fp.flush()
                        os.fsync(fp.fileno())
                time.sleep(options.interval)
            except requests.ConnectionError as error:
                print(error, file=sys.stderr)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Combine JSON report files used in Evergreen."""

import errno
import json
import os
import sys
from optparse import OptionParser

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buildscripts.resmokelib.testing import report  # pylint: disable=wrong-import-position
from buildscripts.resmokelib import utils  # pylint: disable=wrong-import-position


def read_json_file(json_file):
    """Read JSON file."""
    with open(json_file) as json_data:
        return json.load(json_data)


def report_exit(combined_test_report):
    """Return report exit code.

    The exit code of this script is based on the following:
        0:  All tests have status "pass", or only non-dynamic tests have status "silentfail".
        31: At least one test has status "fail" or "timeout".
    Note: A test can be considered dynamic if its name contains a ":" character.
    """

    ret = 0
    for test in combined_test_report.test_infos:
        if test.status in ["fail", "timeout"]:
            return 31
    return ret


def check_error(input_count, output_count):
    """Raise error if both input and output exist, or if neither exist."""
    if (not input_count) and (not output_count):
        raise ValueError("None of the input file(s) or output file exists")

    if input_count and output_count:
        raise ValueError("Both input file and output files exist")


def main():
    """Execute Main program."""
    usage = "usage: %prog [options] report1.json report2.json ..."
    parser = OptionParser(description=__doc__, usage=usage)
    parser.add_option(
        "-o", "--output-file", dest="outfile", default="-",
        help=("If '-', then the combined report file is written to stdout."
              " Any other value is treated as the output file name. By default,"
              " output is written to stdout."))
    parser.add_option("-x", "--no-report-exit", dest="report_exit", default=True,
                      action="store_false",
                      help="Do not exit with a non-zero code if any test in the report fails.")

    (options, args) = parser.parse_args()

    if not args:
        sys.exit("No report files were specified")

    report_files = args
    report_files_count = len(report_files)
    test_reports = []

    for report_file in report_files:
        try:
            report_file_json = read_json_file(report_file)
            test_reports.append(report.TestReport.from_dict(report_file_json))
        except IOError as err:
            # errno.ENOENT is the error code for "No such file or directory".
            if err.errno == errno.ENOENT:
                report_files_count -= 1
                continue
            raise

    combined_test_report = report.TestReport.combine(*test_reports)
    combined_report = combined_test_report.as_dict()

    if options.outfile == "-":
        outfile_exists = False  # Nothing will be overridden when writing to stdout.
    else:
        outfile_exists = os.path.exists(options.outfile)

    check_error(report_files_count, outfile_exists)

    if not outfile_exists:
        with utils.open_or_use_stdout(options.outfile) as fh:
            json.dump(combined_report, fh)

    if options.report_exit:
        sys.exit(report_exit(combined_test_report))
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()

#!/bin/bash
#
# consolidate-repos.sh
#
# Create new repo directory under /var/www-org/repo.consolidated
# containing every deb and every rpm under /var/www-org/ with proper
# repo metadata for apt and yum
#

source_dir=/var/www-org/

repodir=/var/www-org/repo.consolidated

gpg_recip='<richard@10gen.com>'

stable_branch="2.6"
unstable_branch="2.7"

echo "Using directory: $repodir"

# set up repo dirs if they don't exist
#
mkdir -p "$repodir/apt/ubuntu"
mkdir -p "$repodir/apt/debian"
mkdir -p "$repodir/yum/redhat"

# to support different $releasever values in yum repo configurations
#
if [ ! -e "$repodir/yum/redhat/6Server" ]
then
  ln -s 6 "$repodir/yum/redhat/6Server"
fi

if [ ! -e "$repodir/yum/redhat/7Server" ]
then
  ln -s 7 "$repodir/yum/redhat/7Server"
fi

if [ ! -e "$repodir/yum/redhat/5Server" ]
then
  ln -s 5 "$repodir/yum/redhat/5Server"
fi

echo "Scanning and copying package files from $source_dir"
echo ". = skipping existing file, @ = copying file"
for package in $(find "$source_dir" -not \( -path "$repodir" -prune \) -and \( -name \*.rpm -o -name \*.deb -o -name Release \))
do
  new_package_location="$repodir$(echo "$package" | sed 's/\/var\/www-org\/[^\/]*//;')"
  # skip if the directory structure looks weird
  #
  if echo "$new_package_location" | grep -q /repo/
  then
    continue
  fi

  # skip if not community package
  #
  if ! echo "$new_package_location" | grep -q org
  then
    continue
  fi
  # skip if it's already there
  #
  if [ -e "$new_package_location" -a "$(basename "$package")" != "Release" ]
  then
      echo -n .
  else
    mkdir -p "$(dirname "$new_package_location")"
    echo -n @
    cp "$package" "$new_package_location"
  fi
done
echo

# packages are in place, now create metadata
#
for debian_dir in "$repodir"/apt/ubuntu "$repodir"/apt/debian
do
  cd "$debian_dir"
  for section_dir in $(find dists -type d -name multiverse -o -name main)
  do
    for arch_dir in "$section_dir"/{binary-i386,binary-amd64}
    do
      echo "Generating Packages file under $debian_dir/$arch_dir"
      if [ ! -d $arch_dir ]
      then
        mkdir $arch_dir
      fi
      dpkg-scanpackages --multiversion "$arch_dir"   > "$arch_dir"/Packages
      gzip -9c  "$arch_dir"/Packages >  "$arch_dir"/Packages.gz
    done
  done

  for release_file in $(find "$debian_dir" -name Release)
  do
    release_dir=$(dirname "$release_file")
    echo "Generating Release file under $release_dir"
    cd $release_dir
    tempfile=$(mktemp /tmp/ReleaseXXXXXX)
    tempfile2=$(mktemp /tmp/ReleaseXXXXXX)
    mv Release $tempfile
    head -7 $tempfile > $tempfile2
    apt-ftparchive release . >> $tempfile2
    cp $tempfile2 Release
    chmod 644 Release
    rm Release.gpg
    echo "Signing Release file"
    gpg -r "$gpg_recip" --no-secmem-warning -abs --output Release.gpg  Release
  done
done

# Create symlinks for stable and unstable branches
#
# Examples:
#
# /var/www-org/repo.consolidated/yum/redhat/5/mongodb-org/unstable -> 2.5
# /var/www-org/repo.consolidated/yum/redhat/6/mongodb-org/unstable -> 2.5
# /var/www-org/repo.consolidated/apt/ubuntu/dists/precise/mongodb-org/unstable -> 2.5
# /var/www-org/repo.consolidated/apt/debian/dists/wheezy/mongodb-org/unstable -> 2.5
#
for unstable_branch_dir in "$repodir"/yum/redhat/*/*/$unstable_branch "$repodir"/yum/amazon/*/*/$unstable_branch "$repodir"/apt/debian/dists/*/*/$unstable_branch "$repodir"/apt/ubuntu/dists/*/*/$unstable_branch "$repodir"/zypper/suse/*/*/$unstable_branch
do
  full_unstable_path=$(dirname "$unstable_branch_dir")/unstable
  if [ -e "$unstable_branch_dir" -a ! -e "$full_unstable_path" ]
  then
    echo "Linking unstable branch directory $unstable_branch_dir to $full_unstable_path"
    ln -s $unstable_branch $full_unstable_path
  fi
done

for stable_branch_dir in "$repodir"/yum/redhat/*/*/$stable_branch "$repodir"/yum/amazon/*/*/$stable_branch "$repodir"/apt/debian/dists/*/*/$stable_branch "$repodir"/apt/ubuntu/dists/*/*/$stable_branch "$repodir"/zypper/suse/*/*/$stable_branch
do
  full_stable_path=$(dirname "$stable_branch_dir")/stable
  if [ -e "$stable_branch_dir" -a ! -e "$full_stable_path" ]
  then
    echo "Linking stable branch directory $stable_branch_dir to $full_stable_path"
    ln -s $stable_branch $full_stable_path
  fi
done

for rpm_dir in $(find "$repodir"/yum/redhat "$repodir"/yum/amazon "$repodir"/zypper/suse -type d -name x86_64 -o -name i386)
do
  echo "Generating redhat repo metadata under $rpm_dir"
  cd "$rpm_dir"
  createrepo .
done

#!/bin/bash
#
# consolidate-repos-enterprise.sh
#
# Create new repo directory under /var/www-enterprise/repo.consolidated
# containing every deb and every rpm under /var/www-enterprise/ with proper
# repo metadata for apt and yum
#

source_dir=/var/www-enterprise/

repodir=/var/www-enterprise/repo.consolidated

gpg_recip='<richard@10gen.com>'

stable_branch="2.6"
unstable_branch="2.7"

echo "Using directory: $repodir"

# set up repo dirs if they don't exist
#
mkdir -p "$repodir/apt/ubuntu"
mkdir -p "$repodir/apt/debian"
mkdir -p "$repodir/yum/redhat"

# to support different $releasever values in yum repo configurations
#
if [ ! -e "$repodir/yum/redhat/7Server" ]
then
  ln -s 7 "$repodir/yum/redhat/7Server"
fi

if [ ! -e "$repodir/yum/redhat/6Server" ]
then
  ln -s 6 "$repodir/yum/redhat/6Server"
fi

if [ ! -e "$repodir/yum/redhat/5Server" ]
then
  ln -s 5 "$repodir/yum/redhat/5Server"
fi

echo "Scanning and copying package files from $source_dir"
echo ". = skipping existing file, @ = copying file"
for package in $(find "$source_dir" -not \( -path "$repodir" -prune \) -and \( -name \*.rpm -o -name \*.deb -o -name Release \))
do
  new_package_location="$repodir$(echo "$package" | sed 's/\/var\/www-enterprise\/[^\/]*//;')"
  # skip if the directory structure looks weird
  #
  if echo "$new_package_location" | grep -q /repo/
  then
    continue
  fi

  # skip if not enterprise package
  #
  if ! echo "$new_package_location" | grep -q enterprise
  then
    continue
  fi
  # skip if it's already there
  #
  if [ -e "$new_package_location" -a "$(basename "$package")" != "Release" ]
  then
      echo -n .
  else
    mkdir -p "$(dirname "$new_package_location")"
    echo -n @
    cp "$package" "$new_package_location"
  fi
done
echo

# packages are in place, now create metadata
#
for debian_dir in "$repodir"/apt/ubuntu "$repodir"/apt/debian
do
  cd "$debian_dir"
  for section_dir in $(find dists -type d -name multiverse -o -name main)
  do
    for arch_dir in "$section_dir"/{binary-i386,binary-amd64}
    do
      echo "Generating Packages file under $debian_dir/$arch_dir"
      if [ ! -d $arch_dir ]
      then
        mkdir $arch_dir
      fi
      dpkg-scanpackages --multiversion "$arch_dir"   > "$arch_dir"/Packages
      gzip -9c  "$arch_dir"/Packages >  "$arch_dir"/Packages.gz
    done
  done

  for release_file in $(find "$debian_dir" -name Release)
  do
    release_dir=$(dirname "$release_file")
    echo "Generating Release file under $release_dir"
    cd $release_dir
    tempfile=$(mktemp /tmp/ReleaseXXXXXX)
    tempfile2=$(mktemp /tmp/ReleaseXXXXXX)
    mv Release $tempfile
    head -7 $tempfile > $tempfile2
    apt-ftparchive release . >> $tempfile2
    cp $tempfile2 Release
    chmod 644 Release
    rm Release.gpg
    echo "Signing Release file"
    gpg -r "$gpg_recip" --no-secmem-warning -abs --output Release.gpg  Release
  done
done

# Create symlinks for stable and unstable branches
#
# Examples:
#
# /var/www-enterprise/repo.consolidated/yum/redhat/5/mongodb-enterprise/unstable -> 2.5
# /var/www-enterprise/repo.consolidated/yum/redhat/6/mongodb-enterprise/unstable -> 2.5
# /var/www-enterprise/repo.consolidated/apt/ubuntu/dists/precise/mongodb-enterprise/unstable -> 2.5
# /var/www-enterprise/repo.consolidated/apt/debian/dists/wheezy/mongodb-enterprise/unstable -> 2.5
#
for unstable_branch_dir in "$repodir"/yum/redhat/*/*/$unstable_branch "$repodir"/yum/amazon/*/*/$unstable_branch "$repodir"/apt/debian/dists/*/*/$unstable_branch "$repodir"/apt/ubuntu/dists/*/*/$unstable_branch "$repodir"/zypper/suse/*/*/$unstable_branch
do
  full_unstable_path=$(dirname "$unstable_branch_dir")/unstable
  if [ -e "$unstable_branch_dir" -a ! -e "$full_unstable_path" ]
  then
    echo "Linking unstable branch directory $unstable_branch_dir to $full_unstable_path"
    ln -s $unstable_branch $full_unstable_path
  fi
done

for stable_branch_dir in "$repodir"/yum/redhat/*/*/$stable_branch "$repodir"/yum/amazon/*/*/$stable_branch "$repodir"/apt/debian/dists/*/*/$stable_branch "$repodir"/apt/ubuntu/dists/*/*/$stable_branch "$repodir"/zypper/suse/*/*/$stable_branch
do
  full_stable_path=$(dirname "$stable_branch_dir")/stable
  if [ -e "$stable_branch_dir" -a ! -e "$full_stable_path" ]
  then
    echo "Linking stable branch directory $stable_branch_dir to $full_stable_path"
    ln -s $stable_branch $full_stable_path
  fi
done

for rpm_dir in $(find "$repodir"/yum/redhat "$repodir"/zypper/suse -type d -name x86_64 -o -name i386)
do
  echo "Generating redhat repo metadata under $rpm_dir"
  cd "$rpm_dir"
  createrepo .
done

#!/usr/bin/env python3
"""Produce a report of all assertions in the MongoDB server codebase.

Parses .cpp files for assertions and verifies assertion codes are distinct.
Optionally replaces zero codes in source code with new distinct values.
"""

import bisect
import os.path
import sys
from collections import defaultdict, namedtuple
from optparse import OptionParser
from functools import reduce
from pathlib import Path

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import regex as re
except ImportError:
    print("*** Run 'pip3 install --user regex' to speed up error code checking")
    import re  # type: ignore

ASSERT_NAMES = ["uassert", "massert", "fassert", "fassertFailed"]
MAXIMUM_CODE = 9999999  # JIRA Ticket + XX

# pylint: disable=invalid-name
codes = []  # type: ignore
# pylint: enable=invalid-name

# Each AssertLocation identifies the C++ source location of an assertion
AssertLocation = namedtuple("AssertLocation", ['sourceFile', 'byteOffset', 'lines', 'code'])

list_files = False  # pylint: disable=invalid-name

_CODE_PATTERNS = [
    re.compile(p + r'\s*(?P<code>\d+)', re.MULTILINE) for p in [
        # All the asserts and their optional variant suffixes
        r"(?:f|i|m|msg|t|u)(?:assert)"
        r"(?:ed)?"
        r"(?:Failed)?"
        r"(?:WithStatus)?"
        r"(?:NoTrace)?"
        r"(?:StatusOK)?"
        r"(?:WithContext)?"
        r"\s*\(",
        # DBException and AssertionException constructors
        r"(?:DB|Assertion)Exception\s*[({]",
        # Calls to all LOGV2* variants
        r"LOGV2(?:\w*)?\s*\(",
        # Forwards a dynamic code to LOGV2
        r"logAndBackoff\(",
        # Error coersions
        r"ErrorCodes::Error\s*[({]",
    ]
]

_DIR_EXCLUDE_RE = re.compile(r'(\..*'
                             r'|pcre-.*'
                             r'|32bit.*'
                             r'|mongodb-.*'
                             r'|debian.*'
                             r'|mongo-cxx-driver.*'
                             r'|.*gotools.*'
                             r'|.*mozjs.*'
                             r')')

_FILE_INCLUDE_RE = re.compile(r'.*\.(cpp|c|h|py|idl)')


def get_all_source_files(prefix='.'):
    """Return source files."""

    def walk(path):
        for fx in path.iterdir():
            if fx.is_dir():
                if fx.is_symlink() and fx.parent.name != "modules":
                    continue
                if _DIR_EXCLUDE_RE.fullmatch(fx.name):
                    continue
                for child in walk(fx):
                    yield child
            elif fx.is_file() and _FILE_INCLUDE_RE.fullmatch(fx.name):
                yield fx

    for child in walk(Path(prefix)):
        yield str(child)


def foreach_source_file(callback, src_root):
    """Invoke a callback on the text of each source file."""
    for source_file in get_all_source_files(prefix=src_root):
        if list_files:
            print('scanning file: ' + source_file)
        with open(source_file, 'r', encoding='utf-8') as fh:
            callback(source_file, fh.read())


def parse_source_files(callback, src_root):
    """Walk MongoDB sourcefiles and invoke a callback for each AssertLocation found."""

    def scan_for_codes(source_file, text):
        for pat in _CODE_PATTERNS:
            for match in pat.finditer(text):
                # Note that this will include the text of the full match but will report the
                # position of the beginning of the code portion rather than the beginning of the
                # match. This is to position editors on the spot that needs to change.
                loc = AssertLocation(source_file, match.start('code'), match.group(0),
                                     match.group('code'))
                callback(loc)

    foreach_source_file(scan_for_codes, src_root)


def get_line_and_column_for_position(loc, _file_cache=None):
    """Convert an absolute position in a file into a line number."""
    if _file_cache is None:
        _file_cache = {}
    if loc.sourceFile not in _file_cache:
        with open(loc.sourceFile) as fh:
            text = fh.read()
            line_offsets = [0]
            for line in text.splitlines(True):
                line_offsets.append(line_offsets[-1] + len(line))
            _file_cache[loc.sourceFile] = line_offsets

    # These are both 1-based, but line is handled by starting the list with 0.
    line = bisect.bisect(_file_cache[loc.sourceFile], loc.byteOffset)
    column = loc.byteOffset - _file_cache[loc.sourceFile][line - 1] + 1
    return (line, column)


def is_terminated(lines):
    """Determine if assert is terminated, from .cpp/.h source lines as text."""
    code_block = " ".join(lines)
    return ';' in code_block or code_block.count('(') - code_block.count(')') <= 0


def get_next_code(seen, server_ticket=0):
    """Find next unused assertion code.

    Called by: SConstruct and main()
    Since SConstruct calls us, codes[] must be global OR WE REPARSE EVERYTHING
    """
    if not codes:
        (_, _, seen) = read_error_codes()

    if server_ticket:
        # Each SERVER ticket is allocated 100 error codes ranging from TICKET_00 -> TICKET_99.
        def generator(seen, ticket):
            avail_codes = list(range(ticket * 100, (ticket + 1) * 100))
            avail_codes.reverse()
            while avail_codes:
                code = avail_codes.pop()
                if str(code) in seen:
                    continue
                yield code
            return "No more available codes for ticket. Ticket: {}".format(ticket)

        return generator(seen, server_ticket)

    # No server ticket. Return a generator that counts starting at highest + 1.
    highest = reduce(lambda x, y: max(int(x), int(y)), (loc.code for loc in codes))
    return iter(range(highest + 1, MAXIMUM_CODE))


def check_error_codes():
    """Check error codes as SConstruct expects a boolean response from this function."""
    (_, errors, _) = read_error_codes()
    return len(errors) == 0


def read_error_codes(src_root='src/mongo'):
    """Define callback, call parse_source_files() with callback, save matches to global codes list."""
    seen = {}
    errors = []
    dups = defaultdict(list)
    skips = []
    malformed = []  # type: ignore

    # define validation callbacks
    def check_dups(assert_loc):
        """Check for duplicates."""
        codes.append(assert_loc)
        code = assert_loc.code

        if not code in seen:
            seen[code] = assert_loc
        else:
            if not code in dups:
                # on first duplicate, add original to dups, errors
                dups[code].append(seen[code])
                errors.append(seen[code])

            dups[code].append(assert_loc)
            errors.append(assert_loc)

    def validate_code(assert_loc):
        """Check for malformed codes."""
        code = int(assert_loc.code)
        if code > MAXIMUM_CODE:
            malformed.append(assert_loc)
            errors.append(assert_loc)

    def callback(assert_loc):
        validate_code(assert_loc)
        check_dups(assert_loc)

    parse_source_files(callback, src_root)

    if "0" in seen:
        code = "0"
        bad = seen[code]
        errors.append(bad)
        line, col = get_line_and_column_for_position(bad)
        print("ZERO_CODE:")
        print("  %s:%d:%d:%s" % (bad.sourceFile, line, col, bad.lines))

    for loc in skips:
        line, col = get_line_and_column_for_position(loc)
        print("EXCESSIVE SKIPPING OF ERROR CODES:")
        print("  %s:%d:%d:%s" % (loc.sourceFile, line, col, loc.lines))

    for code, locations in list(dups.items()):
        print("DUPLICATE IDS: %s" % code)
        for loc in locations:
            line, col = get_line_and_column_for_position(loc)
            print("  %s:%d:%d:%s" % (loc.sourceFile, line, col, loc.lines))

    for loc in malformed:
        line, col = get_line_and_column_for_position(loc)
        print("MALFORMED ID: %s" % loc.code)
        print("  %s:%d:%d:%s" % (loc.sourceFile, line, col, loc.lines))

    return (codes, errors, seen)


def replace_bad_codes(errors, next_code_generator):  # pylint: disable=too-many-locals
    """Modify C++ source files to replace invalid assertion codes.

    For now, we only modify zero codes.

    Args:
        errors: list of AssertLocation
        next_code_generator: generator -> int, next non-conflicting assertion code
    """
    zero_errors = [e for e in errors if int(e.code) == 0]
    skip_errors = [e for e in errors if int(e.code) != 0]

    for loc in skip_errors:
        line, col = get_line_and_column_for_position(loc)
        print("SKIPPING NONZERO code=%s: %s:%d:%d" % (loc.code, loc.sourceFile, line, col))

    # Dedupe, sort, and reverse so we don't have to update offsets as we go.
    for assert_loc in reversed(sorted(set(zero_errors))):
        (source_file, byte_offset, _, _) = assert_loc
        line_num, _ = get_line_and_column_for_position(assert_loc)
        print("UPDATING_FILE: %s:%s" % (source_file, line_num))

        ln = line_num - 1

        with open(source_file, 'r+') as fh:
            print("LINE_%d_BEFORE:%s" % (line_num, fh.readlines()[ln].rstrip()))

            fh.seek(0)
            text = fh.read()
            assert text[byte_offset] == '0'
            fh.seek(0)
            fh.write(text[:byte_offset])
            fh.write(str(next(next_code_generator)))
            fh.write(text[byte_offset + 1:])
            fh.seek(0)

            print("LINE_%d_AFTER :%s" % (line_num, fh.readlines()[ln].rstrip()))


def coerce_to_number(ticket_value):
    """Coerce the input into a number.

    If the input is a number, return itself. Otherwise parses input strings of two forms.
    'SERVER-12345' and '12345' will both return 12345'.
    """
    if isinstance(ticket_value, int):
        return ticket_value

    ticket_re = re.compile(r'(?:SERVER-)?(\d+)', re.IGNORECASE)
    matches = ticket_re.fullmatch(ticket_value)
    if not matches:
        print("Unknown ticket number. Input: " + ticket_value)
        return -1

    return int(matches.group(1))


def main():
    """Main."""
    parser = OptionParser(description=__doc__.strip())
    parser.add_option("--fix", dest="replace", action="store_true", default=False,
                      help="Fix zero codes in source files [default: %default]")
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true", default=False,
                      help="Suppress output on success [default: %default]")
    parser.add_option("--list-files", dest="list_files", action="store_true", default=False,
                      help="Print the name of each file as it is scanned [default: %default]")
    parser.add_option(
        "--ticket", dest="ticket", type="str", action="store", default=0,
        help="Generate error codes for a given SERVER ticket number. Inputs can be of"
        " the form: `--ticket=12345` or `--ticket=SERVER-12345`.")
    (options, _) = parser.parse_args()

    global list_files  # pylint: disable=global-statement,invalid-name
    list_files = options.list_files

    (_, errors, seen) = read_error_codes()
    ok = len(errors) == 0

    if ok and options.quiet:
        return

    next_code_gen = get_next_code(seen, coerce_to_number(options.ticket))

    print("ok: %s" % ok)
    if not options.replace:
        print("next: %s" % next(next_code_gen))

    if ok:
        sys.exit(0)
    elif options.replace:
        replace_bad_codes(errors, next_code_gen)
    else:
        print(ERROR_HELP)
        sys.exit(1)


ERROR_HELP = """
ERRORS DETECTED. To correct, run "buildscripts/errorcodes.py --fix" to replace zero codes.
Other errors require manual correction.
"""

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""ESLint module.

Will download a prebuilt ESLint binary if necessary (i.e. it isn't installed, isn't in the current
path, or is the wrong version). It works in much the same way as clang_format.py. In lint mode, it
will lint the files or directory paths passed. In lint-patch mode, for upload.py, it will see if
there are any candidate files in the supplied patch. Fix mode will run ESLint with the --fix
option, and that will update the files with missing semicolons and similar repairable issues.
There is also a -d mode that assumes you only want to run one copy of ESLint per file / directory
parameter supplied. This lets ESLint search for candidate files to lint.
"""

import logging
import os
import shutil
import string
import subprocess
import sys
import tarfile
import tempfile
import threading
from typing import Optional
import urllib.error
import urllib.parse
import urllib.request

from distutils import spawn  # pylint: disable=no-name-in-module
from optparse import OptionParser
import structlog

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.realpath(__file__)))))

# pylint: disable=wrong-import-position
from buildscripts.linter.filediff import gather_changed_files_for_lint
from buildscripts.linter import git, parallel
# pylint: enable=wrong-import-position

##############################################################################
#
# Constants for ESLint
#
#

# Expected version of ESLint.
# If you want to update the version, please refer to `buildscripts/eslint/README.md`
ESLINT_VERSION = "7.22.0"

# Name of ESLint as a binary.
ESLINT_PROGNAME = "eslint"

# URL location of our provided ESLint binaries.
ESLINT_HTTP_LINUX_CACHE = "https://s3.amazonaws.com/boxes.10gen.com/build/eslint-" + \
                           ESLINT_VERSION + "-linux.tar.gz"
ESLINT_HTTP_DARWIN_CACHE = "https://s3.amazonaws.com/boxes.10gen.com/build/eslint-" + \
                            ESLINT_VERSION + "-darwin.tar.gz"

# Path in the tarball to the ESLint binary.
ESLINT_SOURCE_TAR_BASE = string.Template(ESLINT_PROGNAME + "-$platform-$arch")

LOGGER = structlog.get_logger(__name__)


def callo(args):
    """Call a program, and capture its output."""
    return subprocess.check_output(args).decode('utf-8')


def extract_eslint(tar_path, target_file):
    """Extract ESLint tar file."""
    tarfp = tarfile.open(tar_path)
    for name in tarfp.getnames():
        if name == target_file:
            tarfp.extract(name)
    tarfp.close()


def get_eslint_from_cache(dest_file, platform, arch):
    """Get ESLint binary from mongodb's cache."""
    # Get URL
    if platform == "Linux":
        url = ESLINT_HTTP_LINUX_CACHE
    elif platform == "Darwin":
        url = ESLINT_HTTP_DARWIN_CACHE
    else:
        raise ValueError('ESLint is not available as a binary for ' + platform)

    dest_dir = tempfile.gettempdir()
    temp_tar_file = os.path.join(dest_dir, "temp.tar.gz")

    # Download the file
    print("Downloading ESLint %s from %s, saving to %s" % (ESLINT_VERSION, url, temp_tar_file))
    urllib.request.urlretrieve(url, temp_tar_file)

    # pylint: disable=too-many-function-args
    print("Extracting ESLint %s to %s" % (ESLINT_VERSION, dest_file))
    eslint_distfile = ESLINT_SOURCE_TAR_BASE.substitute(platform=platform, arch=arch)
    extract_eslint(temp_tar_file, eslint_distfile)
    shutil.move(eslint_distfile, dest_file)


class ESLint(object):
    """Class encapsulates finding a suitable copy of ESLint, and linting an individual file."""

    def __init__(self, path, cache_dir):  # pylint: disable=too-many-branches
        """Initialize ESLint."""
        eslint_progname = ESLINT_PROGNAME

        # Initialize ESLint configuration information
        if sys.platform.startswith("linux"):
            self.arch = "x86_64"
            self.tar_path = None
        elif sys.platform == "darwin":
            self.arch = "x86_64"
            self.tar_path = None

        self.path = None

        # Find ESLint now
        if path is not None:
            if os.path.isfile(path):
                self.path = path
            else:
                print("WARNING: Could not find ESLint at %s" % path)

        # Check the environment variable
        if "MONGO_ESLINT" in os.environ:
            self.path = os.environ["MONGO_ESLINT"]

            if self.path and not self._validate_version(warn=True):
                self.path = None

        # Check the user's PATH environment variable now
        if self.path is None:
            self.path = spawn.find_executable(eslint_progname)

            if self.path and not self._validate_version(warn=True):
                self.path = None

        # Have not found it yet, download it from the web
        if self.path is None:
            if not os.path.isdir(cache_dir):
                os.makedirs(cache_dir)

            self.path = os.path.join(cache_dir, eslint_progname)

            if os.path.isfile(self.path) and not self._validate_version(warn=True):
                print(
                    "WARNING: removing ESLint from %s to download the correct version" % self.path)
                os.remove(self.path)

            if not os.path.isfile(self.path):
                if sys.platform.startswith("linux"):
                    get_eslint_from_cache(self.path, "Linux", self.arch)
                elif sys.platform == "darwin":
                    get_eslint_from_cache(self.path, "Darwin", self.arch)
                else:
                    print("ERROR: eslint.py does not support downloading ESLint "
                          "on this platform, please install ESLint " + ESLINT_VERSION)
        # Validate we have the correct version
        if not self._validate_version():
            raise ValueError('correct version of ESLint was not found.')

        self.print_lock = threading.Lock()

    def _validate_version(self, warn=False):
        """Validate ESLint is the expected version."""
        esl_version = callo([self.path, "--version"]).rstrip()
        # Ignore the leading v in the version string.
        if ESLINT_VERSION == esl_version[1:]:
            return True

        if warn:
            print("WARNING: ESLint found in path %s, but the version is incorrect: %s" %
                  (self.path, esl_version))
        return False

    def _lint(self, file_name, print_diff):
        """Check the specified file for linting errors."""
        # ESLint returns non-zero on a linting error. That's all we care about
        # so only enter the printing logic if we have an error.
        try:
            callo([self.path, "-f", "unix", file_name])
        except subprocess.CalledProcessError as err:
            if print_diff:
                # Take a lock to ensure error messages do not get mixed when printed to the screen
                with self.print_lock:
                    print("ERROR: ESLint found errors in " + file_name)
                    print(err.output)
            return False

        return True

    def lint(self, file_name):
        """Check the specified file has no linting errors."""
        return self._lint(file_name, print_diff=True)

    def autofix(self, file_name):
        """Run ESLint in fix mode."""
        return not subprocess.call([self.path, "--fix", file_name])


def is_interesting_file(file_name):
    """Return true if this file should be checked."""
    return ((file_name.startswith("src/mongo") or file_name.startswith("jstests"))
            and file_name.endswith(".js"))


def _get_build_dir():
    """Get the location of the scons build directory in case we need to download ESLint."""
    return os.path.join(git.get_base_dir(), "build")


def _lint_files(eslint, files):
    """Lint a list of files with ESLint."""
    eslint = ESLint(eslint, _get_build_dir())

    print("Running ESLint %s at %s" % (ESLINT_VERSION, eslint.path))
    lint_clean = parallel.parallel_process([os.path.abspath(f) for f in files], eslint.lint)

    if not lint_clean:
        print("ERROR: ESLint found errors. Run ESLint manually to see errors in "
              "files that were skipped")
        sys.exit(1)

    return True


def lint_patch(eslint, infile):
    """Lint patch command entry point."""
    files = git.get_files_to_check_from_patch(infile, is_interesting_file)

    # Patch may have files that we do not want to check which is fine
    if files:
        return _lint_files(eslint, files)
    return True


def lint_git_diff(eslint: Optional[str]) -> bool:
    """
    Lint the files that have changes since the last git commit.

    :param eslint: Path to eslint command.
    :return: True if lint was successful.
    """
    files = gather_changed_files_for_lint(is_interesting_file)

    # Patch may have files that we do not want to check which is fine
    if files:
        return _lint_files(eslint, files)
    return True


def lint(eslint, dirmode, glob):
    """Lint files command entry point."""
    if dirmode and glob:
        files = glob
    else:
        files = git.get_files_to_check(glob, is_interesting_file)

    _lint_files(eslint, files)

    return True


def _autofix_files(eslint, files):
    """Auto-fix the specified files with ESLint."""
    eslint = ESLint(eslint, _get_build_dir())

    print("Running ESLint %s at %s" % (ESLINT_VERSION, eslint.path))
    autofix_clean = parallel.parallel_process([os.path.abspath(f) for f in files], eslint.autofix)

    if not autofix_clean:
        print("ERROR: failed to auto-fix files")
        return False
    return True


def autofix_func(eslint, dirmode, glob):
    """Auto-fix files command entry point."""
    if dirmode:
        files = glob
    else:
        files = git.get_files_to_check(glob, is_interesting_file)

    return _autofix_files(eslint, files)


def main():
    """Execute Main entry point."""
    success = False
    usage = "%prog [-e <eslint>] [-d] lint|lint-patch|fix [glob patterns] "
    description = ("The script will try to find ESLint version %s on your system and run it. "
                   "If it won't find the version it will try to download it and then run it. "
                   "Commands description: lint runs ESLint on provided patterns or all .js "
                   "files under `jstests/` and `src/mongo`; "
                   "lint-patch runs ESLint against .js files modified in the "
                   "provided patch file (for upload.py); "
                   "fix runs ESLint with --fix on provided patterns "
                   "or files under jstests/ and src/mongo." % ESLINT_VERSION)
    epilog = "*Unless you specify -d a separate ESLint process will be launched for every file"
    parser = OptionParser(usage=usage, description=description, epilog=epilog)
    parser.add_option(
        "-e",
        "--eslint",
        type="string",
        dest="eslint",
        help="Fully qualified path to eslint executable",
    )
    parser.add_option(
        "-d",
        "--dirmode",
        action="store_true",
        default=True,
        dest="dirmode",
        help="Considers the glob patterns as directories and runs ESLint process "
        "against each pattern",
    )

    (options, args) = parser.parse_args(args=sys.argv)

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    structlog.configure(logger_factory=structlog.stdlib.LoggerFactory())

    if len(args) > 1:
        command = args[1]
        searchlist = args[2:]
        if not searchlist:
            searchlist = ["jstests/", "src/mongo/"]

        if command == "lint":
            success = lint(options.eslint, options.dirmode, searchlist)
        elif command == "lint-patch":
            if not args[2:]:
                success = False
                print("You must provide the patch's fully qualified file name with lint-patch")
            else:
                success = lint_patch(options.eslint, searchlist)
        elif command == "lint-git-diff":
            success = lint_git_diff(options.eslint)
        elif command == "fix":
            success = autofix_func(options.eslint, options.dirmode, searchlist)
        else:
            parser.print_help()
    else:
        parser.print_help()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Activate an evergreen task in the existing build."""
import os
import sys

import click
import structlog
from pydantic.main import BaseModel
from evergreen.api import RetryingEvergreenApi, EvergreenApi

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pylint: disable=wrong-import-position
from buildscripts.util.cmdutils import enable_logging
from buildscripts.util.fileops import read_yaml_file
from buildscripts.util.taskname import remove_gen_suffix
# pylint: enable=wrong-import-position

LOGGER = structlog.getLogger(__name__)

EVG_CONFIG_FILE = "./.evergreen.yml"


class EvgExpansions(BaseModel):
    """
    Evergreen expansions file contents.

    build_id: ID of build being run.
    task_name: Name of task creating the generated configuration.
    """

    build_id: str
    task_name: str

    @classmethod
    def from_yaml_file(cls, path: str) -> "EvgExpansions":
        """Read the generation configuration from the given file."""
        return cls(**read_yaml_file(path))

    @property
    def task(self) -> str:
        """Get the task being generated."""
        return remove_gen_suffix(self.task_name)


def activate_task(build_id: str, task_name: str, evg_api: EvergreenApi) -> None:
    """
    Activate the given task in the specified build.

    :param build_id: Build to activate task in.
    :param task_name: Name of task to activate.
    :param evg_api: Evergreen API client.
    """
    build = evg_api.build_by_id(build_id)
    task_list = build.get_tasks()
    for task in task_list:
        if task.display_name == task_name:
            LOGGER.info("Activating task", task_id=task.task_id, task_name=task.display_name)
            evg_api.configure_task(task.task_id, activated=True)

            # if any(ARCHIVE_DIST_TEST_TASK in dependency["id"] for dependency in task.depends_on):
            #     _activate_archive_debug_symbols(evg_api, task_list)


# def _activate_archive_debug_symbols(evg_api: EvergreenApi, task_list):
#     debug_iter = filter(lambda tsk: tsk.display_name == ACTIVATE_ARCHIVE_DIST_TEST_DEBUG_TASK,
#                         task_list)
#     activate_symbol_tasks = list(debug_iter)
#
#     if len(activate_symbol_tasks) == 1:
#         activated_symbol_task = activate_symbol_tasks[0]
#         if not activated_symbol_task.activated:
#             LOGGER.info("Activating debug symbols archival", task_id=activated_symbol_task.task_id)
#             evg_api.configure_task(activated_symbol_task.task_id, activated=True)


@click.command()
@click.option("--expansion-file", type=str, required=True,
              help="Location of expansions file generated by evergreen.")
@click.option("--evergreen-config", type=str, default=EVG_CONFIG_FILE,
              help="Location of evergreen configuration file.")
@click.option("--verbose", is_flag=True, default=False, help="Enable verbose logging.")
def main(expansion_file: str, evergreen_config: str, verbose: bool) -> None:
    """
    Activate the associated generated executions based in the running build.

    The `--expansion-file` should contain all the configuration needed to generate the tasks.
    \f
    :param expansion_file: Configuration file.
    :param evergreen_config: Evergreen configuration file.
    :param verbose: Use verbose logging.
    """
    enable_logging(verbose)
    expansions = EvgExpansions.from_yaml_file(expansion_file)
    evg_api = RetryingEvergreenApi.get_api(config_file=evergreen_config)

    activate_task(expansions.build_id, expansions.task, evg_api)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

#!/usr/bin/env python3
"""Wrapper around burn_in_tests for evergreen execution."""
import logging
import os
import sys
from datetime import datetime, timedelta
from math import ceil
from typing import Optional, List, Dict, Set

import click
import requests
import structlog
from git import Repo
from shrub.v2 import ShrubProject, BuildVariant, Task, TaskDependency, ExistingTask
from evergreen import RetryingEvergreenApi, EvergreenApi

from buildscripts.burn_in_tests import RepeatConfig, BurnInExecutor, TaskInfo, FileChangeDetector, \
    DEFAULT_REPO_LOCATIONS, BurnInOrchestrator
from buildscripts.ciconfig.evergreen import parse_evergreen_file, EvergreenProjectConfig
from buildscripts.patch_builds.change_data import RevisionMap
from buildscripts.patch_builds.evg_change_data import generate_revision_map_from_manifest
from buildscripts.patch_builds.task_generation import TimeoutInfo, resmoke_commands, \
    validate_task_generation_limit
from buildscripts.task_generation.constants import CONFIG_FILE, EVERGREEN_FILE, ARCHIVE_DIST_TEST_DEBUG_TASK
from buildscripts.util.fileops import write_file
from buildscripts.util.taskname import name_generated_task
from buildscripts.util.teststats import TestRuntime, HistoricTaskData

DEFAULT_PROJECT = "mongodb-mongo-master"
DEFAULT_VARIANT = "enterprise-rhel-80-64-bit-dynamic-required"
BURN_IN_TESTS_GEN_TASK = "burn_in_tests_gen"
BURN_IN_TESTS_TASK = "burn_in_tests"
AVG_TEST_RUNTIME_ANALYSIS_DAYS = 14
AVG_TEST_SETUP_SEC = 4 * 60
AVG_TEST_TIME_MULTIPLIER = 3
MIN_AVG_TEST_OVERFLOW_SEC = float(60)
MIN_AVG_TEST_TIME_SEC = 5 * 60

LOGGER = structlog.getLogger(__name__)
EXTERNAL_LOGGERS = {
    "evergreen",
    "git",
    "urllib3",
}


def _configure_logging(verbose: bool):
    """
    Configure logging for the application.

    :param verbose: If True set log level to DEBUG.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="[%(asctime)s - %(name)s - %(levelname)s] %(message)s",
        level=level,
        stream=sys.stdout,
    )
    for log_name in EXTERNAL_LOGGERS:
        logging.getLogger(log_name).setLevel(logging.WARNING)


class GenerateConfig(object):
    """Configuration for how to generate tasks."""

    def __init__(self, build_variant: str, project: str, run_build_variant: Optional[str] = None,
                 distro: Optional[str] = None, task_id: Optional[str] = None,
                 task_prefix: str = "burn_in", include_gen_task: bool = True) -> None:
        # pylint: disable=too-many-arguments,too-many-locals
        """
        Create a GenerateConfig.

        :param build_variant: Build variant to get tasks from.
        :param project: Project to run tasks on.
        :param run_build_variant: Build variant to run new tasks on.
        :param distro: Distro to run tasks on.
        :param task_id: Evergreen task being run under.
        :param task_prefix: Prefix to include in generated task names.
        :param include_gen_task: Indicates the "_gen" task should be grouped in the display task.
        """
        self.build_variant = build_variant
        self._run_build_variant = run_build_variant
        self.distro = distro
        self.project = project
        self.task_id = task_id
        self.task_prefix = task_prefix
        self.include_gen_task = include_gen_task

    @property
    def run_build_variant(self):
        """Build variant tasks should run against."""
        if self._run_build_variant:
            return self._run_build_variant
        return self.build_variant

    def validate(self, evg_conf: EvergreenProjectConfig):
        """
        Raise an exception if this configuration is invalid.

        :param evg_conf: Evergreen configuration.
        :return: self.
        """
        self._check_variant(self.build_variant, evg_conf)
        return self

    @staticmethod
    def _check_variant(build_variant: str, evg_conf: EvergreenProjectConfig):
        """
        Check if the build_variant is found in the evergreen file.

        :param build_variant: Build variant to check.
        :param evg_conf: Evergreen configuration to check against.
        """
        if not evg_conf.get_variant(build_variant):
            raise ValueError(f"Build variant '{build_variant}' not found in Evergreen file")


def _parse_avg_test_runtime(test: str,
                            task_avg_test_runtime_stats: List[TestRuntime]) -> Optional[float]:
    """
    Parse list of test runtimes to find runtime for particular test.

    :param task_avg_test_runtime_stats: List of average historic runtimes of tests.
    :param test: Test name.
    :return: Historical average runtime of the test.
    """
    for test_stat in task_avg_test_runtime_stats:
        if test_stat.test_name == test:
            return test_stat.runtime
    return None


def _calculate_timeout(avg_test_runtime: float) -> int:
    """
    Calculate timeout_secs for the Evergreen task.

    :param avg_test_runtime: How long a test has historically taken to run.
    :return: The test runtime times AVG_TEST_TIME_MULTIPLIER, or MIN_AVG_TEST_TIME_SEC (whichever
        is higher).
    """
    return max(MIN_AVG_TEST_TIME_SEC, ceil(avg_test_runtime * AVG_TEST_TIME_MULTIPLIER))


def _calculate_exec_timeout(repeat_config: RepeatConfig, avg_test_runtime: float) -> int:
    """
    Calculate exec_timeout_secs for the Evergreen task.

    :param repeat_config: Information about how the test will repeat.
    :param avg_test_runtime: How long a test has historically taken to run.
    :return: repeat_tests_secs + an amount of padding time so that the test has time to finish on
        its final run.
    """
    LOGGER.debug("Calculating exec timeout", repeat_config=repeat_config,
                 avg_test_runtime=avg_test_runtime)
    repeat_tests_secs = repeat_config.repeat_tests_secs
    if avg_test_runtime > repeat_tests_secs and repeat_config.repeat_tests_min:
        # If a single execution of the test takes longer than the repeat time, then we don't
        # have to worry about the repeat time at all and can just use the average test runtime
        # and minimum number of executions to calculate the exec timeout value.
        return ceil(avg_test_runtime * AVG_TEST_TIME_MULTIPLIER * repeat_config.repeat_tests_min)

    test_execution_time_over_limit = avg_test_runtime - (repeat_tests_secs % avg_test_runtime)
    test_execution_time_over_limit = max(MIN_AVG_TEST_OVERFLOW_SEC, test_execution_time_over_limit)
    return ceil(repeat_tests_secs + (test_execution_time_over_limit * AVG_TEST_TIME_MULTIPLIER) +
                AVG_TEST_SETUP_SEC)


class TaskGenerator:
    """Class to generate task configurations."""

    def __init__(self, generate_config: GenerateConfig, repeat_config: RepeatConfig,
                 task_info: TaskInfo, task_runtime_stats: List[TestRuntime]) -> None:
        """
        Create a new task generator.

        :param generate_config: Generate configuration to use.
        :param repeat_config: Repeat configuration to use.
        :param task_info: Information about how tasks should be generated.
        :param task_runtime_stats: Historic runtime of tests associated with task.
        """
        self.generate_config = generate_config
        self.repeat_config = repeat_config
        self.task_info = task_info
        self.task_runtime_stats = task_runtime_stats

    def generate_timeouts(self, test: str) -> TimeoutInfo:
        """
        Add timeout.update command to list of commands for a burn in execution task.

        :param test: Test name.
        :return: TimeoutInfo to use.
        """
        if self.task_runtime_stats:
            avg_test_runtime = _parse_avg_test_runtime(test, self.task_runtime_stats)
            if avg_test_runtime:
                LOGGER.debug("Avg test runtime", test=test, runtime=avg_test_runtime)

                timeout = _calculate_timeout(avg_test_runtime)
                exec_timeout = _calculate_exec_timeout(self.repeat_config, avg_test_runtime)
                LOGGER.debug("Using timeout overrides", exec_timeout=exec_timeout, timeout=timeout)
                timeout_info = TimeoutInfo.overridden(exec_timeout, timeout)

                LOGGER.debug("Override runtime for test", test=test, timeout=timeout_info)
                return timeout_info

        return TimeoutInfo.default_timeout()

    def generate_name(self, index: int) -> str:
        """
        Generate a subtask name.

        :param index: Index of subtask.
        :return: Name to use for generated sub-task.
        """
        prefix = self.generate_config.task_prefix
        task_name = self.task_info.display_task_name
        return name_generated_task(f"{prefix}:{task_name}", index, len(self.task_info.tests),
                                   self.generate_config.run_build_variant)

    def create_task(self, index: int, test_name: str) -> Task:
        """
        Create the task configuration for the given test using the given index.

        :param index: Index of sub-task being created.
        :param test_name: Name of test that should be executed.
        :return: Configuration for generating the specified task.
        """
        resmoke_args = self.task_info.resmoke_args

        sub_task_name = self.generate_name(index)
        LOGGER.debug("Generating sub-task", sub_task=sub_task_name)

        test_unix_style = test_name.replace('\\', '/')
        run_tests_vars = {
            "resmoke_args":
                f"{resmoke_args} {self.repeat_config.generate_resmoke_options()} {test_unix_style}"
        }

        timeout = self.generate_timeouts(test_name)
        commands = resmoke_commands("run tests", run_tests_vars, timeout,
                                    self.task_info.require_multiversion)
        dependencies = {TaskDependency(ARCHIVE_DIST_TEST_DEBUG_TASK)}

        return Task(sub_task_name, commands, dependencies)


class EvergreenFileChangeDetector(FileChangeDetector):
    """A file changes detector for detecting test change in evergreen."""

    def __init__(self, task_id: str, evg_api: EvergreenApi) -> None:
        """
        Create a new evergreen file change detector.

        :param task_id: Id of task being run under.
        :param evg_api: Evergreen API client.
        """
        self.task_id = task_id
        self.evg_api = evg_api

    def create_revision_map(self, repos: List[Repo]) -> RevisionMap:
        """
        Create a map of the repos and the given revisions to diff against.

        :param repos: List of repos being tracked.
        :return: Map of repositories and revisions to diff against.
        """
        return generate_revision_map_from_manifest(repos, self.task_id, self.evg_api)


class GenerateBurnInExecutor(BurnInExecutor):
    """A burn-in executor that generates tasks."""

    # pylint: disable=too-many-arguments
    def __init__(self, generate_config: GenerateConfig, repeat_config: RepeatConfig,
                 evg_api: EvergreenApi, generate_tasks_file: Optional[str] = None,
                 history_end_date: Optional[datetime] = None) -> None:
        """
        Create a new generate burn-in executor.

        :param generate_config: Configuration for how to generate tasks.
        :param repeat_config: Configuration for how tests should be repeated.
        :param evg_api: Evergreen API client.
        :param generate_tasks_file: File to write generated task configuration to.
        :param history_end_date: End date of range to query for historic test data.
        """
        self.generate_config = generate_config
        self.repeat_config = repeat_config
        self.evg_api = evg_api
        self.generate_tasks_file = generate_tasks_file
        self.history_end_date = history_end_date if history_end_date else datetime.utcnow()\
            .replace(microsecond=0)

    def get_task_runtime_history(self, task: str) -> List[TestRuntime]:
        """
        Query the runtime history of the specified task.

        :param task: Task to query.
        :return: List of runtime histories for all tests in specified task.
        """
        try:
            project = self.generate_config.project
            variant = self.generate_config.build_variant
            end_date = self.history_end_date
            start_date = end_date - timedelta(days=AVG_TEST_RUNTIME_ANALYSIS_DAYS)
            test_stats = HistoricTaskData.from_evg(self.evg_api, project, start_date=start_date,
                                                   end_date=end_date, task=task, variant=variant)
            return test_stats.get_tests_runtimes()
        except requests.HTTPError as err:
            if err.response.status_code == requests.codes.SERVICE_UNAVAILABLE:
                # Evergreen may return a 503 when the service is degraded.
                # We fall back to returning no test history
                return []
            else:
                raise

    def create_generated_tasks(self, tests_by_task: Dict[str, TaskInfo]) -> Set[Task]:
        """
        Create generate.tasks configuration for the the given tests and tasks.

        :param tests_by_task: Dictionary of tasks and test to generate configuration for.
        :return: Shrub tasks containing the configuration for generating specified tasks.
        """
        tasks: Set[Task] = set()
        for task in sorted(tests_by_task):
            task_info = tests_by_task[task]
            task_runtime_stats = self.get_task_runtime_history(task_info.display_task_name)
            task_generator = TaskGenerator(self.generate_config, self.repeat_config, task_info,
                                           task_runtime_stats)

            for index, test_name in enumerate(task_info.tests):
                tasks.add(task_generator.create_task(index, test_name))

        return tasks

    def get_existing_tasks(self) -> Optional[Set[ExistingTask]]:
        """Get any existing tasks that should be included in the generated display task."""
        if self.generate_config.include_gen_task:
            return {ExistingTask(BURN_IN_TESTS_GEN_TASK)}
        return None

    def add_config_for_build_variant(self, build_variant: BuildVariant,
                                     tests_by_task: Dict[str, TaskInfo]) -> None:
        """
        Add configuration for generating tasks to the given build variant.

        :param build_variant: Build variant to update.
        :param tests_by_task: Tasks and tests to update.
        """
        tasks = self.create_generated_tasks(tests_by_task)
        build_variant.display_task(BURN_IN_TESTS_TASK, tasks,
                                   execution_existing_tasks=self.get_existing_tasks())

    def create_generate_tasks_configuration(self, tests_by_task: Dict[str, TaskInfo]) -> str:
        """
        Create the configuration with the configuration to generate the burn_in tasks.

        :param tests_by_task: Dictionary of tasks and test to generate.
        :return: Configuration to use to create generated tasks.
        """
        build_variant = BuildVariant(self.generate_config.run_build_variant)
        self.add_config_for_build_variant(build_variant, tests_by_task)

        shrub_project = ShrubProject.empty()
        shrub_project.add_build_variant(build_variant)

        if not validate_task_generation_limit(shrub_project):
            sys.exit(1)

        return shrub_project.json()

    def execute(self, tests_by_task: Dict[str, TaskInfo]) -> None:
        """
        Execute the given tests in the given tasks.

        :param tests_by_task: Dictionary of tasks to run with tests to run in each.
        """
        json_text = self.create_generate_tasks_configuration(tests_by_task)
        assert self.generate_tasks_file is not None
        if self.generate_tasks_file:
            write_file(self.generate_tasks_file, json_text)


# pylint: disable=too-many-arguments
def burn_in(task_id: str, build_variant: str, generate_config: GenerateConfig,
            repeat_config: RepeatConfig, evg_api: EvergreenApi, evg_conf: EvergreenProjectConfig,
            repos: List[Repo], generate_tasks_file: str) -> None:
    """
    Run burn_in_tests.

    :param task_id: Id of task running.
    :param build_variant: Build variant to run against.
    :param generate_config: Configuration for how to generate tasks.
    :param repeat_config: Configuration for how to repeat tests.
    :param evg_api: Evergreen API client.
    :param evg_conf: Evergreen project configuration.
    :param repos: Git repos containing changes.
    :param generate_tasks_file: File to write generate tasks configuration to.
    """
    change_detector = EvergreenFileChangeDetector(task_id, evg_api)
    executor = GenerateBurnInExecutor(generate_config, repeat_config, evg_api, generate_tasks_file)

    burn_in_orchestrator = BurnInOrchestrator(change_detector, executor, evg_conf)
    burn_in_orchestrator.burn_in(repos, build_variant)


@click.command()
@click.option("--generate-tasks-file", "generate_tasks_file", default=None, metavar='FILE',
              help="Run in 'generate.tasks' mode. Store task config to given file.")
@click.option("--build-variant", "build_variant", default=DEFAULT_VARIANT, metavar='BUILD_VARIANT',
              help="Tasks to run will be selected from this build variant.")
@click.option("--run-build-variant", "run_build_variant", default=None, metavar='BUILD_VARIANT',
              help="Burn in tasks will be generated on this build variant.")
@click.option("--distro", "distro", default=None, metavar='DISTRO',
              help="The distro the tasks will execute on.")
@click.option("--project", "project", default=DEFAULT_PROJECT, metavar='PROJECT',
              help="The evergreen project the tasks will execute on.")
@click.option("--repeat-tests", "repeat_tests_num", default=None, type=int,
              help="Number of times to repeat tests.")
@click.option("--repeat-tests-min", "repeat_tests_min", default=None, type=int,
              help="The minimum number of times to repeat tests if time option is specified.")
@click.option("--repeat-tests-max", "repeat_tests_max", default=None, type=int,
              help="The maximum number of times to repeat tests if time option is specified.")
@click.option("--repeat-tests-secs", "repeat_tests_secs", default=None, type=int, metavar="SECONDS",
              help="Repeat tests for the given time (in secs).")
@click.option("--evg-api-config", "evg_api_config", default=CONFIG_FILE, metavar="FILE",
              help="Configuration file with connection info for Evergreen API.")
@click.option("--verbose", "verbose", default=False, is_flag=True, help="Enable extra logging.")
@click.option("--task_id", "task_id", required=True, metavar='TASK_ID',
              help="The evergreen task id.")
# pylint: disable=too-many-arguments,too-many-locals
def main(build_variant: str, run_build_variant: str, distro: str, project: str,
         generate_tasks_file: str, repeat_tests_num: Optional[int], repeat_tests_min: Optional[int],
         repeat_tests_max: Optional[int], repeat_tests_secs: Optional[int], evg_api_config: str,
         verbose: bool, task_id: str):
    """
    Run new or changed tests in repeated mode to validate their stability.

    burn_in_tests detects jstests that are new or changed since the last git command and then
    runs those tests in a loop to validate their reliability.

    The `--origin-rev` argument allows users to specify which revision should be used as the last
    git command to compare against to find changed files. If the `--origin-rev` argument is provided,
    we find changed files by comparing your latest changes to this revision. If not provided, we
    find changed test files by comparing your latest changes to HEAD. The revision provided must
    be a revision that exists in the mongodb repository.

    The `--repeat-*` arguments allow configuration of how burn_in_tests repeats tests. Tests can
    either be repeated a specified number of times with the `--repeat-tests` option, or they can
    be repeated for a certain time period with the `--repeat-tests-secs` option.

    Specifying the `--generate-tasks-file`, burn_in_tests will run generate a configuration
    file that can then be sent to the Evergreen 'generate.tasks' command to create evergreen tasks
    to do all the test executions. This is the mode used to run tests in patch builds.

    NOTE: There is currently a limit of the number of tasks burn_in_tests will attempt to generate
    in evergreen. The limit is 1000. If you change enough tests that more than 1000 tasks would
    be generated, burn_in_test will fail. This is to avoid generating more tasks than evergreen
    can handle.
    \f

    :param build_variant: Build variant to query tasks from.
    :param run_build_variant:Build variant to actually run against.
    :param distro: Distro to run tests on.
    :param project: Project to run tests on.
    :param generate_tasks_file: Create a generate tasks configuration in this file.
    :param repeat_tests_num: Repeat each test this number of times.
    :param repeat_tests_min: Repeat each test at least this number of times.
    :param repeat_tests_max: Once this number of repetitions has been reached, stop repeating.
    :param repeat_tests_secs: Continue repeating tests for this number of seconds.
    :param evg_api_config: Location of configuration file to connect to evergreen.
    :param verbose: Log extra debug information.
    :param task_id: Id of evergreen task being run in.
    """
    _configure_logging(verbose)

    repeat_config = RepeatConfig(repeat_tests_secs=repeat_tests_secs,
                                 repeat_tests_min=repeat_tests_min,
                                 repeat_tests_max=repeat_tests_max,
                                 repeat_tests_num=repeat_tests_num)  # yapf: disable

    repos = [Repo(x) for x in DEFAULT_REPO_LOCATIONS if os.path.isdir(x)]
    evg_conf = parse_evergreen_file(EVERGREEN_FILE)
    evg_api = RetryingEvergreenApi.get_api(config_file=evg_api_config)

    generate_config = GenerateConfig(build_variant=build_variant,
                                     run_build_variant=run_build_variant,
                                     distro=distro,
                                     project=project,
                                     task_id=task_id)  # yapf: disable
    generate_config.validate(evg_conf)

    burn_in(task_id, build_variant, generate_config, repeat_config, evg_api, evg_conf, repos,
            generate_tasks_file)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

"""Convert Evergreen's expansions.yml to an eval-able shell script."""
import sys
import platform
from shlex import quote
from typing import Any


def _error(msg: str) -> None:
    print(f"___expansions_error={quote(msg)}")
    sys.exit(1)


try:
    import yaml
    import click
except ModuleNotFoundError:
    _error("ERROR: Failed to import a dependency. This is almost certainly because "
           "the task did not initialize the venv immediately after cloning the repository.")


def _load_defaults(defaults_file: str) -> dict:
    with open(defaults_file) as fh:
        defaults = yaml.safe_load(fh)
        if not isinstance(defaults, dict):
            _error("ERROR: expected to read a dictionary. expansions.defaults.yml"
                   "must be a dictionary. Check the indentation.")

        # expansions MUST be strings. Reject any that are not
        bad_expansions = set()
        for key, value in defaults.items():
            if not isinstance(value, str):
                bad_expansions.add(key)

        if bad_expansions:
            _error("ERROR: all default expansions must be strings. You can "
                   " fix this error by quoting the values in expansions.defaults.yml. "
                   "Integers, floating points, 'true', 'false', and 'null' "
                   "must be quoted. The following keys were interpreted as "
                   f"other types: {bad_expansions}")

        # These values show up if 1. Python's str is used to naively convert
        # a boolean to str, 2. A human manually entered one of those strings.
        # Either way, our shell scripts expect 'true' or 'false' (leading
        # lowercase), and we reject them as errors. This will probably save
        # someone a lot of time, but if this assumption proves wrong, start
        # a conversation in #server-testing.
        risky_boolean_keys = set()
        for key, value in defaults.items():
            if value in ("True", "False"):
                risky_boolean_keys.add(key)

        if risky_boolean_keys:
            _error("ERROR: Found keys which had 'True' or 'False' as values. "
                   "Shell scripts assume that booleans are represented as 'true'"
                   " or 'false' (leading lowercase). If you added a new boolean, "
                   "ensure that it's represented in lowercase. If not, please report this in "
                   f"#server-testing. Keys with bad values: {risky_boolean_keys}")

        return defaults


def _load_expansions(expansions_file) -> dict:
    with open(expansions_file) as fh:
        expansions = yaml.safe_load(fh)

        if not isinstance(expansions, dict):
            _error("ERROR: expected to read a dictionary. Has the output format "
                   "of expansions.write changed?")

        if not expansions:
            _error("ERROR: found 0 expansions. This is almost certainly wrong.")

        return expansions


@click.command()
@click.argument("expansions_file", type=str)
@click.argument("defaults_file", type=str)
def _main(expansions_file: str, defaults_file: str):
    try:
        defaults = _load_defaults(defaults_file)
        expansions = _load_expansions(expansions_file)

        # inject defaults into expansions
        for key, value in defaults.items():
            if key not in expansions:
                expansions[key] = value

        for key, value in expansions.items():
            print(f"{key}={quote(value)}; ", end="")

    except Exception as ex:  # pylint: disable=broad-except
        _error(ex)


if __name__ == "__main__":
    _main()  # pylint: disable=no-value-for-parameter

#!/usr/bin/env python3
"""Generate configuration for a build variant."""
import os
import re
from concurrent.futures import ThreadPoolExecutor as Executor
from datetime import datetime, timedelta
from time import perf_counter
from typing import Optional, Any, List, Set

import click
import inject
import structlog
from pydantic import BaseModel
from evergreen import EvergreenApi, RetryingEvergreenApi
from evergreen import Task as EvgTask

from buildscripts.ciconfig.evergreen import EvergreenProjectConfig, parse_evergreen_file, Task, \
    Variant
from buildscripts.task_generation.constants import ACTIVATE_ARCHIVE_DIST_TEST_DEBUG_TASK
from buildscripts.task_generation.evg_config_builder import EvgConfigBuilder
from buildscripts.task_generation.gen_config import GenerationConfiguration
from buildscripts.task_generation.gen_task_validation import GenTaskValidationService
from buildscripts.task_generation.multiversion_util import MultiversionUtilService, \
    SHARDED_MIXED_VERSION_CONFIGS, REPL_MIXED_VERSION_CONFIGS
from buildscripts.task_generation.resmoke_proxy import ResmokeProxyConfig
from buildscripts.task_generation.suite_split import SuiteSplitConfig, SuiteSplitParameters
from buildscripts.task_generation.suite_split_strategies import SplitStrategy, FallbackStrategy, \
    greedy_division, round_robin_fallback
from buildscripts.task_generation.task_types.fuzzer_tasks import FuzzerGenTaskParams
from buildscripts.task_generation.task_types.gentask_options import GenTaskOptions
from buildscripts.task_generation.task_types.multiversion_tasks import MultiversionGenTaskParams
from buildscripts.task_generation.task_types.resmoke_tasks import ResmokeGenTaskParams
from buildscripts.util.cmdutils import enable_logging
from buildscripts.util.fileops import read_yaml_file
from buildscripts.util.taskname import remove_gen_suffix

LOGGER = structlog.get_logger(__name__)

DEFAULT_TEST_SUITE_DIR = os.path.join("buildscripts", "resmokeconfig", "suites")
MAX_WORKERS = 16
LOOKBACK_DURATION_DAYS = 14
MAX_TASK_PRIORITY = 99
GENERATED_CONFIG_DIR = "generated_resmoke_config"
GEN_PARENT_TASK = "generator_tasks"
EXPANSION_RE = re.compile(r"\${(?P<id>[a-zA-Z0-9_]+)(\|(?P<default>.*))?}")


class EvgExpansions(BaseModel):
    """
    Evergreen expansions needed to generate tasks.

    build_id: Build ID being run under.
    build_variant: Build variant being generated.
    gen_task_gran: Granularity of how tasks are being generated.
    is_patch: Whether generation is part of a patch build.
    project: Evergreen project being run under.
    max_test_per_suite: Maximum amount of tests to include in a suite.
    max_sub_suites: Maximum number of sub-suites to generate per task.
    resmoke_repeat_suites: Number of times suites should be repeated.
    revision: Git revision being run against.
    task_name: Name of task running.
    target_resmoke_time: Target time of generated sub-suites.
    task_id: ID of task being run under.
    """

    build_id: str
    build_variant: str
    is_patch: Optional[bool]
    project: str
    max_tests_per_suite: Optional[int] = 100
    max_sub_suites: Optional[int] = 5
    resmoke_repeat_suites: Optional[int] = None
    revision: str
    task_name: str
    target_resmoke_time: Optional[int] = None
    task_id: str

    @classmethod
    def from_yaml_file(cls, path: str) -> "EvgExpansions":
        """
        Read the evergreen expansions from the given YAML file.

        :param path: Path to expansions YAML file.
        :return: Expansions read from file.
        """
        return cls(**read_yaml_file(path))

    def build_suite_split_config(self, start_date: datetime,
                                 end_date: datetime) -> SuiteSplitConfig:
        """
        Get the configuration for splitting suites based on Evergreen expansions.

        :param start_date: Start date for historic stats lookup.
        :param end_date: End date for historic stats lookup.
        :return: Configuration to use for splitting suites.
        """
        return SuiteSplitConfig(
            evg_project=self.project,
            target_resmoke_time=self.target_resmoke_time if self.target_resmoke_time else 60,
            max_sub_suites=self.max_sub_suites,
            max_tests_per_suite=self.max_tests_per_suite,
            start_date=start_date,
            end_date=end_date,
        )

    def build_evg_config_gen_options(self) -> GenTaskOptions:
        """
        Get the configuration for generating tasks from Evergreen expansions.

        :return: Configuration to use for splitting suites.
        """
        return GenTaskOptions(
            create_misc_suite=True,
            is_patch=self.is_patch,
            generated_config_dir=GENERATED_CONFIG_DIR,
            use_default_timeouts=False,
        )

    def config_location(self) -> str:
        """Location where generated configuration is stored."""
        task = remove_gen_suffix(self.task_name)
        return f"{self.build_variant}/{self.revision}/generate_tasks/{task}_gen-{self.build_id}.tgz"


def translate_run_var(run_var: str, build_variant: Variant) -> Any:
    """
    Translate the given "run_var" into an actual value.

    Run_vars can contain evergreen expansions, in which case, the expansion (and possible default
    value) need to be translated into a value we can use.

    :param run_var: Run var to translate.
    :param build_variant: Build variant configuration.
    :return: Value of run_var.
    """
    match = EXPANSION_RE.search(run_var)
    if match:
        value = build_variant.expansion(match.group("id"))
        if value is None:
            value = match.group("default")
        return value
    return run_var


def get_version_configs(is_sharded: bool) -> List[str]:
    """Get the version configurations to use."""
    if is_sharded:
        return SHARDED_MIXED_VERSION_CONFIGS
    return REPL_MIXED_VERSION_CONFIGS


class GenerateBuildVariantOrchestrator:
    """Orchestrator for generating tasks in a build variant."""

    # pylint: disable=too-many-arguments
    @inject.autoparams()
    def __init__(
            self,
            gen_task_validation: GenTaskValidationService,
            gen_task_options: GenTaskOptions,
            evg_project_config: EvergreenProjectConfig,
            evg_expansions: EvgExpansions,
            multiversion_util: MultiversionUtilService,
            evg_api: EvergreenApi,
    ) -> None:
        """
        Initialize the orchestrator.

        :param gen_task_validation: Service to validate task generation.
        :param gen_task_options: Options for how tasks should be generated.
        :param evg_project_config: Configuration for Evergreen Project.
        :param evg_expansions: Evergreen expansions for running task.
        :param multiversion_util: Multiversion utility service.
        :param evg_api: Evergreen API client.
        """
        self.gen_task_validation = gen_task_validation
        self.gen_task_options = gen_task_options
        self.evg_project_config = evg_project_config
        self.evg_expansions = evg_expansions
        self.multiversion_util = multiversion_util
        self.evg_api = evg_api

    def get_build_variant_expansion(self, build_variant_name: str, expansion: str) -> Any:
        """
        Get the value of the given expansion for the specified build variant.

        :param build_variant_name: Build Variant to query.
        :param expansion: Expansion to query.
        :return: Value of given expansion.
        """
        build_variant = self.evg_project_config.get_variant(build_variant_name)
        return build_variant.expansion(expansion)

    def task_def_to_split_params(self, task_def: Task,
                                 build_variant_gen: str) -> SuiteSplitParameters:
        """
        Build parameters for how a task should be split based on its task definition.

        :param task_def: Task definition in evergreen project config.
        :param build_variant_gen: Name of Build Variant being generated.
        :return: Parameters for how task should be split.
        """
        build_variant = self.evg_project_config.get_variant(build_variant_gen)
        task = remove_gen_suffix(task_def.name)
        run_vars = task_def.generate_resmoke_tasks_command.get("vars", {})

        suite = run_vars.get("suite", task)
        return SuiteSplitParameters(
            build_variant=build_variant_gen,
            task_name=task,
            suite_name=suite,
            filename=suite,
            is_asan=build_variant.is_asan_build(),
        )

    def task_def_to_gen_params(self, task_def: Task, build_variant: str) -> ResmokeGenTaskParams:
        """
        Build parameters for how a task should be generated based on its task definition.

        :param task_def: Task definition in evergreen project config.
        :param build_variant: Name of Build Variant being generated.
        :return: Parameters for how task should be generated.
        """
        run_func = task_def.generate_resmoke_tasks_command
        run_vars = run_func["vars"]

        repeat_suites = 1
        if self.evg_expansions.resmoke_repeat_suites:
            repeat_suites = self.evg_expansions.resmoke_repeat_suites

        return ResmokeGenTaskParams(
            use_large_distro=run_vars.get("use_large_distro"),
            require_multiversion=run_vars.get("require_multiversion"),
            repeat_suites=repeat_suites,
            resmoke_args=run_vars.get("resmoke_args"),
            resmoke_jobs_max=run_vars.get("resmoke_jobs_max"),
            large_distro_name=self.get_build_variant_expansion(build_variant, "large_distro_name"),
            config_location=self.evg_expansions.config_location(),
        )

    def task_def_to_mv_gen_params(self, task_def: Task, build_variant: str, is_sharded: bool,
                                  version_config: List[str]) -> MultiversionGenTaskParams:
        """
        Build parameters for how a task should be generated based on its task definition.

        :param task_def: Task definition in evergreen project config.
        :param build_variant: Name of Build Variant being generated.
        :param is_sharded: True if the tasks being generated are for a sharded config.
        :param version_config: List of version configurations to generate.
        :return: Parameters for how task should be generated.
        """
        run_vars = task_def.generate_resmoke_tasks_command["vars"]
        task = remove_gen_suffix(task_def.name)

        return MultiversionGenTaskParams(
            mixed_version_configs=version_config,
            is_sharded=is_sharded,
            resmoke_args=run_vars.get("resmoke_args"),
            parent_task_name=task,
            origin_suite=run_vars.get("suite", task),
            use_large_distro=run_vars.get("use_large_distro"),
            large_distro_name=self.get_build_variant_expansion(build_variant, "large_distro_name"),
            config_location=self.evg_expansions.config_location(),
        )

    def task_def_to_fuzzer_params(
            self, task_def: Task, build_variant: str, is_sharded: Optional[bool] = None,
            version_config: Optional[List[str]] = None) -> FuzzerGenTaskParams:
        """
        Build parameters for how a fuzzer task should be generated based on its task definition.

        :param task_def: Task definition in evergreen project config.
        :param build_variant: Name of Build Variant being generated.
        :param is_sharded: True task if for a sharded configuration.
        :param version_config: List of version configs task is being generated for.
        :return: Parameters for how a fuzzer task should be generated.
        """
        variant = self.evg_project_config.get_variant(build_variant)
        run_vars = task_def.generate_resmoke_tasks_command["vars"]
        run_vars = {k: translate_run_var(v, variant) for k, v in run_vars.items()}

        return FuzzerGenTaskParams(
            task_name=run_vars.get("name"),
            variant=build_variant,
            suite=run_vars.get("suite"),
            num_files=int(run_vars.get("num_files")),
            num_tasks=int(run_vars.get("num_tasks")),
            resmoke_args=run_vars.get("resmoke_args"),
            npm_command=run_vars.get("npm_command", "jstestfuzz"),
            jstestfuzz_vars=run_vars.get("jstestfuzz_vars", ""),
            continue_on_failure=run_vars.get("continue_on_failure"),
            resmoke_jobs_max=run_vars.get("resmoke_jobs_max"),
            should_shuffle=run_vars.get("should_shuffle"),
            timeout_secs=run_vars.get("timeout_secs"),
            require_multiversion=run_vars.get("require_multiversion"),
            use_large_distro=run_vars.get("use_large_distro", False),
            large_distro_name=self.get_build_variant_expansion(build_variant, "large_distro_name"),
            config_location=self.evg_expansions.config_location(),
            is_sharded=is_sharded,
            version_config=version_config,
        )

    def generate(self, task_id: str, build_variant_name: str, output_file: str) -> None:
        """
        Write task configuration for a build variant to disk.

        :param task_id: ID of running task.
        :param build_variant_name: Name of build variant to generate.
        :param output_file: Filename to write generated configuration to.
        """
        if not self.gen_task_validation.should_task_be_generated(task_id):
            LOGGER.info("Not generating configuration due to previous successful generation.")
            return

        builder = EvgConfigBuilder()  # pylint: disable=no-value-for-parameter
        builder = self.generate_build_variant(builder, build_variant_name)

        generated_config = builder.build(output_file)
        generated_config.write_all_to_dir(self.gen_task_options.generated_config_dir)

    # pylint: disable=too-many-locals
    def generate_build_variant(self, builder: EvgConfigBuilder,
                               build_variant_name: str) -> EvgConfigBuilder:
        """
        Generate task configuration for a build variant.

        :param builder: Evergreen configuration builder to use.
        :param build_variant_name: Name of build variant to generate.
        :return: Evergreen configuration builder with build variant configuration.
        """
        LOGGER.info("Generating config", build_variant=build_variant_name)
        start_time = perf_counter()
        task_list = self.evg_project_config.get_variant(build_variant_name).task_names
        tasks_to_hide = set()
        with Executor(max_workers=MAX_WORKERS) as exe:
            jobs = []
            for task_name in task_list:
                task_def = self.evg_project_config.get_task(task_name)
                if task_def.is_generate_resmoke_task:
                    tasks_to_hide.add(task_name)

                    is_sharded = None
                    version_list = None
                    run_vars = task_def.generate_resmoke_tasks_command["vars"]
                    suite = run_vars.get("suite")
                    is_jstestfuzz = run_vars.get("is_jstestfuzz", False)
                    implicit_multiversion = run_vars.get("implicit_multiversion", False)

                    if implicit_multiversion:
                        assert suite is not None
                        is_sharded = self.multiversion_util.is_suite_sharded(suite)
                        version_list = get_version_configs(is_sharded)

                    if is_jstestfuzz:
                        fuzzer_params = self.task_def_to_fuzzer_params(
                            task_def, build_variant_name, is_sharded, version_list)
                        jobs.append(exe.submit(builder.generate_fuzzer, fuzzer_params))
                    else:
                        split_params = self.task_def_to_split_params(task_def, build_variant_name)
                        if implicit_multiversion:
                            gen_params = self.task_def_to_mv_gen_params(
                                task_def, build_variant_name, is_sharded, version_list)
                            jobs.append(
                                exe.submit(builder.add_multiversion_suite, split_params,
                                           gen_params))
                        else:
                            gen_params = self.task_def_to_gen_params(task_def, build_variant_name)
                            jobs.append(
                                exe.submit(builder.generate_suite, split_params, gen_params))

            [j.result() for j in jobs]  # pylint: disable=expression-not-assigned

            builder.generate_archive_dist_test_debug_activator_task(build_variant_name)
            # TODO: SERVER-59102 Check if this still causes a circular dependency on generator_tasks.
            # tasks_to_hide.add(ACTIVATE_ARCHIVE_DIST_TEST_DEBUG_TASK)

        end_time = perf_counter()
        duration = end_time - start_time

        LOGGER.info("Finished BV", build_variant=build_variant_name, duration=duration,
                    task_count=len(tasks_to_hide))

        builder.add_display_task(GEN_PARENT_TASK, tasks_to_hide, build_variant_name)
        self.adjust_gen_tasks_priority(tasks_to_hide)
        return builder

    def adjust_task_priority(self, task: EvgTask) -> None:
        """
        Increase the priority of the given task by 1.

        :param task: Task to increase priority of.
        """
        priority = min(task.priority + 1, MAX_TASK_PRIORITY)
        LOGGER.info("Configure task", task_id=task.task_id, priority=priority)
        self.evg_api.configure_task(task.task_id, priority=priority)

    def adjust_gen_tasks_priority(self, gen_tasks: Set[str]) -> int:
        """
        Increase the priority of any "_gen" tasks.

        We want to minimize the time it tasks for the "_gen" tasks to activate the generated
        sub-tasks. We will do that by increase the priority of the "_gen" tasks.

        :param gen_tasks: Set of "_gen" tasks that were found.
        """
        build = self.evg_api.build_by_id(self.evg_expansions.build_id)
        task_list = build.get_tasks()

        with Executor(max_workers=MAX_WORKERS) as exe:
            jobs = [
                exe.submit(self.adjust_task_priority, task) for task in task_list
                if task.display_name in gen_tasks
            ]

        results = [j.result() for j in jobs]
        return len(results)


@click.command()
@click.option("--expansion-file", type=str, required=True,
              help="Location of expansions file generated by evergreen.")
@click.option("--evg-api-config", type=str, required=True,
              help="Location of evergreen api configuration.")
@click.option("--evg-project-config", type=str, default="etc/evergreen.yml",
              help="Location of Evergreen project configuration.")
@click.option("--output-file", type=str, help="Name of output file to write.")
@click.option("--verbose", is_flag=True, default=False, help="Enable verbose logging.")
def main(expansion_file: str, evg_api_config: str, evg_project_config: str, output_file: str,
         verbose: bool) -> None:
    """
    Generate task configuration for a build variant.
    \f
    :param expansion_file: Location of evergreen expansions for task.
    :param evg_api_config: Location of file containing evergreen API authentication information.
    :param evg_project_config: Location of file containing evergreen project configuration.
    :param output_file: Location to write generated configuration to.
    :param verbose: Should verbose logging be used.
    """
    enable_logging(verbose)

    end_date = datetime.utcnow().replace(microsecond=0)
    start_date = end_date - timedelta(days=LOOKBACK_DURATION_DAYS)

    evg_expansions = EvgExpansions.from_yaml_file(expansion_file)

    # pylint: disable=no-value-for-parameter
    def dependencies(binder: inject.Binder) -> None:
        binder.bind(EvgExpansions, evg_expansions)
        binder.bind(SuiteSplitConfig, evg_expansions.build_suite_split_config(start_date, end_date))
        binder.bind(SplitStrategy, greedy_division)
        binder.bind(FallbackStrategy, round_robin_fallback)
        binder.bind(GenTaskOptions, evg_expansions.build_evg_config_gen_options())
        binder.bind(EvergreenApi, RetryingEvergreenApi.get_api(config_file=evg_api_config))
        binder.bind(EvergreenProjectConfig, parse_evergreen_file(evg_project_config))
        binder.bind(GenerationConfiguration, GenerationConfiguration.from_yaml_file())
        binder.bind(ResmokeProxyConfig,
                    ResmokeProxyConfig(resmoke_suite_dir=DEFAULT_TEST_SUITE_DIR))

    inject.configure(dependencies)

    orchestrator = GenerateBuildVariantOrchestrator()  # pylint: disable=no-value-for-parameter
    start_time = perf_counter()
    orchestrator.generate(evg_expansions.task_id, evg_expansions.build_variant, output_file)
    end_time = perf_counter()

    LOGGER.info("Total runtime", duration=end_time - start_time)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter

#!/usr/bin/env python3
"""Generate multiple powercycle tasks to run in evergreen."""
from collections import namedtuple
from typing import Any, List, Tuple, Set

import click
from shrub.v2 import BuildVariant, FunctionCall, ShrubProject, Task, TaskDependency, ExistingTask
from shrub.v2.command import BuiltInCommand

from buildscripts.util.fileops import write_file
from buildscripts.util.read_config import read_config_file
from buildscripts.util.taskname import name_generated_task

Config = namedtuple("config", [
    "current_task_name",
    "task_names",
    "num_tasks",
    "timeout_params",
    "remote_credentials_vars",
    "set_up_ec2_instance_vars",
    "run_powercycle_vars",
    "build_variant",
    "distro",
])


def make_config(expansions_file: Any) -> Config:
    """Group expansions into config."""
    expansions = read_config_file(expansions_file)
    current_task_name = expansions.get("task_name", "powercycle")
    task_names = expansions.get("task_names", "powercycle_smoke_skip_compile")
    # Avoid duplicated task names
    task_names = {task_name for task_name in task_names.split(" ")}
    num_tasks = int(expansions.get("num_tasks", 10))
    timeout_params = {
        "exec_timeout_secs": int(expansions.get("exec_timeout_secs", 7200)),
        "timeout_secs": int(expansions.get("timeout_secs", 1800)),
    }
    remote_credentials_vars = {
        "private_key_file": "src/powercycle.pem",
        "private_key_remote": "${__project_aws_ssh_key_value}",
    }
    set_up_ec2_instance_vars = {
        "set_up_retry_count": int(expansions.get("set_up_retry_count", 2)),
    }
    run_powercycle_vars = {
        "run_powercycle_args": expansions.get("run_powercycle_args"),
    }
    build_variant = expansions.get("build_variant")
    distro = expansions.get("distro_id")

    return Config(current_task_name, task_names, num_tasks, timeout_params, remote_credentials_vars,
                  set_up_ec2_instance_vars, run_powercycle_vars, build_variant, distro)


def get_setup_commands() -> Tuple[List[FunctionCall], Set[TaskDependency]]:
    """Return setup commands."""
    return [
        FunctionCall("do setup"),
    ], {TaskDependency("archive_dist_test_debug")}


def get_skip_compile_setup_commands() -> Tuple[List[FunctionCall], set]:
    """Return skip compile setup commands."""
    return [
        BuiltInCommand("manifest.load", {}),
        FunctionCall("git get project"),
        FunctionCall("f_expansions_write"),
        FunctionCall("kill processes"),
        FunctionCall("cleanup environment"),
        FunctionCall("set up venv"),
        FunctionCall("upload pip requirements"),
        FunctionCall("configure evergreen api credentials"),
        FunctionCall("get compiled binaries"),
    ], set()


@click.command()
@click.argument("expansions_file", type=str, default="expansions.yml")
@click.argument("output_file", type=str, default="powercycle_tasks.json")
def main(expansions_file: str = "expansions.yml",
         output_file: str = "powercycle_tasks.json") -> None:
    """Generate multiple powercycle tasks to run in evergreen."""

    config = make_config(expansions_file)
    build_variant = BuildVariant(config.build_variant)

    sub_tasks = set()
    for task_name in config.task_names:
        if "skip_compile" in task_name:
            commands, task_dependency = get_skip_compile_setup_commands()
        else:
            commands, task_dependency = get_setup_commands()

        commands.extend([
            FunctionCall("set up remote credentials", config.remote_credentials_vars),
            BuiltInCommand("timeout.update", config.timeout_params),
            FunctionCall("set up EC2 instance", config.set_up_ec2_instance_vars),
            FunctionCall("run powercycle test", config.run_powercycle_vars),
        ])

        sub_tasks.update({
            Task(
                name_generated_task(task_name, index, config.num_tasks, config.build_variant),
                commands, task_dependency)
            for index in range(config.num_tasks)
        })

    build_variant.display_task(
        config.current_task_name.replace("_gen", ""), sub_tasks, distros=[config.distro],
        execution_existing_tasks={ExistingTask(config.current_task_name)})
    shrub_project = ShrubProject.empty()
    shrub_project.add_build_variant(build_variant)

    write_file(output_file, shrub_project.json())


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Determine the number of resmoke jobs to run."""

import argparse
import logging
import platform
import re
import sys
from collections import defaultdict

import psutil
import structlog
import yaml

LOGGER = structlog.get_logger(__name__)

CPU_COUNT = psutil.cpu_count()
PLATFORM_MACHINE = platform.machine()
SYS_PLATFORM = sys.platform

# The following constants define tasks that should override the resmoke jobs in various
# configurations. The factor value will set the max number of resmoke jobs based on the number
# of CPUs a machine has. For example, if the factor is 0.5 and a machine has 8 CPUs, the max resmoke
# jobs would be 4 (8 * 0.5). If the running task has multiple overrides that apply, the lowest
# value will be used.
#
# The task name is specified as a regex. The task name used will be the task executing the test,
# which means if the task has been split to run in sub-tasks, an extra "_0", "_1", ... will be
# appended to the task name. For this reason, most task names should end with a ".*".

# Apply factor for a task based on the build variant it is running on.
VARIANT_TASK_FACTOR_OVERRIDES = {
    "enterprise-rhel-80-64-bit": [{"task": r"logical_session_cache_replication.*", "factor": 0.75}],
    "enterprise-rhel-80-64-bit-inmem": [{"task": "secondary_reads_passthrough", "factor": 0.3}]
}

TASKS_FACTORS = [{"task": r"replica_sets.*", "factor": 0.5}, {"task": r"sharding.*", "factor": 0.5}]

DISTRO_MULTIPLIERS = {"rhel80-large": 1.618}

# Apply factor for a task based on the machine type it is running on.
MACHINE_TASK_FACTOR_OVERRIDES = {
    "aarch64":
        TASKS_FACTORS,
    "ppc64le": [
        dict(task=r"causally_consistent_hedged_reads_jscore_passthrough.*", factor=0.125),
        dict(task=r"causally_consistent_read_concern_snapshot_passthrough.*", factor=0.125),
        dict(task=r"sharded_causally_consistent_read_concern_snapshot_passthrough.*", factor=0.125),
    ],
}

# Apply factor for a task based on the platform it is running on.
PLATFORM_TASK_FACTOR_OVERRIDES = {"win32": TASKS_FACTORS, "cygwin": TASKS_FACTORS}

# Apply factor for a task everywhere it is run.
GLOBAL_TASK_FACTOR_OVERRIDES = {
    r"causally_consistent_hedged_reads_jscore_passthrough.*": 0.25,
    r"logical_session_cache.*_refresh_jscore_passthrough.*": 0.25,
    r"multi_shard_.*multi_stmt_txn_.*jscore_passthrough.*": 0.125,
    r"replica_sets_reconfig_jscore_passthrough.*": 0.25,
    r"replica_sets_reconfig_jscore_stepdown_passthrough.*": 0.25,
    r"replica_sets_reconfig_kill_primary_jscore_passthrough.*": 0.25,
    r"sharded_causally_consistent_jscore_passthrough.*": 0.75,
    r"sharded_collections_jscore_passthrough.*": 0.75,
}


def global_task_factor(task_name, overrides, factor):
    """
    Check for a global task override and return factor.

    :param task_name: Name of task to check for.
    :param overrides: Global override data.
    :param factor: Default factor if there is no override.
    :return: Factor that should be used based on global overrides.
    """
    for task_re, task_factor in overrides.items():
        if re.compile(task_re).match(task_name):
            return task_factor

    return factor


def get_task_factor(task_name, overrides, override_type, factor):
    """Check for task override and return factor."""
    for task_override in overrides.get(override_type, []):
        if re.compile(task_override["task"]).match(task_name):
            return task_override["factor"]
    return factor


def determine_final_multiplier(distro):
    """Determine the final multiplier."""
    multipliers = defaultdict(lambda: 1, DISTRO_MULTIPLIERS)
    return multipliers[distro]


def determine_factor(task_name, variant, distro, factor):
    """Determine the job factor."""
    factors = [
        get_task_factor(task_name, MACHINE_TASK_FACTOR_OVERRIDES, PLATFORM_MACHINE, factor),
        get_task_factor(task_name, PLATFORM_TASK_FACTOR_OVERRIDES, SYS_PLATFORM, factor),
        get_task_factor(task_name, VARIANT_TASK_FACTOR_OVERRIDES, variant, factor),
        global_task_factor(task_name, GLOBAL_TASK_FACTOR_OVERRIDES, factor),
    ]
    return min(factors) * determine_final_multiplier(distro)


def determine_jobs(task_name, variant, distro, jobs_max=0, job_factor=1.0):
    """Determine the resmoke jobs."""
    if jobs_max < 0:
        raise ValueError("The jobs_max must be >= 0.")
    if job_factor <= 0:
        raise ValueError("The job_factor must be > 0.")
    factor = determine_factor(task_name, variant, distro, job_factor)
    jobs_available = int(round(CPU_COUNT * factor))
    if jobs_max == 0:
        return max(1, jobs_available)
    return min(jobs_max, jobs_available)


def output_jobs(jobs, outfile):
    """Output jobs configuration to the specified location."""
    output = {"resmoke_jobs": jobs}

    if outfile:
        with open(outfile, "w") as fh:
            yaml.dump(output, stream=fh, default_flow_style=False)

    yaml.dump(output, stream=sys.stdout, default_flow_style=False)


def main():
    """Determine the resmoke jobs value a task should use in Evergreen."""
    parser = argparse.ArgumentParser(description=main.__doc__)

    parser.add_argument("--taskName", dest="task", required=True, help="Task being executed.")
    parser.add_argument("--buildVariant", dest="variant", required=True,
                        help="Build variant task is being executed on.")
    parser.add_argument("--distro", dest="distro", required=True,
                        help="Distro task is being executed on.")
    parser.add_argument(
        "--jobFactor", dest="jobs_factor", type=float, default=1.0,
        help=("Job factor to use as a mulitplier with the number of CPUs. Defaults"
              " to %(default)s."))
    parser.add_argument(
        "--jobsMax", dest="jobs_max", type=int, default=0,
        help=("Maximum number of jobs to use. Specify 0 to indicate the number of"
              " jobs is determined by --jobFactor and the number of CPUs. Defaults"
              " to %(default)s."))
    parser.add_argument(
        "--outFile", dest="outfile", help=("File to write configuration to. If"
                                           " unspecified no file is generated."))

    options = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    structlog.configure(logger_factory=structlog.stdlib.LoggerFactory())

    LOGGER.info("Finding job count", task=options.task, variant=options.variant,
                platform=PLATFORM_MACHINE, sys=SYS_PLATFORM, cpu_count=CPU_COUNT)

    jobs = determine_jobs(options.task, options.variant, options.distro, options.jobs_max,
                          options.jobs_factor)
    if jobs < CPU_COUNT:
        print("Reducing number of jobs to run from {} to {}".format(CPU_COUNT, jobs))
    output_jobs(jobs, options.outfile)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Script to gather information about how tags are used in evergreen tasks."""

from __future__ import absolute_import
from __future__ import print_function

import argparse
import os
import sys

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buildscripts.ciconfig import evergreen  # pylint: disable=wrong-import-position

DEFAULT_EVERGREEN_FILE = "etc/evergreen.yml"


def parse_command_line():
    """Parse command line options."""
    parser = argparse.ArgumentParser(description=main.__doc__)

    parser.add_argument("--list-tags", action="store_true", default=False,
                        help="List all tags used by tasks in evergreen yml.")
    parser.add_argument("--list-tasks", type=str, help="List all tasks for the given buildvariant.")
    parser.add_argument("--list-variants-and-tasks", action="store_true",
                        help="List all tasks for every buildvariant.")
    parser.add_argument("-t", "--tasks-for-tag", type=str, default=None, action="append",
                        help="List all tasks that use the given tag.")
    parser.add_argument("-x", "--remove-tasks-for-tag-filter", type=str, default=None,
                        action="append", help="Remove tasks tagged with given tag.")
    parser.add_argument("--evergreen-file", type=str, default=DEFAULT_EVERGREEN_FILE,
                        help="Location of evergreen file.")

    options = parser.parse_args()

    return options


def get_all_task_tags(evg_config):
    """Get all task tags used in the evergreen configuration."""
    if evg_config.tasks:
        return sorted(set.union(*[task.tags for task in evg_config.tasks]))
    return set()


def list_all_tags(evg_config):
    """
    Print all task tags found in the evergreen configuration.

    :param evg_config: evergreen configuration.
    """
    all_tags = get_all_task_tags(evg_config)
    for tag in all_tags:
        print(tag)


def get_all_tasks(evg_config, build_variant):
    """
    Get all tasks for the given build variant.

    :param evg_config: Evergreen configuration.
    :param build_variant: Build Variant to get tasks for.
    :return: List of task name belonging to given build variant.
    """
    bv = evg_config.get_variant(build_variant)
    return bv.task_names


def list_all_tasks(evg_config, build_variant):
    """
    Print all tasks for the given build variant.

    :param evg_config: Evergreen configuration.
    :param build_variant: Build Variant to get tasks for.
    """
    tasks = get_all_tasks(evg_config, build_variant)
    for task in tasks:
        print(task)


def list_all_variants_and_tasks(evg_config):
    """
    Print all tasks for every build variant.

    :param evg_config: Evergreen configuration.
    """
    for variant in evg_config.variant_names:
        tasks = get_all_tasks(evg_config, variant)
        for task in tasks:
            print("%s | %s" % (variant, task))


def is_task_tagged(task, tags, filters):
    """
    Determine if given task match tag query.

    :param task: Task to check.
    :param tags: List of tags that should belong to the task.
    :param filters: List of tags that should not belong to the task.
    :return: True if task matches the query.
    """
    if all(tag in task.tags for tag in tags):
        if not filters or not any(tag in task.tags for tag in filters):
            return True

    return False


def get_tasks_with_tag(evg_config, tags, filters):
    """
    Get all tasks marked with the given tag in the evergreen configuration.

    :param evg_config: evergreen configuration.
    :param tags: tag to search for.
    :param filters: lst of tags to filter out.
    :return: list of tasks marked with the given tag.
    """
    return sorted([task.name for task in evg_config.tasks if is_task_tagged(task, tags, filters)])


def list_tasks_with_tag(evg_config, tags, filters):
    """
    Print all tasks that are marked with the given tag.

    :param evg_config: evergreen configuration.
    :param tags: list of tags to search for.
    :param filters: list of tags to filter out.
    """
    task_list = get_tasks_with_tag(evg_config, tags, filters)
    for task in task_list:
        print(task)


def main():
    """Gather information about how tags are used in evergreen tasks."""
    options = parse_command_line()

    evg_config = evergreen.parse_evergreen_file(options.evergreen_file)

    if options.list_tags:
        list_all_tags(evg_config)

    if options.list_variants_and_tasks:
        list_all_variants_and_tasks(evg_config)

    if options.list_tasks:
        list_all_tasks(evg_config, options.list_tasks)

    if options.tasks_for_tag:
        list_tasks_with_tag(evg_config, options.tasks_for_tag, options.remove_tasks_for_tag_filter)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Determine the timeout value a task should use in evergreen."""

import argparse
import math
import sys
from datetime import timedelta
from typing import Optional

import yaml

COMMIT_QUEUE_ALIAS = "__commit_queue"
UNITTEST_TASK = "run_unittests"

COMMIT_QUEUE_TIMEOUT = timedelta(minutes=40)
DEFAULT_REQUIRED_BUILD_TIMEOUT = timedelta(hours=1, minutes=20)
DEFAULT_NON_REQUIRED_BUILD_TIMEOUT = timedelta(hours=2)
# 2x the longest "run tests" phase for unittests as of c9bf1dbc9cc46e497b2f12b2d6685ef7348b0726,
# which is 5 mins 47 secs, excluding outliers below
UNITTESTS_TIMEOUT = timedelta(minutes=12)

SPECIFIC_TASK_OVERRIDES = {
    "linux-64-debug": {"auth": timedelta(minutes=60)},
    "enterprise-windows-all-feature-flags-suggested": {
        "replica_sets_jscore_passthrough": timedelta(hours=3),
        "replica_sets_update_v1_oplog_jscore_passthrough": timedelta(hours=2, minutes=30),
    },
    "enterprise-windows-suggested": {
        "replica_sets_jscore_passthrough": timedelta(hours=3),
        "replica_sets_update_v1_oplog_jscore_passthrough": timedelta(hours=2, minutes=30),
    },
    "enterprise-windows-inmem": {
        "replica_sets_jscore_passthrough": timedelta(hours=2, minutes=30),
    },
    "enterprise-windows": {"replica_sets_jscore_passthrough": timedelta(hours=3), },
    "windows-debug-suggested": {
        "replica_sets_initsync_jscore_passthrough": timedelta(hours=2, minutes=30),
        "replica_sets_jscore_passthrough": timedelta(hours=2, minutes=30),
        "replica_sets_update_v1_oplog_jscore_passthrough": timedelta(hours=2, minutes=30),
    },
    "windows": {
        "replica_sets": timedelta(hours=3),
        "replica_sets_jscore_passthrough": timedelta(hours=2, minutes=30),
    },
    "ubuntu1804-debug-suggested": {"replica_sets_jscore_passthrough": timedelta(hours=3), },
    "enterprise-rhel-80-64-bit-coverage": {
        "replica_sets_jscore_passthrough": timedelta(hours=2, minutes=30),
    },
    "macos": {"replica_sets_jscore_passthrough": timedelta(hours=2, minutes=30), },
    "enterprise-macos": {"replica_sets_jscore_passthrough": timedelta(hours=2, minutes=30), },

    # unittests outliers
    # repeated execution runs a suite 10 times
    "linux-64-repeated-execution": {UNITTEST_TASK: 10 * UNITTESTS_TIMEOUT},
    # some of the a/ub/t san variants need a little extra time
    "enterprise-ubuntu2004-debug-tsan": {UNITTEST_TASK: 2 * UNITTESTS_TIMEOUT},
    "ubuntu1804-asan": {UNITTEST_TASK: 2 * UNITTESTS_TIMEOUT},
    "ubuntu1804-ubsan": {UNITTEST_TASK: 2 * UNITTESTS_TIMEOUT},
    "ubuntu1804-debug-asan": {UNITTEST_TASK: 2 * UNITTESTS_TIMEOUT},
    "ubuntu1804-debug-aubsan-lite": {UNITTEST_TASK: 2 * UNITTESTS_TIMEOUT},
    "ubuntu1804-debug-ubsan": {UNITTEST_TASK: 2 * UNITTESTS_TIMEOUT},
}


def _is_required_build_variant(build_variant: str) -> bool:
    """
    Determine if the given build variants is a required build variant.

    :param build_variant: Name of build variant to check.
    :return: True if the given build variant is required.
    """
    return build_variant.endswith("-required")


def _has_override(variant: str, task_name: str) -> bool:
    """
    Determine if the given task has a timeout override.

    :param variant: Build Variant task is running on.
    :param task_name: Task to check.
    :return: True if override exists for task.
    """
    return variant in SPECIFIC_TASK_OVERRIDES and task_name in SPECIFIC_TASK_OVERRIDES[variant]


def determine_timeout(task_name: str, variant: str, idle_timeout: Optional[timedelta] = None,
                      exec_timeout: Optional[timedelta] = None, evg_alias: str = '') -> timedelta:
    """
    Determine what exec timeout should be used.

    :param task_name: Name of task being run.
    :param variant: Name of build variant being run.
    :param idle_timeout: Idle timeout if specified.
    :param exec_timeout: Override to use for exec_timeout or 0 if no override.
    :param evg_alias: Evergreen alias running the task.
    :return: Exec timeout to use for running task.
    """
    determined_timeout = DEFAULT_NON_REQUIRED_BUILD_TIMEOUT

    if exec_timeout and exec_timeout.total_seconds() != 0:
        determined_timeout = exec_timeout

    elif task_name == UNITTEST_TASK and not _has_override(variant, task_name):
        determined_timeout = UNITTESTS_TIMEOUT

    elif evg_alias == COMMIT_QUEUE_ALIAS:
        determined_timeout = COMMIT_QUEUE_TIMEOUT

    elif _has_override(variant, task_name):
        determined_timeout = SPECIFIC_TASK_OVERRIDES[variant][task_name]

    elif _is_required_build_variant(variant):
        determined_timeout = DEFAULT_REQUIRED_BUILD_TIMEOUT

    # The timeout needs to be at least as large as the idle timeout.
    if idle_timeout and determined_timeout.total_seconds() < idle_timeout.total_seconds():
        return idle_timeout

    return determined_timeout


def output_timeout(task_timeout: timedelta, output_file: Optional[str]) -> None:
    """
    Output timeout configuration to the specified location.

    :param task_timeout: Timeout to output.
    :param output_file: Location of output file to write.
    """
    output = {
        "exec_timeout_secs": math.ceil(task_timeout.total_seconds()),
    }

    if output_file:
        with open(output_file, "w") as outfile:
            yaml.dump(output, stream=outfile, default_flow_style=False)

    yaml.dump(output, stream=sys.stdout, default_flow_style=False)


def main():
    """Determine the timeout value a task should use in evergreen."""
    parser = argparse.ArgumentParser(description=main.__doc__)

    parser.add_argument("--task-name", dest="task", required=True, help="Task being executed.")
    parser.add_argument("--build-variant", dest="variant", required=True,
                        help="Build variant task is being executed on.")
    parser.add_argument("--evg-alias", dest="evg_alias", required=True,
                        help="Evergreen alias used to trigger build.")
    parser.add_argument("--timeout", dest="timeout", type=int, help="Timeout to use (in sec).")
    parser.add_argument("--exec-timeout", dest="exec_timeout", type=int,
                        help="Exec timeout ot use (in sec).")
    parser.add_argument("--out-file", dest="outfile", help="File to write configuration to.")

    options = parser.parse_args()

    timeout_override = timedelta(seconds=options.timeout) if options.timeout else None
    exec_timeout_override = timedelta(
        seconds=options.exec_timeout) if options.exec_timeout else None
    task_timeout = determine_timeout(options.task, options.variant, timeout_override,
                                     exec_timeout_override, options.evg_alias)
    output_timeout(task_timeout, options.outfile)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Feature flag tags check.

Check that on changing feature flag from disabled to enabled by default in all js tests that
had that feature flag in tags there is a tag that requires the latest FCV.
"""

import argparse
import os
import subprocess
import sys

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pylint: disable=wrong-import-position
from buildscripts.resmokelib import selector
from buildscripts.resmokelib.multiversionconstants import REQUIRES_FCV_TAG_LATEST
from buildscripts.resmokelib.utils import jscomment


def _run_git_cmd(cmd_args, cwd=None, silent=True):
    """Run git command."""
    run_args = {}
    if cwd:
        run_args["cwd"] = cwd
    if silent:
        run_args["stdout"] = subprocess.DEVNULL
        run_args["stderr"] = subprocess.DEVNULL
    subprocess.run(["git"] + cmd_args, **run_args, check=False)


def get_tests_with_feature_flag_tags(feature_flags, ent_path):
    """Get the list of tests with feature flag tag."""
    selector_config = {
        "roots": ["jstests/**/*.js", f"{ent_path}/jstests/**/*.js"],
        "include_with_any_tags": feature_flags,
    }
    tests, _ = selector.filter_tests("js_test", selector_config)
    return tests


def get_tests_missing_fcv_tag(tests):
    """Get the list of tests missing requires FCV tag."""
    found_tests = []
    for test in tests:
        try:
            test_tags = jscomment.get_tags(test)
        except FileNotFoundError:
            continue
        else:
            if REQUIRES_FCV_TAG_LATEST not in test_tags:
                found_tests.append(test)
    return found_tests


def main(diff_file, ent_path):
    """Run the main function."""
    with open("base_all_feature_flags.txt", "r") as fh:
        base_feature_flags = fh.read().split()
    with open("patch_all_feature_flags.txt", "r") as fh:
        patch_feature_flags = fh.read().split()
    enabled_feature_flags = [flag for flag in base_feature_flags if flag not in patch_feature_flags]

    if not enabled_feature_flags:
        print(
            "No feature flags were enabled by default in this patch/commit; skipping feature flag checks"
        )
        sys.exit(0)

    tests_with_feature_flag_tag = get_tests_with_feature_flag_tags(enabled_feature_flags, ent_path)

    _run_git_cmd(["apply", diff_file])
    _run_git_cmd(["apply", diff_file], cwd=ent_path)
    tests_missing_fcv_tag = get_tests_missing_fcv_tag(tests_with_feature_flag_tag)

    if tests_missing_fcv_tag:
        print(f"Found tests missing `{REQUIRES_FCV_TAG_LATEST}` tag:\n" +
              "\n".join(tests_missing_fcv_tag))
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--diff-file-name", type=str,
                        help="Name of the file containing the git diff")
    parser.add_argument("--enterprise-path", type=str, help="Path to the enterprise module")
    args = parser.parse_args()
    main(args.diff_file_name, args.enterprise_path)

#!/usr/bin/env python3
"""
Generate the compile expansions file used by Evergreen as part of the push/release process.

Invoke by specifying an output file.
$ python generate_compile_expansions.py --out compile_expansions.yml
"""

import argparse
import json
import os
import re
import sys
import shlex
import yaml

VERSION_JSON = "version.json"


def generate_expansions():
    """Entry point for the script.

    This calls functions to generate version and scons cache expansions and
    writes them to a file.
    """
    args = parse_args()
    expansions = {}
    expansions.update(generate_version_expansions())
    expansions.update(generate_scons_cache_expansions())

    with open(args.out, "w") as out:
        print("saving compile expansions to {0}: ({1})".format(args.out, expansions))
        yaml.safe_dump(expansions, out, default_flow_style=False)


def parse_args():
    """Parse program arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    return parser.parse_args()


def generate_version_expansions():
    """Generate expansions from a version.json file if given, or $MONGO_VERSION."""
    expansions = {}

    if os.path.exists(VERSION_JSON):
        with open(VERSION_JSON, "r") as fh:
            data = fh.read()
            version_data = json.loads(data)
        version_line = version_data['version']
        version_parts = match_verstr(version_line)
        if not version_parts:
            raise ValueError("Unable to parse version.json")
    else:
        if not os.getenv("MONGO_VERSION"):
            raise Exception("$MONGO_VERSION not set and no version.json provided")
        version_line = os.getenv("MONGO_VERSION").lstrip("r")
        version_parts = match_verstr(version_line)
        if not version_parts:
            raise ValueError("Unable to parse version from stdin and no version.json provided")

    if version_parts[0]:
        expansions["suffix"] = "latest"
        expansions["src_suffix"] = "latest"
        expansions["is_release"] = "false"
    else:
        expansions["suffix"] = version_line
        expansions["src_suffix"] = "r{0}".format(version_line)
        expansions["is_release"] = "true"
    expansions["version"] = version_line

    return expansions


def generate_scons_cache_expansions():
    """Generate scons cache expansions from some files and environment variables."""
    expansions = {}
    if sys.platform.startswith("win"):
        system_id_path = r"c:\mongodb-build-system-id"
        default_cache_path_base = r"z:\data\scons-cache"
    else:
        system_id_path = "/etc/mongodb-build-system-id"
        default_cache_path_base = "/data/scons-cache"

    if os.path.isfile(system_id_path):
        with open(system_id_path, "r") as fh:
            default_cache_path = os.path.join(default_cache_path_base, fh.readline().strip())

            expansions["scons_cache_path"] = default_cache_path

            scons_cache_mode = os.getenv("SCONS_CACHE_MODE")

            if scons_cache_mode in (None, ""):
                scons_cache_mode = "nolinked"

            if os.getenv("USE_SCONS_CACHE") not in (None, False, "false", ""):
                expansions[
                    "scons_cache_args"] = "--cache={0} --cache-signature-mode=validate --cache-dir={1}".format(
                        scons_cache_mode, shlex.quote(default_cache_path))
    return expansions


def match_verstr(verstr):
    """Match a version string and capture the "extra" part.

    If the version is a release like "2.3.4" or "2.3.4-rc0", this will return
    None. If the version is a pre-release like "2.3.4-325-githash" or
    "2.3.4-pre-", this will return "-pre-" or "-325-githash" If the version
    begins with the letter 'r', it will also match, e.g. r2.3.4, r2.3.4-rc0,
    r2.3.4-git234, r2.3.4-rc0-234-githash If the version is invalid (i.e.
    doesn't start with "2.3.4" or "2.3.4-rc0", this will return False.
    """
    res = re.match(r'^r?(?:\d+\.\d+\.\d+(?:-rc\d+|-alpha\d+)?)(-.*)?', verstr)
    if not res:
        return False
    return res.groups()


if __name__ == "__main__":
    generate_expansions()

#!/usr/bin/env python3
"""
Generate the compile expansions file used by Evergreen as part of the push/release process.

Invoke by specifying an output file.
$ python generate_compile_expansions.py --out compile_expansions.yml
"""

import argparse
import json
import os
import re
import sys
import shlex
import yaml

VERSION_JSON = "version.json"


def generate_expansions():
    """Entry point for the script.

    This calls functions to generate version and scons cache expansions and
    writes them to a file.
    """
    args = parse_args()
    expansions = {}
    expansions.update(generate_version_expansions())
    expansions.update(generate_scons_cache_expansions())

    with open(args.out, "w") as out:
        print("saving compile expansions to {0}: ({1})".format(args.out, expansions))
        yaml.safe_dump(expansions, out, default_flow_style=False)


def parse_args():
    """Parse program arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    return parser.parse_args()


def generate_version_expansions():
    """Generate expansions from a version.json file if given, or $MONGO_VERSION."""
    expansions = {}

    if os.path.exists(VERSION_JSON):
        with open(VERSION_JSON, "r") as fh:
            data = fh.read()
            version_data = json.loads(data)
        version_line = version_data['version']
        version_parts = match_verstr(version_line)
        if not version_parts:
            raise ValueError("Unable to parse version.json")
    else:
        if not os.getenv("MONGO_VERSION"):
            raise Exception("$MONGO_VERSION not set and no version.json provided")
        version_line = os.getenv("MONGO_VERSION").lstrip("r")
        version_parts = match_verstr(version_line)
        if not version_parts:
            raise ValueError("Unable to parse version from stdin and no version.json provided")

    if version_parts[0]:
        expansions["suffix"] = "latest"
        expansions["src_suffix"] = "latest"
        expansions["is_release"] = "false"
    else:
        expansions["suffix"] = version_line
        expansions["src_suffix"] = "r{0}".format(version_line)
        expansions["is_release"] = "true"
    expansions["version"] = version_line

    return expansions


def generate_scons_cache_expansions():
    """Generate scons cache expansions from some files and environment variables."""
    expansions = {}

    # Get the scons cache mode
    scons_cache_mode = os.getenv("SCONS_CACHE_MODE", "nolinked")

    # Get the host uuid
    if sys.platform.startswith("win"):
        system_id_path = r"c:\mongodb-build-system-id"
    else:
        system_id_path = "/etc/mongodb-build-system-id"

    if os.path.isfile(system_id_path):
        with open(system_id_path, "r") as fh:
            system_uuid = fh.readline().strip()

    # Set the scons shared cache setting

    # Global shared cache using EFS
    if os.getenv("SCONS_CACHE_SCOPE") == "shared":
        if sys.platform.startswith("win"):
            shared_mount_root = 'X:\\'
        else:
            shared_mount_root = '/efs'
        default_cache_path = os.path.join(shared_mount_root, system_uuid, "scons-cache")
        expansions["scons_cache_path"] = default_cache_path
        expansions[
            "scons_cache_args"] = "--cache={0} --cache-signature-mode=validate --cache-dir={1}".format(
                scons_cache_mode, shlex.quote(default_cache_path))

    # Local shared cache - host-based
    elif os.getenv("SCONS_CACHE_SCOPE") == "local":

        if sys.platform.startswith("win"):
            default_cache_path_base = r"z:\data\scons-cache"
        else:
            default_cache_path_base = "/data/scons-cache"

        default_cache_path = os.path.join(default_cache_path_base, system_uuid)
        expansions["scons_cache_path"] = default_cache_path
        expansions[
            "scons_cache_args"] = "--cache={0} --cache-signature-mode=validate --cache-dir={1}".format(
                scons_cache_mode, shlex.quote(default_cache_path))
    # No cache
    else:
        # Anything else is 'none'
        print("No cache used")

    return expansions


def match_verstr(verstr):
    """Match a version string and capture the "extra" part.

    If the version is a release like "2.3.4" or "2.3.4-rc0", this will return
    None. If the version is a pre-release like "2.3.4-325-githash" or
    "2.3.4-pre-", this will return "-pre-" or "-325-githash" If the version
    begins with the letter 'r', it will also match, e.g. r2.3.4, r2.3.4-rc0,
    r2.3.4-git234, r2.3.4-rc0-234-githash If the version is invalid (i.e.
    doesn't start with "2.3.4" or "2.3.4-rc0", this will return False.
    """
    res = re.match(r'^r?(?:\d+\.\d+\.\d+(?:-rc\d+|-alpha\d+)?)(-.*)?', verstr)
    if not res:
        return False
    return res.groups()


if __name__ == "__main__":
    generate_expansions()

#!/usr/bin/env python3
"""Stub file pointing users to the new invocation."""

if __name__ == "__main__":
    print("Hello! It seems you've executed 'buildscripts/hang_analyzer.py'. We have recently\n"
          "repackaged the hang analyzer as a subcommand of resmoke. It can now be invoked as\n"
          "'./buildscripts/resmoke.py hang-analyzer' with all of the same arguments as before.")

#!/bin/bash

# Create a git hook driver in this repo. The hook will run all
# excutable scripts in the directory ~/.githooks/<repo name>/[HOOK_TRIGGER]/
# where HOOK_TRIGGER can be any one of the following:
# https://git-scm.com/docs/githooks#_hooks
# If you add a new type of HOOK_TRIGGER, this script needs to be run again.

# Find out the repo name and where the .git directory is for this repo
origin="$(git config remote.origin.url)"
repo="$(basename -s .git $origin)"
tld="$(git rev-parse --show-toplevel)"

# Location for the hooks.
# WARNING: if you change this you'll need to change the value of the
# "hooks_dir" variable in the heredoc below as well
hooks_dir="$HOME/.githooks/$repo"

tmp_hook=$(mktemp)

usage() {
    echo "Usage: $(basename $0) [-f|-h]"
    echo
    echo "  git hooks in $tld/.git/hooks that run scripts in $hooks_dir"
    echo "  -f force overwriting existing hooks"
    echo "  -h print this help"
    echo
    return
}

if [ ! -d "$hooks_dir" ]; then
  # Bail out if the githooks directory doesn't exist.
  echo "Place your scripts in the directory $hooks_dir/[HOOK_TRIGGER]/ where [HOOK_TRIGGER] is the"
  echo "name of the git hook that you want your script to run under, like 'pre-push' or 'pre-commit'"
  echo
  echo "See the full list of git hooks here: https://git-scm.com/docs/githooks#_hooks"
  echo
  echo "Once you have placed your scripts, re-run this file."
  echo
  return
fi

# --- Command line options ---
force=0
while getopts fh opt_arg; do
    case $opt_arg in
        f) force=1 ;;
        *) usage   ;;
    esac
done
shift $(expr $OPTIND - 1)
# ----------------------------

set -e

cat > $tmp_hook <<'EOF'
#!/bin/bash

# set GITHOOKS_QUIET to anything to anything to make this script quiet
quiet=${GITHOOKS_QUIET:-""}
[ -z "$quiet" ] && echo -e "[INFO] Starting up hook runner ..."

origin="$(git config remote.origin.url)"
repo="$(basename -s .git $origin)"
this_script=$(basename "$0")

hooks_dir=$HOME/.githooks/$repo/$this_script
if [ ! -d "$hooks_dir" ]; then
    echo "[WARNING] Hooks directory doesn't exist: $hooks_dir. Running $this_script anyway"
    sleep 2
    exit 0
fi

# Only run if the remote URL matches the MongoDB repo.
url=$2
if [ "$this_script" == "pre-push" ] && ! [[ $url =~ (mongodb|10gen)/ ]]; then
    echo "Skipping pre-push hook for non-MongoDB remote URL: $url"
    exit 0
fi

num_hooks_run=0
all_hooks="$(/bin/ls -1 $hooks_dir 2>/dev/null)"

for hook_name in $(echo $all_hooks) ; do
    hook_path=$hooks_dir/$hook_name

    if [ -e "$hook_path" ]; then
        [ -z "$quiet" ] && echo -e "[INFO] Running hook: $hook_path"
        $hook_path "$@"
        
        hook_status=$?
        num_hooks_run=$((num_hooks_run + 1))
        
        if [ $hook_status -ne 0 ]; then
            echo "[ERROR] Hook $hook_path returned non-zero status: $hook_status"
            exit $hook_status
        else
            [ -z "$quiet" ] && echo "[INFO] Done."
        fi
    fi
done

if [ $num_hooks_run -eq 0 ]; then
    echo "[WARNING] Couldn't find any hooks to run in $hooks_dir. Running $this_script anyway"
    sleep 2
fi

exit 0
EOF

hook_types=($(ls -d $hooks_dir/*))

for cur_hook_full_path in "${hook_types[@]}"
do
    cur_hook_type=$(basename $cur_hook_full_path)
    cur_hook_path="$tld/.git/hooks/$cur_hook_type"

    echo "Installing $cur_hook_type hook in $cur_hook_path ..."

    # If there's already a hook installed, bail out. We don't want to overwrite it.
    # (unless -f is passed in the command line)
    if [ -e "$cur_hook_path" -a $force -eq 0 ]; then
        echo "[ERROR] Found an existing $cur_hook_type hook: $cur_hook_path"
        exit 1
    fi

    cp $tmp_hook $cur_hook_path
    chmod +x $cur_hook_path
done

rm $tmp_hook
echo "Done."

"""Module to access a JIRA server."""

import jira


class JiraClient(object):
    """A client for JIRA."""

    CLOSE_TRANSITION_NAME = "Close Issue"
    RESOLVE_TRANSITION_NAME = "Resolve Issue"
    FIXED_RESOLUTION_NAME = "Fixed"
    WONT_FIX_RESOLUTION_NAME = "Won't Fix"

    def __init__(  # pylint: disable=too-many-arguments
            self, server, username=None, password=None, access_token=None, access_token_secret=None,
            consumer_key=None, key_cert=None):
        """Initialize the JiraClient with the server URL and user credentials."""
        opts = {"server": server, "verify": True}
        basic_auth = None
        oauth_dict = None
        if access_token and access_token_secret and consumer_key and key_cert:
            oauth_dict = {
                "access_token": access_token, "access_token_secret": access_token_secret,
                "consumer_key": consumer_key, "key_cert": key_cert
            }
        elif username and password:
            basic_auth = (username, password)
        else:
            raise TypeError("Must specify Basic Auth (using arguments username & password)"
                            " or OAuth (using arguments access_token, access_token_secret,"
                            " consumer_key & key_cert_file) credentials")
        self._jira = jira.JIRA(options=opts, basic_auth=basic_auth, oauth=oauth_dict, validate=True)

        self._transitions = {}
        self._resolutions = {}

    def create_issue(self, project, summary, description, labels=None):
        """Create an issue."""
        fields = {
            "project": project, "issuetype": {"name": "Task"}, "summary": summary,
            "description": description
        }
        new_issue = self._jira.create_issue(fields=fields)
        if labels:
            new_issue.update(fields={"labels": labels})
        return new_issue.key

    def close_issue(self, issue_key, resolution, fix_version=None):
        """Close an issue."""
        issue = self._jira.issue(issue_key)
        resolution_id = self._get_resolution_id(resolution)
        if resolution_id is None:
            raise ValueError("No resolution found for '{0}'. Leaving issue '{1}' open.".format(
                resolution, issue_key))
        close_transition_id = self._get_transition_id(issue, JiraClient.CLOSE_TRANSITION_NAME)
        if close_transition_id is None:
            raise ValueError("No transition found for '{0}'. Leaving issue '{1}' open.".format(
                JiraClient.CLOSE_TRANSITION_NAME, issue_key))
        fields = {"resolution": {"id": resolution_id}}
        if fix_version:
            fields["fixVersions"] = [{"name": fix_version}]
        self._jira.transition_issue(issue, close_transition_id, fields=fields)

    def _get_transition_id(self, issue, name):
        project_key = issue.fields.project.key
        project_transitions = self._transitions.setdefault(project_key, {})

        transition_id = project_transitions.get(name)
        if transition_id:
            return transition_id
        transitions = self._jira.transitions(issue)
        for transition in transitions:
            project_transitions[transition["name"]] = transition["id"]
            if transition["name"] == name:
                transition_id = transition["id"]
        return transition_id

    def _get_resolution_id(self, name):
        resolution_id = self._resolutions.get(name)
        if resolution_id is not None:
            return resolution_id
        resolutions = self._jira.resolutions()
        for resolution in resolutions:
            self._resolutions[resolution.name] = resolution.id
            if resolution.name == name:
                resolution_id = resolution.id
        return resolution_id

#!/usr/bin/env python3
"""Helper script for constructing an archive (zip or tar) from a list of files.

The output format (tar, tgz, zip) is determined from the file name, unless the user specifies
--format on the command line.

This script simplifies the specification of filename transformations, so that, e.g.,
src/mongo/foo.cpp and build/linux2/normal/buildinfo.cpp can get put into the same
directory in the archive, perhaps mongodb-2.0.2/src/mongo.

Usage:

make_archive.py -o <output-file> [--format (tar|tgz|zip)] \\
    [--transform match1=replacement1 [--transform match2=replacement2 [...]]] \\
    <input file 1> [...]

If the input file names start with "@", the file is expected to contain a list of
whitespace-separated file names to include in the archive.  This helps get around the Windows
command line length limit.

Transformations are processed in command-line order and are short-circuiting.  So, if a file matches
match1, it is never compared against match2 or later.  Matches are just python startswith()
comparisons.

For a detailed usage example, see src/SConscript.client or src/mongo/SConscript.
"""

import optparse
import os
import sys
import shlex
import shutil
import zipfile
import tempfile
from subprocess import (Popen, PIPE, STDOUT)


def main(argv):
    """Execute Main program."""
    args = []
    for arg in argv[1:]:
        if arg.startswith("@"):
            file_name = arg[1:]
            f_handle = open(file_name, "r")
            args.extend(s1.strip('"') for s1 in shlex.split(f_handle.readline(), posix=False))
            f_handle.close()
        else:
            args.append(arg)

    opts = parse_options(args)
    if opts.archive_format in ('tar', 'tgz'):
        make_tar_archive(opts)
    elif opts.archive_format == 'zip':
        make_zip_archive(opts)
    else:
        raise ValueError('Unsupported archive format "%s"' % opts.archive_format)


def delete_directory(directory):
    """Recursively deletes a directory and its contents."""
    try:
        shutil.rmtree(directory)
    except Exception:  # pylint: disable=broad-except
        pass


def make_tar_archive(opts):
    """Generate tar archive.

    Given the parsed options, generates the 'opt.output_filename'
    tarball containing all the files in 'opt.input_filename' renamed
    according to the mappings in 'opts.transformations'.

    e.g. for an input file named "a/mongo/build/DISTSRC", and an
    existing transformation {"a/mongo/build": "release"}, the input
    file will be written to the tarball as "release/DISTSRC"

    All files to be compressed are copied into new directories as
    required by 'opts.transformations'. Once the tarball has been
    created, all temporary directory structures created for the
    purposes of compressing, are removed.
    """
    tar_options = "cvf"
    if opts.archive_format == 'tgz':
        tar_options += "z"

    # clean and create a temp directory to copy files to
    enclosing_archive_directory = tempfile.mkdtemp(prefix='archive_', dir=os.path.abspath('build'))
    output_tarfile = os.path.join(os.getcwd(), opts.output_filename)

    tar_command = ["tar", tar_options, output_tarfile]

    for input_filename in opts.input_filenames:
        preferred_filename = get_preferred_filename(input_filename, opts.transformations)
        temp_file_location = os.path.join(enclosing_archive_directory, preferred_filename)
        enclosing_file_directory = os.path.dirname(temp_file_location)
        if not os.path.exists(enclosing_file_directory):
            os.makedirs(enclosing_file_directory)
        print("copying %s => %s" % (input_filename, temp_file_location))
        if os.path.isdir(input_filename):
            shutil.copytree(input_filename, temp_file_location)
        else:
            shutil.copy2(input_filename, temp_file_location)
        tar_command.append(preferred_filename)

    print(" ".join(tar_command))
    # execute the full tar command
    run_directory = os.path.join(os.getcwd(), enclosing_archive_directory)
    proc = Popen(tar_command, stdout=PIPE, stderr=STDOUT, bufsize=0, cwd=run_directory)
    proc.wait()

    # delete temp directory
    delete_directory(enclosing_archive_directory)


def make_zip_archive(opts):
    """Generate the zip archive.

    Given the parsed options, generates the 'opt.output_filename'
    zipfile containing all the files in 'opt.input_filename' renamed
    according to the mappings in 'opts.transformations'.

    All files in 'opt.output_filename' are renamed before being
    written into the zipfile.
    """
    archive = open_zip_archive_for_write(opts.output_filename)
    try:
        for input_filename in opts.input_filenames:
            archive.add(input_filename, arcname=get_preferred_filename(
                input_filename, opts.transformations))
    finally:
        archive.close()


def parse_options(args):
    """Parse program options."""
    parser = optparse.OptionParser()
    parser.add_option('-o', dest='output_filename', default=None,
                      help='Name of the archive to output.', metavar='FILE')
    parser.add_option(
        '--format', dest='archive_format', default=None, choices=('zip', 'tar', 'tgz'),
        help=('Format of archive to create.  '
              'If omitted, use the suffix of the output filename to decide.'))
    parser.add_option('--transform', action='append', dest='transformations', default=[])

    (opts, input_filenames) = parser.parse_args(args)
    opts.input_filenames = []

    for input_filename in input_filenames:
        if input_filename.startswith('@'):
            opts.input_filenames.extend(open(input_filename[1:], 'r').read().split())
        else:
            opts.input_filenames.append(input_filename)

    if opts.output_filename is None:
        parser.error('-o switch is required')

    if opts.archive_format is None:
        if opts.output_filename.endswith('.zip'):
            opts.archive_format = 'zip'
        elif opts.output_filename.endswith('tar.gz') or opts.output_filename.endswith('.tgz'):
            opts.archive_format = 'tgz'
        elif opts.output_filename.endswith('.tar'):
            opts.archive_format = 'tar'
        else:
            parser.error(
                'Could not deduce archive format from output filename "%s"' % opts.output_filename)

    try:
        opts.transformations = [
            xform.replace(os.path.altsep or os.path.sep, os.path.sep).split('=', 1)
            for xform in opts.transformations
        ]
    except Exception as err:  # pylint: disable=broad-except
        parser.error(err)

    return opts


def open_zip_archive_for_write(filename):
    """Open a zip archive for writing and return it."""

    # Infuriatingly, Zipfile calls the "add" method "write", but they're otherwise identical,
    # for our purposes.  WrappedZipFile is a minimal adapter class.
    class WrappedZipFile(zipfile.ZipFile):
        """WrappedZipFile class."""

        def add(self, filename, arcname):
            """Add filename to zip."""
            return self.write(filename, arcname)

    return WrappedZipFile(filename, 'w', zipfile.ZIP_DEFLATED)


def get_preferred_filename(input_filename, transformations):
    """Return preferred filename.

    Perform a prefix subsitution on 'input_filename' for the
    first matching transformation in 'transformations' and
    returns the substituted string.
    """
    for match, replace in transformations:
        match_lower = match.lower()
        input_filename_lower = input_filename.lower()
        if input_filename_lower.startswith(match_lower):
            return replace + input_filename[len(match):]
    return input_filename


if __name__ == '__main__':
    main(sys.argv)
    sys.exit(0)

"""Generate vcxproj and vcxproj.filters files for browsing code in Visual Studio 2015.

To build mongodb, you must use scons. You can use this project to navigate code during debugging.

 HOW TO USE

 First, you need a compile_commands.json file, to generate run the following command:
   scons compiledb

 Next, run the following command
   python buildscripts/make_vcxproj.py FILE_NAME

  where FILE_NAME is the of the file to generate e.g., "mongod"
"""

import io
import json
import os
import re
import sys
import uuid
import argparse
import xml.etree.ElementTree as ET

VCXPROJ_FOOTER = r"""

  <ItemGroup>
    <None Include="src\mongo\db\mongo.ico" />
  </ItemGroup>

  <ItemGroup>
    <ResourceCompile Include="src\mongo\db\db.rc" />
  </ItemGroup>

  <ItemGroup>
    <Natvis Include="buildscripts\win\mongodb.natvis" />
    <Natvis Include="src\third_party\yaml-cpp\yaml-cpp\src\contrib\yaml-cpp.natvis" />
  </ItemGroup>

  <Import Project="$(VCTargetsPath)\Microsoft.Cpp.targets" />
  <ImportGroup Label="ExtensionTargets"></ImportGroup>
</Project>
"""

VCXPROJ_NAMESPACE = 'http://schemas.microsoft.com/developer/msbuild/2003'

# We preserve certain fields by saving them and restoring between file generations
VCXPROJ_FIELDS_TO_PRESERVE = [
    "NMakeBuildCommandLine",
    "NMakeOutput",
    "NMakeCleanCommandLine",
    "NMakeReBuildCommandLine",
]

VCXPROJ_TOOLSVERSION = {
    "14.1": "15.0",
    "14.2": "16.0",
}

VCXPROJ_PLATFORM_TOOLSET = {
    "14.1": "v141",
    "14.2": "v142",
}

VCXPROJ_WINDOWS_TARGET_SDK = {
    "14.1": "10.0.17763.0",
    "14.2": "10.0.18362.0",
}

VCXPROJ_MSVC_DEFAULT_VERSION = "14.1"  # Visual Studio 2017


def get_defines(args):
    """Parse a compiler argument list looking for defines."""
    ret = set()
    for arg in args:
        if arg.startswith('/D'):
            ret.add(arg[2:])
    return ret


def get_includes(args):
    """Parse a compiler argument list looking for includes."""
    ret = set()
    for arg in args:
        if arg.startswith('/I'):
            ret.add(arg[2:])
    return ret


def _read_vcxproj(file_name):
    """Parse a vcxproj file and look for "NMake" prefixed elements in PropertyGroups."""

    # Skip if this the first run
    if not os.path.exists(file_name):
        return None

    tree = ET.parse(file_name)

    interesting_tags = ['{%s}%s' % (VCXPROJ_NAMESPACE, tag) for tag in VCXPROJ_FIELDS_TO_PRESERVE]

    save_elements = {}

    for parent in tree.getroot():
        for child in parent:
            if child.tag in interesting_tags:
                cond = parent.attrib['Condition']
                save_elements[(parent.tag, child.tag, cond)] = child.text

    return save_elements


def _replace_vcxproj(file_name, restore_elements):
    """Parse a vcxproj file, and replace elememts text nodes with values saved before."""
    # Skip if this the first run
    if not restore_elements:
        return

    tree = ET.parse(file_name)

    interesting_tags = ['{%s}%s' % (VCXPROJ_NAMESPACE, tag) for tag in VCXPROJ_FIELDS_TO_PRESERVE]

    for parent in tree.getroot():
        for child in parent:
            if child.tag in interesting_tags:
                # Match PropertyGroup elements based on their condition
                cond = parent.attrib['Condition']
                saved_value = restore_elements[(parent.tag, child.tag, cond)]
                child.text = saved_value

    stream = io.StringIO()

    tree.write(stream, encoding='unicode')

    str_value = stream.getvalue()

    # Strip the "ns0:" namespace prefix because ElementTree does not support default namespaces.
    str_value = str_value.replace("<ns0:", "<").replace("</ns0:", "</").replace(
        "xmlns:ns0", "xmlns")

    with io.open(file_name, mode='w') as file_handle:
        file_handle.write(str_value)


class ProjFileGenerator(object):  # pylint: disable=too-many-instance-attributes
    """Generate a .vcxproj and .vcxprof.filters file."""

    def __init__(self, target, vs_version):
        """Initialize ProjFileGenerator."""
        # we handle DEBUG in the vcxproj header:
        self.common_defines = set()
        self.common_defines.add("DEBUG")
        self.common_defines.add("_DEBUG")

        self.includes = set()
        self.target = target
        self.compiles = []
        self.files = set()
        self.all_defines = set()
        self.vcxproj = None
        self.filters = None
        self.all_defines = set(self.common_defines)
        self.vcxproj_file_name = self.target + ".vcxproj"
        self.tools_version = VCXPROJ_TOOLSVERSION[vs_version]
        self.platform_toolset = VCXPROJ_PLATFORM_TOOLSET[vs_version]
        self.windows_target_sdk = VCXPROJ_WINDOWS_TARGET_SDK[vs_version]

        self.existing_build_commands = _read_vcxproj(self.vcxproj_file_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.vcxproj = open(
            self.vcxproj_file_name,
            "w",
        )

        with open('buildscripts/vcxproj.header', 'r') as header_file:
            header_str = header_file.read()
            header_str = header_str.replace("%_TARGET_%", self.target)
            header_str = header_str.replace("%ToolsVersion%", self.tools_version)
            header_str = header_str.replace("%PlatformToolset%", self.platform_toolset)
            header_str = header_str.replace("%WindowsTargetPlatformVersion%",
                                            self.windows_target_sdk)
            header_str = header_str.replace("%AdditionalIncludeDirectories%", ';'.join(
                sorted(self.includes)))
            self.vcxproj.write(header_str)

        common_defines = self.all_defines
        for comp in self.compiles:
            common_defines = common_defines.intersection(comp['defines'])

        self.vcxproj.write("<!-- common_defines -->\n")
        self.vcxproj.write("<ItemDefinitionGroup><ClCompile><PreprocessorDefinitions>" +
                           ';'.join(common_defines) + ";%(PreprocessorDefinitions)\n")
        self.vcxproj.write("</PreprocessorDefinitions></ClCompile></ItemDefinitionGroup>\n")

        self.vcxproj.write("  <ItemGroup>\n")
        for command in self.compiles:
            defines = command["defines"].difference(common_defines)
            if defines:
                self.vcxproj.write("    <ClCompile Include=\"" + command["file"] +
                                   "\"><PreprocessorDefinitions>" + ';'.join(defines) +
                                   ";%(PreprocessorDefinitions)" +
                                   "</PreprocessorDefinitions></ClCompile>\n")
            else:
                self.vcxproj.write("    <ClCompile Include=\"" + command["file"] + "\" />\n")
        self.vcxproj.write("  </ItemGroup>\n")

        self.filters = open(self.target + ".vcxproj.filters", "w")
        self.filters.write("<?xml version='1.0' encoding='utf-8'?>\n")
        self.filters.write("<Project ToolsVersion='14.0' " +
                           "xmlns='http://schemas.microsoft.com/developer/msbuild/2003'>\n")

        self.__write_filters()

        self.vcxproj.write(VCXPROJ_FOOTER)
        self.vcxproj.close()

        self.filters.write("</Project>\n")
        self.filters.close()

        # Replace build commands
        _replace_vcxproj(self.vcxproj_file_name, self.existing_build_commands)

    def parse_line(self, line):
        """Parse a build line."""
        if line.startswith("cl"):
            self.__parse_cl_line(line[3:])

    def __parse_cl_line(self, line):
        """Parse a compiler line."""
        # Get the file we are compilong
        file_name = re.search(r"/c ([\w\\.-]+) ", line).group(1)

        # Skip files made by scons for configure testing
        if "sconf_temp" in file_name:
            return

        self.files.add(file_name)

        args = line.split(' ')

        file_defines = set()
        for arg in get_defines(args):
            if arg not in self.common_defines:
                file_defines.add(arg)
        self.all_defines = self.all_defines.union(file_defines)

        for arg in get_includes(args):
            self.includes.add(arg)

        self.compiles.append({"file": file_name, "defines": file_defines})

    @staticmethod
    def __is_header(name):
        """Return True if this a header file."""
        headers = [".h", ".hpp", ".hh", ".hxx"]
        for header in headers:
            if name.endswith(header):
                return True
        return False

    @staticmethod
    def __cpp_file(name):
        """Return True if this a C++ header or source file."""
        file_exts = [".cpp", ".c", ".cxx", ".h", ".hpp", ".hh", ".hxx"]
        file_ext = os.path.splitext(name)[1]
        if file_ext in file_exts:
            return True
        return False

    def __write_filters(self):  # pylint: disable=too-many-branches
        """Generate the vcxproj.filters file."""
        # 1. get a list of directories for all the files
        # 2. get all the C++ files in each of these dirs
        # 3. Output these lists of files to vcxproj and vcxproj.headers
        # Note: order of these lists does not matter, VS will sort them anyway
        dirs = set()
        scons_files = set()

        for file_name in self.files:
            dirs.add(os.path.dirname(file_name))

        base_dirs = set()
        for directory in dirs:
            if not os.path.exists(directory):
                print(("Warning: skipping include file scan for directory '%s'" +
                       " because it does not exist.") % str(directory))
                continue

            # Get all the C++ files
            for file_name in os.listdir(directory):
                if self.__cpp_file(file_name):
                    self.files.add(directory + "\\" + file_name)

            # Make sure the set also includes the base directories
            # (i.e. src/mongo and src as examples)
            base_name = os.path.dirname(directory)
            while base_name:
                base_dirs.add(base_name)
                base_name = os.path.dirname(base_name)

        dirs = dirs.union(base_dirs)

        # Get all the scons files
        for directory in dirs:
            if os.path.exists(directory):
                for file_name in os.listdir(directory):
                    if file_name == "SConstruct" or "SConscript" in file_name:
                        scons_files.add(directory + "\\" + file_name)
        scons_files.add("SConstruct")

        # Write a list of directory entries with unique guids
        self.filters.write("  <ItemGroup>\n")
        for file_name in sorted(dirs):
            self.filters.write("    <Filter Include='%s'>\n" % file_name)
            self.filters.write("        <UniqueIdentifier>{%s}</UniqueIdentifier>\n" % uuid.uuid4())
            self.filters.write("    </Filter>\n")
        self.filters.write("  </ItemGroup>\n")

        # Write a list of files to compile
        self.filters.write("  <ItemGroup>\n")
        for file_name in sorted(self.files):
            if not self.__is_header(file_name):
                self.filters.write("    <ClCompile Include='%s'>\n" % file_name)
                self.filters.write("        <Filter>%s</Filter>\n" % os.path.dirname(file_name))
                self.filters.write("    </ClCompile>\n")
        self.filters.write("  </ItemGroup>\n")

        # Write a list of headers
        self.filters.write("  <ItemGroup>\n")
        for file_name in sorted(self.files):
            if self.__is_header(file_name):
                self.filters.write("    <ClInclude Include='%s'>\n" % file_name)
                self.filters.write("        <Filter>%s</Filter>\n" % os.path.dirname(file_name))
                self.filters.write("    </ClInclude>\n")
        self.filters.write("  </ItemGroup>\n")

        # Write a list of scons files
        self.filters.write("  <ItemGroup>\n")
        for file_name in sorted(scons_files):
            self.filters.write("    <None Include='%s'>\n" % file_name)
            self.filters.write("        <Filter>%s</Filter>\n" % os.path.dirname(file_name))
            self.filters.write("    </None>\n")
        self.filters.write("  </ItemGroup>\n")

        # Write a list of headers into the vcxproj
        self.vcxproj.write("  <ItemGroup>\n")
        for file_name in sorted(self.files):
            if self.__is_header(file_name):
                self.vcxproj.write("    <ClInclude Include='%s' />\n" % file_name)
        self.vcxproj.write("  </ItemGroup>\n")

        # Write a list of scons files into the vcxproj
        self.vcxproj.write("  <ItemGroup>\n")
        for file_name in sorted(scons_files):
            self.vcxproj.write("    <None Include='%s' />\n" % file_name)
        self.vcxproj.write("  </ItemGroup>\n")


def main():
    """Execute Main program."""
    parser = argparse.ArgumentParser(description='VS Project File Generator.')
    parser.add_argument('--version', type=str, nargs='?', help="MSVC Toolchain version",
                        default=VCXPROJ_MSVC_DEFAULT_VERSION)
    parser.add_argument('target', type=str, help="File to generate")

    args = parser.parse_args()

    with ProjFileGenerator(args.target, args.version) as projfile:
        with open("compile_commands.json", "rb") as sjh:
            contents = sjh.read().decode('utf-8')
            commands = json.loads(contents)

        for command in commands:
            command_str = command["command"]
            projfile.parse_line(command_str)


main()

"""Utility functions for SCons to discover and configure MongoDB modules.

A MongoDB module is an organized collection of source code and build rules that can be provided at
compile-time to alter or extend the behavior of MongoDB.  The files comprising a single MongoDB
module are arranged in a directory hierarchy, rooted in a directory whose name is by convention the
module name, and containing in that root directory at least two files: a build.py file and a
SConscript file.

MongoDB modules are discovered by a call to the discover_modules() function, whose sole parameter is
the directory which is the immediate parent of all module directories.  The exact directory is
chosen by the SConstruct file, which is the direct consumer of this python module.  The only rule is
that it must be a subdirectory of the src/ directory, to correctly work with the SCons variant
directory system that separates build products for source.

Once discovered, modules are configured by the configure_modules() function, and the build system
integrates their SConscript files into the rest of the build.

MongoDB module build.py files implement a single function, configure(conf, env), which they may use
to configure the supplied "env" object.  The configure functions may add extra LIBDEPS to mongod,
mongos and the mongo shell (TODO: other mongo tools and the C++ client), and through those libraries
alter those programs' behavior.

MongoDB module SConscript files can describe libraries, programs and unit tests, just as other
MongoDB SConscript files do.
"""

__all__ = ('discover_modules', 'discover_module_directories', 'configure_modules',
           'register_module_test')  # pylint: disable=undefined-all-variable

import imp
import inspect
import os


def discover_modules(module_root, allowed_modules):
    """Scan module_root for subdirectories that look like MongoDB modules.

    Return a list of imported build.py module objects.
    """
    found_modules = []

    if allowed_modules is not None:
        allowed_modules = allowed_modules.split(',')

    if not os.path.isdir(module_root):
        return found_modules

    for name in os.listdir(module_root):
        root = os.path.join(module_root, name)
        if name.startswith('.') or not os.path.isdir(root):
            continue

        build_py = os.path.join(root, 'build.py')
        module = None

        if allowed_modules is not None and name not in allowed_modules:
            print("skipping module: %s" % (name))
            continue

        try:
            print("adding module: %s" % (name))
            fp = open(build_py, "r")
            try:
                module = imp.load_module("module_" + name, fp, build_py,
                                         (".py", "r", imp.PY_SOURCE))
                if getattr(module, "name", None) is None:
                    module.name = name
                found_modules.append(module)
            finally:
                fp.close()
        except (FileNotFoundError, IOError):
            pass

    return found_modules


def discover_module_directories(module_root, allowed_modules):
    """Scan module_root for subdirectories that look like MongoDB modules.

    Return a list of directory names.
    """
    if not os.path.isdir(module_root):
        return []

    found_modules = []

    if allowed_modules is not None:
        allowed_modules = allowed_modules.split(',')

    for name in os.listdir(module_root):
        root = os.path.join(module_root, name)
        if name.startswith('.') or not os.path.isdir(root):
            continue

        build_py = os.path.join(root, 'build.py')

        if allowed_modules is not None and name not in allowed_modules:
            print("skipping module: %s" % (name))
            continue

        if os.path.isfile(build_py):
            print("adding module: %s" % (name))
            found_modules.append(name)

    return found_modules


def configure_modules(modules, conf):
    """Run the configure() function in the build.py python modules for each module in "modules".

    The modules were created by discover_modules.

    The configure() function should prepare the Mongo build system for building the module.
    """
    env = conf.env
    env['MONGO_MODULES'] = []
    for module in modules:
        name = module.name
        print("configuring module: %s" % (name))
        modules_configured = module.configure(conf, env)
        if modules_configured:
            for module_name in modules_configured:
                env['MONGO_MODULES'].append(module_name)
        else:
            env['MONGO_MODULES'].append(name)


def get_module_sconscripts(modules):
    """Return all modules' sconscripts."""
    sconscripts = []
    for mod in modules:
        module_dir_path = __get_src_relative_path(os.path.join(os.path.dirname(mod.__file__)))
        sconscripts.append(os.path.join(module_dir_path, 'SConscript'))
    return sconscripts


def __get_src_relative_path(path):
    """Return a path relative to ./src.

    The src directory is important because of its relationship to BUILD_DIR,
    established in the SConstruct file.  For variant directories to work properly
    in SCons, paths relative to the src or BUILD_DIR must often be generated.
    """
    src_dir = os.path.abspath('src')
    path = os.path.abspath(os.path.normpath(path))
    if not path.startswith(src_dir):
        raise ValueError('Path "%s" is not relative to the src directory "%s"' % (path, src_dir))
    result = path[len(src_dir) + 1:]
    return result


def __get_module_path(module_frame_depth):
    """Return the path to the MongoDB module whose build.py is executing "module_frame_depth" frames.

    This is above this function, relative to the "src" directory.
    """
    module_filename = inspect.stack()[module_frame_depth + 1][1]
    return os.path.dirname(__get_src_relative_path(module_filename))


def __get_module_src_path(module_frame_depth):
    """Return the path relative to the SConstruct file of the MongoDB module's source tree.

    module_frame_depth is the number of frames above the current one in which one can find a
    function from the MongoDB module's build.py function.
    """
    return os.path.join('src', __get_module_path(module_frame_depth + 1))


def __get_module_build_path(module_frame_depth):
    """Return the path relative to the SConstruct file of the MongoDB module's build tree.

    module_frame_depth is the number of frames above the current one in which one can find a
    function from the MongoDB module's build.py function.
    """
    return os.path.join('$BUILD_DIR', __get_module_path(module_frame_depth + 1))


def get_current_module_src_path():
    """Return the path relative to the SConstruct file of the current MongoDB module's source tree.

    May only meaningfully be called from within build.py
    """
    return __get_module_src_path(1)


def get_current_module_build_path():
    """Return the path relative to the SConstruct file of the current MongoDB module's build tree.

    May only meaningfully be called from within build.py
    """

    return __get_module_build_path(1)


def get_current_module_libdep_name(libdep_rel_path):
    """Return a $BUILD_DIR relative path to a "libdep_rel_path".

    The "libdep_rel_path" is relative to the MongoDB module's build.py file.

    May only meaningfully be called from within build.py
    """
    return os.path.join(__get_module_build_path(1), libdep_rel_path)

#!/usr/bin/env python3
"""Script and library for symbolizing MongoDB stack traces.

To use as a script, paste the JSON object on the line after ----- BEGIN BACKTRACE ----- into the
standard input of this script. There are numerous caveats. In the default mode, you need
to pass in the path to the executable being symbolized, and if you want shared library stack
traces, you must be on the same system.

There is largely untested support for extracting debug information from S3 buckets. This work
is experimental.

Sample usage:

mongosymb.py --symbolizer-path=/path/to/llvm-symbolizer /path/to/executable </file/with/stacktrace

You can also pass --output-format=json, to get rich json output. It shows some extra information,
but emits json instead of plain text.
"""

import json
import argparse
import os
import subprocess
import sys


def parse_input(trace_doc, dbg_path_resolver):
    """Return a list of frame dicts from an object of {backtrace: list(), processInfo: dict()}."""

    def make_base_addr_map(somap_list):
        """Return map from binary load address to description of library from the somap_list.

        The somap_list is a list of dictionaries describing individual loaded libraries.
        """
        return {so_entry["b"]: so_entry for so_entry in somap_list if "b" in so_entry}

    base_addr_map = make_base_addr_map(trace_doc["processInfo"]["somap"])

    frames = []
    for frame in trace_doc["backtrace"]:
        if "b" not in frame:
            print(
                f"Ignoring frame {frame} as it's missing the `b` field; See SERVER-58863 for discussions"
            )
            continue
        soinfo = base_addr_map.get(frame["b"], {})
        elf_type = soinfo.get("elfType", 0)
        if elf_type == 3:
            addr_base = "0"
        elif elf_type == 2:
            addr_base = frame["b"]
        else:
            addr_base = soinfo.get("vmaddr", "0")
        addr = int(addr_base, 16) + int(frame["o"], 16)
        # addr currently points to the return address which is the one *after* the call. x86 is
        # variable length so going backwards is difficult. However llvm-symbolizer seems to do the
        # right thing if we just subtract 1 byte here. This has the downside of also adjusting the
        # address of instructions that cause signals (such as segfaults and divide-by-zero) which
        # are already correct, but there doesn't seem to be a reliable way to detect that case.
        addr -= 1
        frames.append(
            dict(
                path=dbg_path_resolver.get_dbg_file(soinfo), buildId=soinfo.get("buildId", None),
                offset=frame["o"], addr="0x{:x}".format(addr), symbol=frame.get("s", None)))
    return frames


def symbolize_frames(trace_doc, dbg_path_resolver, symbolizer_path, dsym_hint, input_format,
                     **kwargs):
    """Return a list of symbolized stack frames from a trace_doc in MongoDB stack dump format."""

    # Keep frames in kwargs to avoid changing the function signature.
    frames = kwargs.get("frames")
    if frames is None:
        frames = preprocess_frames(dbg_path_resolver, trace_doc, input_format)

    if not symbolizer_path:
        symbolizer_path = os.environ.get("MONGOSYMB_SYMBOLIZER_PATH", "llvm-symbolizer")

    symbolizer_args = [symbolizer_path]
    for dh in dsym_hint:
        symbolizer_args.append("-dsym-hint={}".format(dh))
    symbolizer_process = subprocess.Popen(args=symbolizer_args, close_fds=True,
                                          stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                          stderr=open("/dev/null"))

    def extract_symbols(stdin):
        """Extract symbol information from the output of llvm-symbolizer.

        Return a list of dictionaries, each of which has fn, file, column and line entries.

        The format of llvm-symbolizer output is that for every CODE line of input,
        it outputs zero or more pairs of lines, and then a blank line. This way, if
        a CODE line of input maps to several inlined functions, you can use the blank
        line to find the end of the list of symbols corresponding to the CODE line.

        The first line of each pair contains the function name, and the second contains the file,
        column and line information.
        """
        result = []
        step = 0
        while True:
            line = stdin.readline().decode()
            if line == "\n":
                break
            if step == 0:
                result.append({"fn": line.strip()})
                step = 1
            else:
                file_name, line, column = line.strip().rsplit(':', 3)
                result[-1].update({"file": file_name, "column": int(column), "line": int(line)})
                step = 0
        return result

    for frame in frames:
        if frame["path"] is None:
            continue
        symbol_line = "CODE {path:} {addr:}\n".format(**frame)
        symbolizer_process.stdin.write(symbol_line.encode())
        symbolizer_process.stdin.flush()
        frame["symbinfo"] = extract_symbols(symbolizer_process.stdout)
    symbolizer_process.stdin.close()
    symbolizer_process.wait()
    return frames


def preprocess_frames(dbg_path_resolver, trace_doc, input_format):
    """Process the paths in frame objects."""
    if input_format == "classic":
        frames = parse_input(trace_doc, dbg_path_resolver)
    elif input_format == "thin":
        frames = trace_doc["backtrace"]
        for frame in frames:
            frame["path"] = dbg_path_resolver.get_dbg_file(frame)
    else:
        raise ValueError('Unknown input format "{}"'.format(input_format))
    return frames


class PathDbgFileResolver(object):
    """PathDbgFileResolver class."""

    def __init__(self, bin_path_guess):
        """Initialize PathDbgFileResolver."""
        self._bin_path_guess = os.path.realpath(bin_path_guess)
        self.mci_build_dir = None

    def get_dbg_file(self, soinfo):
        """Return dbg file name."""
        path = soinfo.get("path", "")
        # TODO: make identifying mongo shared library directory more robust
        if self.mci_build_dir is None and path.startswith("/data/mci/"):
            self.mci_build_dir = path.split("/src/", maxsplit=1)[0]
        return path if path else self._bin_path_guess


class S3BuildidDbgFileResolver(object):
    """S3BuildidDbgFileResolver class."""

    def __init__(self, cache_dir, s3_bucket):
        """Initialize S3BuildidDbgFileResolver."""
        self._cache_dir = cache_dir
        self._s3_bucket = s3_bucket
        self.mci_build_dir = None

    def get_dbg_file(self, soinfo):
        """Return dbg file name."""
        build_id = soinfo.get("buildId", None)
        if build_id is None:
            return None
        build_id = build_id.lower()
        build_id_path = os.path.join(self._cache_dir, build_id + ".debug")
        if not os.path.exists(build_id_path):
            try:
                self._get_from_s3(build_id)
            except Exception:  # pylint: disable=broad-except
                ex = sys.exc_info()[0]
                sys.stderr.write("Failed to find debug symbols for {} in s3: {}\n".format(
                    build_id, ex))
                return None
        if not os.path.exists(build_id_path):
            return None
        return build_id_path

    def _get_from_s3(self, build_id):
        """Download debug symbols from S3."""
        subprocess.check_call(
            ['wget', 'https://s3.amazonaws.com/{}/{}.debug.gz'.format(self._s3_bucket, build_id)],
            cwd=self._cache_dir)
        subprocess.check_call(['gunzip', build_id + ".debug.gz"], cwd=self._cache_dir)


def classic_output(frames, outfile, **kwargs):  # pylint: disable=unused-argument
    """Provide classic output."""
    for frame in frames:
        symbinfo = frame["symbinfo"]
        if symbinfo:
            for sframe in symbinfo:
                outfile.write(" {file:s}:{line:d}:{column:d}: {fn:s}\n".format(**sframe))
        else:
            outfile.write(" {path:s}!!!\n".format(**symbinfo))


def make_argument_parser(parser=None, **kwargs):
    """Make and return an argparse."""
    if parser is None:
        parser = argparse.ArgumentParser(**kwargs)

    parser.add_argument('--dsym-hint', default=[], action='append')
    parser.add_argument('--symbolizer-path', default='')
    parser.add_argument('--input-format', choices=['classic', 'thin'], default='classic')
    parser.add_argument('--output-format', choices=['classic', 'json'], default='classic',
                        help='"json" shows some extra information')
    parser.add_argument('--debug-file-resolver', choices=['path', 's3'], default='path')
    parser.add_argument('--src-dir-to-move', action="store", type=str, default=None,
                        help="Specify a src dir to move to /data/mci/{original_buildid}/src")

    s3_group = parser.add_argument_group(
        "s3 options", description='Options used with \'--debug-file-resolver s3\'')
    s3_group.add_argument('--s3-cache-dir')
    s3_group.add_argument('--s3-bucket')

    # Look for symbols in the cwd by default.
    parser.add_argument('path_to_executable', nargs="?")
    return parser


def main(options):
    """Execute Main program."""

    # Skip over everything before the first '{' since it is likely to be log line prefixes.
    # Additionally, using raw_decode() to ignore extra data after the closing '}' to allow maximal
    # sloppiness in copy-pasting input.
    trace_doc = sys.stdin.read()

    if not trace_doc or not trace_doc.strip():
        print("Please provide the backtrace through stdin for symbolization;"
              "e.g. `your/symbolization/command < /file/with/stacktrace`")
    trace_doc = trace_doc[trace_doc.find('{'):]
    trace_doc = json.JSONDecoder().raw_decode(trace_doc)[0]

    # Search the trace_doc for an object having "backtrace" and "processInfo" keys.
    def bt_search(obj):
        try:
            if "backtrace" in obj and "processInfo" in obj:
                return obj
            for _, val in obj.items():
                res = bt_search(val)
                if res:
                    return res
        except (TypeError, AttributeError):
            pass
        return None

    trace_doc = bt_search(trace_doc)

    if not trace_doc:
        print("could not find json backtrace object in input", file=sys.stderr)
        exit(1)

    output_fn = None
    if options.output_format == 'json':
        output_fn = json.dump
    if options.output_format == 'classic':
        output_fn = classic_output

    resolver = None
    if options.debug_file_resolver == 'path':
        resolver = PathDbgFileResolver(options.path_to_executable)
    elif options.debug_file_resolver == 's3':
        resolver = S3BuildidDbgFileResolver(options.s3_cache_dir, options.s3_bucket)

    frames = preprocess_frames(resolver, trace_doc, options.input_format)

    if options.src_dir_to_move and resolver.mci_build_dir is not None:
        try:
            os.makedirs(resolver.mci_build_dir)
            os.symlink(
                os.path.join(os.getcwd(), options.src_dir_to_move),
                os.path.join(resolver.mci_build_dir, 'src'))
        except FileExistsError:
            pass

    frames = symbolize_frames(frames=frames, trace_doc=trace_doc, dbg_path_resolver=resolver,
                              **vars(options))
    output_fn(frames, sys.stdout, indent=2)


if __name__ == '__main__':
    symbolizer_options = make_argument_parser(description=__doc__).parse_args()
    main(symbolizer_options)
    sys.exit(0)

#!/usr/bin/env python3
"""Script for symbolizing multithread MongoDB stack traces.

Accepts mongod multithread stacktrace lines. These are produced by hitting mongod with SIGUSR2.
Assembles json documents which are fed to the mongosymb library. See mongosymb.py.
"""

import argparse
import json
import re
import subprocess
import sys
import mongosymb


def main():
    """Execute Main program."""

    parent_parser = mongosymb.make_argument_parser(add_help=False)
    parser = argparse.ArgumentParser(parents=[parent_parser], description=__doc__, add_help=True)
    options = parser.parse_args()

    # Remember the prologue between lines,
    # Prologue defines the library ids referred to by each record line.
    prologue = None

    for line in sys.stdin:
        try:
            doc = json.JSONDecoder().raw_decode(line)[0]
            if "attr" not in doc:
                continue
            attr = doc["attr"]

            if "prologue" in attr:
                prologue = attr["prologue"]

            # "threadRecord" is an older name for "record".
            # Accept either name.
            for record_field_name in ("record", "threadRecord"):
                if record_field_name not in attr:
                    continue
                thread_record = attr[record_field_name]
                merged = {**thread_record, **prologue}

                output_fn = None
                if options.output_format == 'json':
                    output_fn = json.dump
                if options.output_format == 'classic':
                    output_fn = mongosymb.classic_output

                resolver = None
                if options.debug_file_resolver == 'path':
                    resolver = mongosymb.PathDbgFileResolver(options.path_to_executable)
                elif options.debug_file_resolver == 's3':
                    resolver = mongosymb.S3BuildidDbgFileResolver(options.s3_cache_dir,
                                                                  options.s3_bucket)

                frames = mongosymb.symbolize_frames(merged, resolver, **vars(options))
                print("\nthread {{name='{}', tid={}}}:".format(thread_record["name"],
                                                               thread_record["tid"]))

                output_fn(frames, sys.stdout, indent=2)

        except json.JSONDecodeError:
            print("failed to parse line: `{}`".format(line), file=sys.stderr)


if __name__ == '__main__':
    main()
    sys.exit(0)

#!/bin/bash
# Script used to mount /data & /log on a separate drive from root.
# This script must be invoked either by a root user or with sudo.
# Currently only supports Windows & Linux.
# See _usage_ for how this script should be invoked.

set -o errexit

# Default options
fs_type=xfs
user_group=$USER:$(id -Gn $USER | cut -f1 -d ' ')

# _usage_: Provides usage infomation
function _usage_ {
  cat << EOF
usage: $0 options
This script supports the following parameters for Windows & Linux platforms:
  -d <deviceNames>,    REQUIRED, Space separated list of devices to mount /data on,
                       i.e., "xvdb xvdc", more than one device indicates a RAID set.
                       For Windows, specify the drive letter, i.e., "d".
  -r <raidDeviceName>, REQUIRED, if more the one device is specified in <deviceNames>,
                       i.e., md0.
                       Not supported on Windows.
  -l <deviceName>,     OPTIONAL, Device name to mount /log on, i.e., "xvdd".
                       For Windows, specify the drive letter, i.e., "e".
  -t <fsType>,         File system type, defaults to '$fs_type'.
                       Not supported on Windows.
  -o <mountOptions>,   File system mount options, i.e., "-m crc=0,finobt=0".
                       Not supported on Windows.
  -u <user:group>,     User:Group to make owner of /data & /log. Defaults to '$user_group'.
EOF
}


# Parse command line options
while getopts "d:l:o:r:t:u:?" option
do
  case $option in
    d)
      data_device_names=$OPTARG
      ;;
    l)
      log_device_name=$OPTARG
      ;;
    o)
      mount_options=$OPTARG
      ;;
    r)
      data_raid_device_name=$OPTARG
        ;;
    t)
      fs_type=$OPTARG
      ;;
    u)
      user_group=$OPTARG
      ;;
    \?|*)
      _usage_
      exit 0
      ;;
  esac
done

function mount_drive {
  local root_dir=$1
  local sub_dirs=$2
  local device_names=$3
  local raid_device_name=$4
  local mount_options=$5
  local fs_type=$6
  local user_group=$7

  # Determine how many devices were specified
  local num_devices=0
  for device_name in $device_names
  do
    local devices="$devices /dev/$device_name"
    let num_devices=num_devices+1
  done

  # $OS is defined in Cygwin
  if [ "Windows_NT" = "$OS" ]; then
    if [ $num_devices -ne 1 ]; then
      echo "Must specify only one drive"
      _usage_
      exit 1
    fi

    local drive_poll_retry=0
    local drive_poll_delay=0
    local drive_retry_max=40

    local drive=$device_names
    local system_drive=c

    while true;
    do
      sleep $drive_poll_delay
      echo "Looking for drive '$drive' to mount $root_dir"
      if [ -d /cygdrive/$drive ]; then
        echo "Found drive"
        rm -rf /$root_dir
        rm -rf /cygdrive/$system_drive/$root_dir
        mkdir $drive:\\$root_dir
        cmd.exe /c mklink /J $system_drive:\\$root_dir $drive:\\$root_dir
        ln -s /cygdrive/$drive/$root_dir /$root_dir
        setfacl -s user::rwx,group::rwx,other::rwx /cygdrive/$drive/$root_dir
        for sub_dir in $sub_dirs
        do
            mkdir -p /cygdrive/$drive/$root_dir/$sub_dir
        done
        chown -R $user_group /cygdrive/$system_drive/$root_dir
        break
      fi
      let drive_poll_retry=drive_poll_retry+1
      if [ $drive_poll_retry -eq $drive_retry_max ]; then
        echo "Timed out trying to mount $root_dir drive."
        exit 1
      fi
      let drive_poll_delay=drive_poll_delay+5
    done

  elif [ $(uname | awk '{print tolower($0)}') = "linux" ]; then
    if [ $num_devices -eq 0 ]; then
      echo "Must specify atleast one device"
      _usage_
      exit 1
    elif [ $num_devices -gt 1 ]; then
      if [ -z "$raid_device_name" ]; then
        echo "Missing RAID device name"
        _usage_
        exit 1
      fi
    fi

    # Unmount the current devices, if already mounted
    umount /mnt || true
    umount $devices || true

    # Determine if we have a RAID set
    if [ ! -z "$raid_device_name" ]; then
      echo "Creating RAID set on '$raid_device_name' for devices '$devices'"
      device_name=/dev/$raid_device_name
      /sbin/udevadm control --stop-exec-queue
      yes | /sbin/mdadm --create $device_name --level=0 -c256 --raid-devices=$num_devices $devices
      /sbin/udevadm control --start-exec-queue
      /sbin/mdadm --detail --scan > /etc/mdadm.conf
      /sbin/blockdev --setra 32 $device_name
    else
      device_name="/dev/$device_names"
    fi

    # Mount the $root_dir drive(s)
    /sbin/mkfs.$fs_type $mount_options -f $device_name
    # We add an entry for the device to /etc/fstab so it is automatically mounted following a
    # machine reboot. The device is not guaranteed to be assigned the same name across restarts so
    # we use its UUID in order to identify it.
    #
    # We also specify type=$fs_type in the /etc/fstab entry because specifying type=auto on
    # Amazon Linux AMI 2018.03 leads to the drive not being mounted automatically following a
    # machine reboot.
    device_uuid=$(blkid -o value -s UUID "$device_name")
    echo "Adding entry to /etc/fstab for device '$device_name' with UUID '$device_uuid'"
    echo "UUID=$device_uuid /$root_dir $fs_type noatime 0 0" | tee -a /etc/fstab
    mkdir /$root_dir || true
    chmod 777 /$root_dir
    mount -t $fs_type "UUID=$device_uuid" /$root_dir
    for sub_dir in $sub_dirs
    do
      mkdir -p /$root_dir/$sub_dir
      chmod 1777 /$root_dir/$sub_dir
    done
    chown -R $user_group /$root_dir
  else
    echo "Unsupported OS '$(uname)'"
    exit 0
  fi
}

mount_drive data "db tmp" "$data_device_names" "$data_raid_device_name" "$mount_options" "$fs_type" "$user_group"
mount_drive log "" "$log_device_name" "" "$mount_options" "$fs_type" "$user_group"

"""Script to fix up our MSI files."""

import argparse
import shutil

import msilib


def exec_delete(db, query):
    """Execute delete on db."""
    view = db.OpenView(query)
    view.Execute(None)

    cur_record = view.Fetch()
    view.Modify(msilib.MSIMODIFY_DELETE, cur_record)
    view.Close()


def exec_update(db, query, column, value):
    """Execute update on db."""
    view = db.OpenView(query)
    view.Execute(None)

    cur_record = view.Fetch()
    cur_record.SetString(column, value)
    view.Modify(msilib.MSIMODIFY_REPLACE, cur_record)
    view.Close()


def main():
    """Execute Main program."""
    parser = argparse.ArgumentParser(description='Trim MSI.')
    parser.add_argument('file', type=argparse.FileType('r'), help='file to trim')
    parser.add_argument('out', type=argparse.FileType('w'), help='file to output to')

    args = parser.parse_args()
    print("Trimming MSI")

    db = msilib.OpenDatabase(args.file.name, msilib.MSIDBOPEN_DIRECT)

    exec_delete(
        db,
        "select * from ControlEvent WHERE Dialog_ = 'LicenseAgreementDlg' AND Control_ = 'Next' AND Event = 'NewDialog' AND Argument = 'CustomizeDlg'"
    )
    exec_delete(
        db,
        "select * from ControlEvent WHERE Dialog_ = 'CustomizeDlg' AND Control_ = 'Back' AND Event = 'NewDialog' AND Argument = 'LicenseAgreementDlg'"
    )
    exec_delete(
        db,
        "select * from ControlEvent WHERE Dialog_ = 'CustomizeDlg' AND Control_ = 'Next' AND Event = 'NewDialog' AND Argument = 'VerifyReadyDlg'"
    )
    exec_delete(
        db,
        "select * from ControlEvent WHERE Dialog_ = 'VerifyReadyDlg' AND Control_ = 'Back' AND Event = 'NewDialog' AND Argument = 'CustomizeDlg'"
    )

    db.Commit()

    shutil.copyfile(args.file.name, args.out.name)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Packager module.

This program makes Debian and RPM repositories for MongoDB, by
downloading our tarballs of statically linked executables and
insinuating them into Linux packages.  It must be run on a
Debianoid, since Debian provides tools to make RPMs, but RPM-based
systems don't provide debian packaging crud.

Notes
-----
* Almost anything that you want to be able to influence about how a
package construction must be embedded in some file that the
packaging tool uses for input (e.g., debian/rules, debian/control,
debian/changelog; or the RPM specfile), and the precise details are
arbitrary and silly.  So this program generates all the relevant
inputs to the packaging tools.

* Once a .deb or .rpm package is made, there's a separate layer of
tools that makes a "repository" for use by the apt/yum layers of
package tools.  The layouts of these repositories are arbitrary and
silly, too.

* Before you run the program on a new host, these are the
prerequisites:

apt-get install dpkg-dev rpm debhelper fakeroot ia32-libs createrepo git-core
echo "Now put the dist gnupg signing keys in ~root/.gnupg"

"""

import argparse
import errno
from glob import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

# The MongoDB names for the architectures we support.
ARCH_CHOICES = ["x86_64", "arm64", "aarch64", "s390x"]

# Made up names for the flavors of distribution we package for.
DISTROS = ["suse", "debian", "redhat", "ubuntu", "amazon", "amazon2"]


class Spec(object):
    """Spec class."""

    def __init__(self, ver, gitspec=None, rel=None):
        """Initialize Spec."""
        self.ver = ver
        self.gitspec = gitspec
        self.rel = rel

    # Commit-triggerd version numbers can be in the form: 3.0.7-pre-, or 3.0.7-5-g3b67ac
    # Patch builds version numbers are in the form: 3.5.5-64-g03945fa-patch-58debcdb3ff1223c9d00005b
    #
    def is_nightly(self):
        """Return True if nightly."""
        return bool(re.search("-$", self.version())) or bool(
            re.search(r"\d-\d+-g[0-9a-f]+$", self.version()))

    def is_patch(self):
        """Return True if patch."""
        return bool(re.search(r"\d-\d+-g[0-9a-f]+-patch-[0-9a-f]+$", self.version()))

    def is_rc(self):
        """Return True if rc."""
        return bool(re.search(r"(-rc|-alpha)\d+$", self.version()))

    def is_pre_release(self):
        """Return True if pre-release."""
        return self.is_rc() or self.is_nightly()

    def version(self):
        """Return version."""
        return self.ver

    def patch_id(self):
        """Return patch id."""
        if self.is_patch():
            return re.sub(r'.*-([0-9a-f]+$)', r'\1', self.version())
        return "none"

    def metadata_gitspec(self):
        """Git revision to use for spec+control+init+manpage files.

        The default is the release tag for the version being packaged.
        """
        if self.gitspec:
            return self.gitspec
        return 'r' + self.version()

    def version_better_than(self, version_string):
        """Return True if 'version_string' is greater than instance version."""
        # FIXME: this is wrong, but I'm in a hurry.
        # e.g., "1.8.2" < "1.8.10", "1.8.2" < "1.8.2-rc1"
        return self.ver > version_string

    def suffix(self):
        """Return suffix."""
        if int(self.ver.split(".")[0]) >= 5:
            return "-org" if int(self.ver.split(".")[1]) == 0 else "-org-unstable"
        else:
            return "-org" if int(self.ver.split(".")[1]) % 2 == 0 else "-org-unstable"

    def prelease(self):
        """Return pre-release verison suffix."""
        # NOTE: This is only called for RPM packages, and only after
        # pversion() below has been called. If you want to change this format
        # and want DEB packages to match, make sure to update pversion()
        # below
        #
        # "N" is either passed in on the command line, or "1"
        if self.rel:
            corenum = self.rel
        else:
            corenum = 1

        # Version suffix for RPM packages:
        # 1) RC's - "0.N.rcX"
        # 2) Nightly (snapshot) - "0.N.latest"
        # 3) Patch builds - "0.N.patch.<patch_id>"
        # 4) Standard release - "N"
        if self.is_rc():
            return "0.%s.%s" % (corenum, re.sub('.*-', '', self.version()))
        elif self.is_nightly():
            return "0.%s.latest" % (corenum)
        elif self.is_patch():
            return "0.%s.patch.%s" % (corenum, self.patch_id())
        return str(corenum)

    def pversion(self, distro):
        """Return the pversion."""
        # Note: Debian packages have funny rules about dashes in
        # version numbers, and RPM simply forbids dashes.  pversion
        # will be the package's version number (but we need to know
        # our upstream version too).

        # For RPM packages this just returns X.Y.X because of the
        # aforementioned rules, and prelease (above) adds a suffix later,
        # so detect this case early
        if re.search("(suse|redhat|fedora|centos|amazon)", distro.name()):
            return re.sub("-.*", "", self.version())

        # For DEB packages, this code sets the full version. If you change
        # this format and want RPM packages to match make sure you change
        # prelease above as well
        if re.search("^(debian|ubuntu)", distro.name()):
            if self.is_nightly():
                ver = re.sub("-.*", "-latest", self.ver)
            elif self.is_patch():
                ver = re.sub("-.*", "", self.ver) + "-patch-" + self.patch_id()
            else:
                ver = self.ver

            return re.sub("-", "~", ver)

        raise Exception("BUG: unsupported platform?")

    def branch(self):
        """Return the major and minor portions of the specified version.

        For example, if the version is "2.5.5" the branch would be "2.5"
        """
        return ".".join(self.ver.split(".")[0:2])


class Distro(object):
    """Distro class."""

    def __init__(self, string):
        """Initialize Distro."""
        self.dname = string

    def name(self):
        """Return name."""
        return self.dname

    @staticmethod
    def pkgbase():
        """Return pkgbase."""
        return "mongodb"

    def archname(self, arch):
        """Return the packaging system's architecture name.

        Power and x86 have different names for apt/yum (ppc64le/ppc64el
        and x86_64/amd64).
        """
        # pylint: disable=too-many-return-statements
        if re.search("^(debian|ubuntu)", self.dname):
            if arch == "ppc64le":
                return "ppc64el"
            elif arch == "s390x":
                return "s390x"
            elif arch == "arm64":
                return "arm64"
            elif arch.endswith("86"):
                return "i386"
            return "amd64"
        elif re.search("^(suse|centos|redhat|fedora|amazon)", self.dname):
            if arch == "ppc64le":
                return "ppc64le"
            elif arch == "s390x":
                return "s390x"
            elif arch.endswith("86"):
                return "i686"
            elif arch == "arm64":
                return "arm64"
            elif arch == "aarch64":
                return "aarch64"
            return "x86_64"
        else:
            raise Exception("BUG: unsupported platform?")
        # pylint: enable=too-many-return-statements

    def repodir(self, arch, build_os, spec):  # noqa: D406,D407,D412,D413
        """Return the directory where we'll place the package files for (distro, distro_version).

        This is in that distro's preferred repository
        layout (as distinct from where that distro's packaging building
        tools place the package files).

        Examples:

        repo/apt/ubuntu/dists/precise/mongodb-org/2.5/multiverse/binary-amd64
        repo/apt/ubuntu/dists/precise/mongodb-org/2.5/multiverse/binary-i386

        repo/apt/ubuntu/dists/trusty/mongodb-org/2.5/multiverse/binary-amd64
        repo/apt/ubuntu/dists/trusty/mongodb-org/2.5/multiverse/binary-i386

        repo/apt/debian/dists/wheezy/mongodb-org/2.5/main/binary-amd64
        repo/apt/debian/dists/wheezy/mongodb-org/2.5/main/binary-i386

        repo/yum/redhat/6/mongodb-org/2.5/x86_64
        yum/redhat/6/mongodb-org/2.5/i386

        repo/zypper/suse/11/mongodb-org/2.5/x86_64
        zypper/suse/11/mongodb-org/2.5/i386
        """

        repo_directory = ""

        if spec.is_pre_release():
            repo_directory = "testing"
        else:
            repo_directory = spec.branch()

        if re.search("^(debian|ubuntu)", self.dname):
            return "repo/apt/%s/dists/%s/mongodb-org/%s/%s/binary-%s/" % (
                self.dname, self.repo_os_version(build_os), repo_directory, self.repo_component(),
                self.archname(arch))
        elif re.search("(redhat|fedora|centos|amazon)", self.dname):
            return "repo/yum/%s/%s/mongodb-org/%s/%s/RPMS/" % (
                self.dname, self.repo_os_version(build_os), repo_directory, self.archname(arch))
        elif re.search("(suse)", self.dname):
            return "repo/zypper/%s/%s/mongodb-org/%s/%s/RPMS/" % (
                self.dname, self.repo_os_version(build_os), repo_directory, self.archname(arch))
        else:
            raise Exception("BUG: unsupported platform?")

    def repo_component(self):
        """Return the name of the section/component/pool we are publishing into.

        Example, "multiverse" for Ubuntu, "main" for debian.
        """
        if self.dname == 'ubuntu':
            return "multiverse"
        elif self.dname == 'debian':
            return "main"
        else:
            raise Exception("unsupported distro: %s" % self.dname)

    def repo_os_version(self, build_os):  # pylint: disable=too-many-branches
        """Return an OS version suitable for package repo directory naming.

        Example, 5, 6 or 7 for redhat/centos, "precise," "wheezy," etc.
        for Ubuntu/Debian, 11 for suse, "2013.03" for amazon.
        """
        # pylint: disable=too-many-return-statements
        if self.dname == 'suse':
            return re.sub(r'^suse(\d+)$', r'\1', build_os)
        if self.dname == 'redhat':
            return re.sub(r'^rhel(\d).*$', r'\1', build_os)
        if self.dname == 'amazon':
            return "2013.03"
        elif self.dname == 'amazon2':
            return "2017.12"
        elif self.dname == 'ubuntu':
            if build_os == 'ubuntu1204':
                return "precise"
            elif build_os == 'ubuntu1404':
                return "trusty"
            elif build_os == 'ubuntu1604':
                return "xenial"
            elif build_os == 'ubuntu1804':
                return "bionic"
            elif build_os == 'ubuntu2004':
                return "focal"
            else:
                raise Exception("unsupported build_os: %s" % build_os)
        elif self.dname == 'debian':
            if build_os == 'debian81':
                return 'jessie'
            elif build_os == 'debian92':
                return 'stretch'
            elif build_os == 'debian10':
                return 'buster'
            else:
                raise Exception("unsupported build_os: %s" % build_os)
        else:
            raise Exception("unsupported distro: %s" % self.dname)
        # pylint: enable=too-many-return-statements

    def make_pkg(self, build_os, arch, spec, srcdir):
        """Return the package."""
        if re.search("^(debian|ubuntu)", self.dname):
            return make_deb(self, build_os, arch, spec, srcdir)
        elif re.search("^(suse|centos|redhat|fedora|amazon)", self.dname):
            return make_rpm(self, build_os, arch, spec, srcdir)
        else:
            raise Exception("BUG: unsupported platform?")

    def build_os(self, arch):
        """Return the build os label in the binary package to download.

        Example, "rhel55" for redhat, "ubuntu1204" for ubuntu, "debian81" for debian,
        "suse11" for suse, etc.
        """
        # Community builds only support amd64
        if arch not in ['x86_64', 'ppc64le', 's390x', 'arm64', 'aarch64']:
            raise Exception("BUG: unsupported architecture (%s)" % arch)

        if re.search("(suse)", self.dname):
            return ["suse11", "suse12", "suse15"]
        elif re.search("(redhat|fedora|centos)", self.dname):
            return ["rhel82", "rhel80", "rhel70", "rhel71", "rhel72", "rhel62", "rhel55", "rhel67"]
        elif self.dname in ['amazon', 'amazon2']:
            return [self.dname]
        elif self.dname == 'ubuntu':
            return [
                "ubuntu1204",
                "ubuntu1404",
                "ubuntu1604",
                "ubuntu1804",
                "ubuntu2004",
            ]
        elif self.dname == 'debian':
            return ["debian81", "debian92", "debian10"]
        else:
            raise Exception("BUG: unsupported platform?")

    def release_dist(self, build_os):
        """Return the release distribution to use in the rpm.

        "el5" for rhel 5.x,
        "el6" for rhel 6.x,
        return anything else unchanged.
        """

        if self.dname == 'amazon':
            return 'amzn1'
        elif self.dname == 'amazon2':
            return 'amzn2'
        return re.sub(r'^rh(el\d).*$', r'\1', build_os)


def get_args(distros, arch_choices):
    """Return the program arguments."""

    distro_choices = []
    for distro in distros:
        for arch in arch_choices:
            distro_choices.extend(distro.build_os(arch))

    parser = argparse.ArgumentParser(description='Build MongoDB Packages')
    parser.add_argument("-s", "--server-version", help="Server version to build (e.g. 2.7.8-rc0)",
                        required=True)
    parser.add_argument("-m", "--metadata-gitspec",
                        help="Gitspec to use for package metadata files", required=False)
    parser.add_argument("-r", "--release-number", help="RPM release number base", type=int,
                        required=False)
    parser.add_argument("-d", "--distros", help="Distros to build for", choices=distro_choices,
                        required=False, default=[], action='append')
    parser.add_argument("-p", "--prefix", help="Directory to build into", required=False)
    parser.add_argument("-a", "--arches", help="Architecture to build", choices=arch_choices,
                        default=[], required=False, action='append')
    parser.add_argument("-t", "--tarball", help="Local tarball to package", required=True,
                        type=lambda x: is_valid_file(parser, x))

    args = parser.parse_args()

    if len(args.distros) * len(args.arches) > 1 and args.tarball:
        parser.error("Can only specify local tarball with one distro/arch combination")

    return args


def main():
    """Execute Main program."""

    distros = [Distro(distro) for distro in DISTROS]

    args = get_args(distros, ARCH_CHOICES)

    spec = Spec(args.server_version, args.metadata_gitspec, args.release_number)

    oldcwd = os.getcwd()
    srcdir = oldcwd + "/../"

    # Where to do all of our work. Use a randomly-created directory if one
    # is not passed in.
    prefix = args.prefix
    if prefix is None:
        prefix = tempfile.mkdtemp()
    print("Working in directory %s" % prefix)

    os.chdir(prefix)
    try:
        # Build a package for each distro/spec/arch tuple, and
        # accumulate the repository-layout directories.
        for (distro, arch) in crossproduct(distros, args.arches):

            for build_os in distro.build_os(arch):
                if build_os in args.distros or not args.distros:

                    filename = tarfile(build_os, arch, spec)
                    ensure_dir(filename)
                    shutil.copyfile(args.tarball, filename)

                    repo = make_package(distro, build_os, arch, spec, srcdir)
                    make_repo(repo, distro, build_os)

    finally:
        os.chdir(oldcwd)


def crossproduct(*seqs):
    """Provide a generator for iterating all the tuples consisting of elements of seqs."""
    num_seqs = len(seqs)
    if num_seqs == 0:
        pass
    elif num_seqs == 1:
        for idx in seqs[0]:
            yield [idx]
    else:
        for lst in crossproduct(*seqs[:-1]):
            for idx in seqs[-1]:
                lst2 = list(lst)
                lst2.append(idx)
                yield lst2


def sysassert(argv):
    """Run argv and assert that it exited with status 0."""
    print("In %s, running %s" % (os.getcwd(), " ".join(argv)))
    sys.stdout.flush()
    sys.stderr.flush()
    assert subprocess.Popen(argv).wait() == 0


def backtick(argv):
    """Run argv and return its output string."""
    print("In %s, running %s" % (os.getcwd(), " ".join(argv)))
    sys.stdout.flush()
    sys.stderr.flush()
    return subprocess.Popen(argv, stdout=subprocess.PIPE).communicate()[0]


def tarfile(build_os, arch, spec):
    """Return the location where we store the downloaded tarball for this package."""
    return "dl/mongodb-linux-%s-%s-%s.tar.gz" % (spec.version(), build_os, arch)


def setupdir(distro, build_os, arch, spec):
    """Return the setup directory name."""
    # The setupdir will be a directory containing all inputs to the
    # distro's packaging tools (e.g., package metadata files, init
    # scripts, etc), along with the already-built binaries).  In case
    # the following format string is unclear, an example setupdir
    # would be dst/x86_64/debian-sysvinit/wheezy/mongodb-org-unstable/
    # or dst/x86_64/redhat/rhel55/mongodb-org-unstable/
    return "dst/%s/%s/%s/%s%s-%s/" % (arch, distro.name(), build_os, distro.pkgbase(),
                                      spec.suffix(), spec.pversion(distro))


def unpack_binaries_into(build_os, arch, spec, where):
    """Unpack the tarfile for (build_os, arch, spec) into directory where."""
    rootdir = os.getcwd()
    ensure_dir(where)
    # Note: POSIX tar doesn't require support for gtar's "-C" option,
    # and Python's tarfile module prior to Python 2.7 doesn't have the
    # features to make this detail easy.  So we'll just do the dumb
    # thing and chdir into where and run tar there.
    os.chdir(where)
    try:
        sysassert(["tar", "xvzf", rootdir + "/" + tarfile(build_os, arch, spec)])
        release_dir = glob('mongodb-linux-*')[0]
        for releasefile in "bin", "LICENSE-Community.txt", "README", "THIRD-PARTY-NOTICES", "MPL-2":
            print("moving file: %s/%s" % (release_dir, releasefile))
            os.rename("%s/%s" % (release_dir, releasefile), releasefile)
        os.rmdir(release_dir)
    except Exception:
        exc = sys.exc_info()[1]
        os.chdir(rootdir)
        raise exc
    os.chdir(rootdir)


def make_package(distro, build_os, arch, spec, srcdir):
    """Construct the package for (arch, distro, spec).

    Get the packaging files from srcdir and any user-specified suffix from suffixes.
    """

    sdir = setupdir(distro, build_os, arch, spec)
    ensure_dir(sdir)
    # Note that the RPM packages get their man pages from the debian
    # directory, so the debian directory is needed in all cases (and
    # innocuous in the debianoids' sdirs).
    for pkgdir in ["debian", "rpm"]:
        print("Copying packaging files from %s to %s" % ("%s/%s" % (srcdir, pkgdir), sdir))
        # FIXME: sh-dash-cee is bad. See if tarfile can do this.
        sysassert([
            "sh", "-c",
            "(cd \"%s\" && tar cf - %s ) | (cd \"%s\" && tar xvf -)" % (srcdir, pkgdir, sdir)
        ])
    # Splat the binaries under sdir.  The "build" stages of the
    # packaging infrastructure will move the files to wherever they
    # need to go.
    unpack_binaries_into(build_os, arch, spec, sdir)

    return distro.make_pkg(build_os, arch, spec, srcdir)


def make_repo(repodir, distro, build_os):
    """Make the repo."""
    if re.search("(debian|ubuntu)", repodir):
        make_deb_repo(repodir, distro, build_os)
    elif re.search("(suse|centos|redhat|fedora|amazon)", repodir):
        make_rpm_repo(repodir)
    else:
        raise Exception("BUG: unsupported platform?")


def make_deb(distro, build_os, arch, spec, srcdir):
    """Make the Debian script."""
    # I can't remember the details anymore, but the initscript/upstart
    # job files' names must match the package name in some way; and
    # see also the --name flag to dh_installinit in the generated
    # debian/rules file.
    suffix = spec.suffix()
    sdir = setupdir(distro, build_os, arch, spec)
    if re.search("debian", distro.name()):
        os.unlink(sdir + "debian/mongod.upstart")
        os.link(sdir + "debian/mongod.service",
                sdir + "debian/%s%s-server.mongod.service" % (distro.pkgbase(), suffix))
        os.unlink(sdir + "debian/init.d")
    elif re.search("ubuntu", distro.name()):
        os.unlink(sdir + "debian/init.d")
        if build_os in ("ubuntu1204", "ubuntu1404", "ubuntu1410"):
            os.link(sdir + "debian/mongod.upstart",
                    sdir + "debian/%s%s-server.mongod.upstart" % (distro.pkgbase(), suffix))
            os.unlink(sdir + "debian/mongod.service")
        else:
            os.link(sdir + "debian/mongod.service",
                    sdir + "debian/%s%s-server.mongod.service" % (distro.pkgbase(), suffix))
            os.unlink(sdir + "debian/mongod.upstart")
    else:
        raise Exception("unknown debianoid flavor: not debian or ubuntu?")
    # Rewrite the control and rules files
    write_debian_changelog(sdir + "debian/changelog", spec, srcdir)
    distro_arch = distro.archname(arch)
    sysassert([
        "cp", "-v", srcdir + "debian/%s%s.control" % (distro.pkgbase(), suffix),
        sdir + "debian/control"
    ])
    sysassert([
        "cp", "-v", srcdir + "debian/%s%s.rules" % (distro.pkgbase(), suffix), sdir + "debian/rules"
    ])

    # old non-server-package postinst will be hanging around for old versions
    #
    if os.path.exists(sdir + "debian/postinst"):
        os.unlink(sdir + "debian/postinst")

    # copy our postinst files
    #
    sysassert(["sh", "-c", "cp -v \"%sdebian/\"*.postinst \"%sdebian/\"" % (srcdir, sdir)])

    with open(sdir + "debian/substvars", "w") as fh:
        # Empty for now. This makes it easier to add substvars to packages
        # later on if we need it.
        fh.write("\n")

    ensure_dir(sdir + "debian/source/format")
    with open(sdir + "debian/source/format", "w") as fh:
        fh.write("1.0\n")

    # Do the packaging.
    oldcwd = os.getcwd()
    try:
        os.chdir(sdir)
        sysassert(["dpkg-buildpackage", "-uc", "-us", "-a" + distro_arch])
    finally:
        os.chdir(oldcwd)
    repo_dir = distro.repodir(arch, build_os, spec)
    ensure_dir(repo_dir)
    # FIXME: see if shutil.copyfile or something can do this without
    # much pain.
    sysassert(["sh", "-c", "cp -v \"%s/../\"*.deb \"%s\"" % (sdir, repo_dir)])
    return repo_dir


def make_deb_repo(repo, distro, build_os):
    """Make the Debian repo."""
    # Note: the Debian repository Packages files must be generated
    # very carefully in order to be usable.
    oldpwd = os.getcwd()
    os.chdir(repo + "../../../../../../")
    try:
        dirs = {
            os.path.dirname(deb)[2:]
            for deb in backtick(["find", ".", "-name", "*.deb"]).decode('utf-8').split()
        }
        for directory in dirs:
            st = backtick(["dpkg-scanpackages", directory, "/dev/null"])
            with open(directory + "/Packages", "wb") as fh:
                fh.write(st)
            bt = backtick(["gzip", "-9c", directory + "/Packages"])
            with open(directory + "/Packages.gz", "wb") as fh:
                fh.write(bt)
    finally:
        os.chdir(oldpwd)
    # Notes: the Release{,.gpg} files must live in a special place,
    # and must be created after all the Packages.gz files have been
    # done.
    s1 = """Origin: mongodb
Label: mongodb
Suite: %s
Codename: %s/mongodb-org
Architectures: amd64 arm64 s390x
Components: %s
Description: MongoDB packages
""" % (distro.repo_os_version(build_os), distro.repo_os_version(build_os), distro.repo_component())
    if os.path.exists(repo + "../../Release"):
        os.unlink(repo + "../../Release")
    if os.path.exists(repo + "../../Release.gpg"):
        os.unlink(repo + "../../Release.gpg")
    oldpwd = os.getcwd()
    os.chdir(repo + "../../")
    s2 = backtick(["apt-ftparchive", "release", "."])
    try:
        with open("Release", 'wb') as fh:
            fh.write(s1.encode('utf-8'))
            fh.write(s2)
    finally:
        os.chdir(oldpwd)


def move_repos_into_place(src, dst):  # pylint: disable=too-many-branches
    """Move the repos into place."""
    # Find all the stuff in src/*, move it to a freshly-created
    # directory beside dst, then play some games with symlinks so that
    # dst is a name the new stuff and dst+".old" names the previous
    # one.  This feels like a lot of hooey for something so trivial.

    # First, make a crispy fresh new directory to put the stuff in.
    i = 0
    while True:
        date_suffix = time.strftime("%Y-%m-%d")
        dname = dst + ".%s.%d" % (date_suffix, i)
        try:
            os.mkdir(dname)
            break
        except OSError:
            exc = sys.exc_info()[1]
            if exc.errno == errno.EEXIST:
                pass
            else:
                raise exc
        i = i + 1

    # Put the stuff in our new directory.
    for src_file in os.listdir(src):
        sysassert(["cp", "-rv", src + "/" + src_file, dname])

    # Make a symlink to the new directory; the symlink will be renamed
    # to dst shortly.
    i = 0
    while True:
        tmpnam = dst + ".TMP.%d" % i
        try:
            os.symlink(dname, tmpnam)
            break
        except OSError:  # as exc: # Python >2.5
            exc = sys.exc_info()[1]
            if exc.errno == errno.EEXIST:
                pass
            else:
                raise exc
        i = i + 1

    # Make a symlink to the old directory; this symlink will be
    # renamed shortly, too.
    oldnam = None
    if os.path.exists(dst):
        i = 0
        while True:
            oldnam = dst + ".old.%d" % i
            try:
                os.symlink(os.readlink(dst), oldnam)
                break
            except OSError:  # as exc: # Python >2.5
                exc = sys.exc_info()[1]
                if exc.errno == errno.EEXIST:
                    pass
                else:
                    raise exc

    os.rename(tmpnam, dst)
    if oldnam:
        os.rename(oldnam, dst + ".old")


def write_debian_changelog(path, spec, srcdir):
    """Write the debian changelog."""
    oldcwd = os.getcwd()
    os.chdir(srcdir)
    preamble = ""
    try:
        sb = preamble + backtick([
            "sh", "-c",
            "git archive %s debian/changelog | tar xOf -" % spec.metadata_gitspec()
        ]).decode('utf-8')
    finally:
        os.chdir(oldcwd)
    lines = sb.split("\n")
    # If the first line starts with "mongodb", it's not a revision
    # preamble, and so frob the version number.
    lines[0] = re.sub("^mongodb \\(.*\\)", "mongodb (%s)" % (spec.pversion(Distro("debian"))),
                      lines[0])
    # Rewrite every changelog entry starting in mongodb<space>
    lines = [re.sub("^mongodb ", "mongodb%s " % (spec.suffix()), line) for line in lines]
    lines = [re.sub("^  --", " --", line) for line in lines]
    sb = "\n".join(lines)
    with open(path, 'w') as fh:
        fh.write(sb)


def make_rpm(distro, build_os, arch, spec, srcdir):  # pylint: disable=too-many-locals
    """Create the RPM specfile."""
    suffix = spec.suffix()
    sdir = setupdir(distro, build_os, arch, spec)

    specfile = srcdir + "rpm/mongodb%s.spec" % suffix
    init_spec = specfile.replace(".spec", "-init.spec")

    # The Debian directory is here for the manpages so we we need to remove the service file
    # from it so that RPM packages don't end up with the Debian file.
    os.unlink(sdir + "debian/mongod.service")

    # Swap out systemd files, different systemd spec files, and init scripts as needed based on
    # underlying os version. Arranged so that new distros moving forward automatically use
    # systemd. Note: the SUSE init packages use a different init script than then other RPM
    # distros.
    #
    if distro.name() == "suse" and distro.repo_os_version(build_os) in ("10", "11"):
        os.unlink(sdir + "rpm/init.d-mongod")
        os.link(sdir + "rpm/init.d-mongod.suse", sdir + "rpm/init.d-mongod")

        os.unlink(specfile)
        os.link(init_spec, specfile)
    elif distro.name() == "redhat" and distro.repo_os_version(build_os) in ("5", "6"):
        os.unlink(specfile)
        os.link(init_spec, specfile)
    elif distro.name() == "amazon":
        os.unlink(specfile)
        os.link(init_spec, specfile)

    topdir = ensure_dir('%s/rpmbuild/%s/' % (os.getcwd(), build_os))
    for subdir in ["BUILD", "RPMS", "SOURCES", "SPECS", "SRPMS"]:
        ensure_dir("%s/%s/" % (topdir, subdir))
    distro_arch = distro.archname(arch)

    # Places the RPM Spec file where it's expected for the rpmbuild execution later.
    shutil.copy(specfile, topdir + "SPECS")

    oldcwd = os.getcwd()
    os.chdir(sdir + "/../")
    try:
        sysassert([
            "tar", "-cpzf",
            topdir + "SOURCES/mongodb%s-%s.tar.gz" % (suffix, spec.pversion(distro)),
            os.path.basename(os.path.dirname(sdir))
        ])
    finally:
        os.chdir(oldcwd)
    # Do the build.

    flags = [
        "-D",
        f"_topdir {topdir}",
        "-D",
        f"dist .{distro.release_dist(build_os)}",
        "-D",
        "_use_internal_dependency_generator 0",
        "-D",
        f"dynamic_version {spec.pversion(distro)}",
        "-D",
        f"dynamic_release {spec.prelease()}",
    ]

    # Versions of RPM after 4.4 ignore our BuildRoot tag so we need to
    # specify it on the command line args to rpmbuild
    if ((distro.name() == "suse" and distro.repo_os_version(build_os) == "15")
            or (distro.name() == "redhat" and distro.repo_os_version(build_os) == "8")):
        flags.extend([
            "--buildroot",
            os.path.join(topdir, "BUILDROOT"),
        ])

    sysassert(["rpmbuild", "-ba", "--target", distro_arch] + flags +
              ["%s/SPECS/mongodb%s.spec" % (topdir, suffix)])
    repo_dir = distro.repodir(arch, build_os, spec)
    ensure_dir(repo_dir)
    # FIXME: see if some combination of shutil.copy<hoohah> and glob
    # can do this without shelling out.
    sysassert(["sh", "-c", "cp -v \"%s/RPMS/%s/\"*.rpm \"%s\"" % (topdir, distro_arch, repo_dir)])
    return repo_dir


def make_rpm_repo(repo):
    """Make the RPM repo."""
    oldpwd = os.getcwd()
    os.chdir(repo + "../")
    try:
        sysassert(["createrepo", "."])
    finally:
        os.chdir(oldpwd)


def ensure_dir(filename):
    """Ensure that the dirname directory of filename exists, and return filename."""
    dirpart = os.path.dirname(filename)
    try:
        os.makedirs(dirpart)
    except OSError:  # as exc: # Python >2.5
        exc = sys.exc_info()[1]
        if exc.errno == errno.EEXIST:
            pass
        else:
            raise exc
    return filename


def is_valid_file(parser, filename):
    """Check if file exists, and return the filename."""
    if not os.path.exists(filename):
        parser.error("The file %s does not exist!" % filename)
        return None
    return filename


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Packager Enterprise module."""

# This program makes Debian and RPM repositories for MongoDB, by
# downloading our tarballs of statically linked executables and
# insinuating them into Linux packages.  It must be run on a
# Debianoid, since Debian provides tools to make RPMs, but RPM-based
# systems don't provide debian packaging crud.

# Notes:
#
# * Almost anything that you want to be able to influence about how a
# package construction must be embedded in some file that the
# packaging tool uses for input (e.g., debian/rules, debian/control,
# debian/changelog; or the RPM specfile), and the precise details are
# arbitrary and silly.  So this program generates all the relevant
# inputs to the packaging tools.
#
# * Once a .deb or .rpm package is made, there's a separate layer of
# tools that makes a "repository" for use by the apt/yum layers of
# package tools.  The layouts of these repositories are arbitrary and
# silly, too.
#
# * Before you run the program on a new host, these are the
# prerequisites:
#
# apt-get install dpkg-dev rpm debhelper fakeroot ia32-libs createrepo git-core libsnmp15
# echo "Now put the dist gnupg signing keys in ~root/.gnupg"

import errno
from glob import glob
import os
import re
import shutil
import sys
import tempfile
import time

sys.path.append(os.getcwd())

import packager  # pylint: disable=wrong-import-position

# The MongoDB names for the architectures we support.
ARCH_CHOICES = ["x86_64", "ppc64le", "s390x", "arm64", "aarch64"]

# Made up names for the flavors of distribution we package for.
DISTROS = ["suse", "debian", "redhat", "ubuntu", "amazon", "amazon2"]


class EnterpriseSpec(packager.Spec):
    """EnterpriseSpec class."""

    def suffix(self):
        """Suffix."""
        if int(self.ver.split(".")[0]) >= 5:
            return "-enterprise" if int(self.ver.split(".")[1]) == 0 else "-enterprise-unstable"
        else:
            return "-enterprise" if int(self.ver.split(".")[1]) % 2 == 0 else "-enterprise-unstable"


class EnterpriseDistro(packager.Distro):
    """EnterpriseDistro class."""

    def repodir(self, arch, build_os, spec):  # noqa: D406,D407,D412,D413
        """Return the directory where we'll place the package files.

        This is for (distro, distro_version) in that distro's preferred repository
        layout (as distinct from where that distro's packaging building
        tools place the package files).

        Packages will go into repos corresponding to the major release
        series (2.5, 2.6, 2.7, 2.8, etc.) except for RC's and nightlies
        which will go into special separate "testing" directories

        Examples:

        repo/apt/ubuntu/dists/precise/mongodb-enterprise/testing/multiverse/binary-amd64
        repo/apt/ubuntu/dists/precise/mongodb-enterprise/testing/multiverse/binary-i386

        repo/apt/ubuntu/dists/precise/mongodb-enterprise/2.5/multiverse/binary-amd64
        repo/apt/ubuntu/dists/precise/mongodb-enterprise/2.5/multiverse/binary-i386

        repo/apt/ubuntu/dists/trusty/mongodb-enterprise/2.5/multiverse/binary-amd64
        repo/apt/ubuntu/dists/trusty/mongodb-enterprise/2.5/multiverse/binary-i386

        repo/apt/debian/dists/wheezy/mongodb-enterprise/2.5/main/binary-amd64
        repo/apt/debian/dists/wheezy/mongodb-enterprise/2.5/main/binary-i386

        repo/yum/redhat/6/mongodb-enterprise/2.5/x86_64
        repo/yum/redhat/6/mongodb-enterprise/2.5/i386

        repo/zypper/suse/11/mongodb-enterprise/2.5/x86_64
        repo/zypper/suse/11/mongodb-enterprise/2.5/i386

        repo/zypper/suse/11/mongodb-enterprise/testing/x86_64
        repo/zypper/suse/11/mongodb-enterprise/testing/i386
        """

        repo_directory = ""

        if spec.is_pre_release():
            repo_directory = "testing"
        else:
            repo_directory = spec.branch()

        if re.search("^(debian|ubuntu)", self.dname):
            return "repo/apt/%s/dists/%s/mongodb-enterprise/%s/%s/binary-%s/" % (
                self.dname, self.repo_os_version(build_os), repo_directory, self.repo_component(),
                self.archname(arch))
        elif re.search("(redhat|fedora|centos|amazon)", self.dname):
            return "repo/yum/%s/%s/mongodb-enterprise/%s/%s/RPMS/" % (
                self.dname, self.repo_os_version(build_os), repo_directory, self.archname(arch))
        elif re.search("(suse)", self.dname):
            return "repo/zypper/%s/%s/mongodb-enterprise/%s/%s/RPMS/" % (
                self.dname, self.repo_os_version(build_os), repo_directory, self.archname(arch))
        else:
            raise Exception("BUG: unsupported platform?")

    def build_os(self, arch):  # pylint: disable=too-many-branches
        """Return the build os label in the binary package to download.

        The labels "rhel57", "rhel62", "rhel67", "rhel70", "rhel80" are for redhat,
        the others are delegated to the super class.
        """
        # pylint: disable=too-many-return-statements
        if arch == "ppc64le":
            if self.dname == 'ubuntu':
                return ["ubuntu1604", "ubuntu1804"]
            if self.dname == 'redhat':
                return ["rhel71", "rhel81"]
            return []
        if arch == "s390x":
            if self.dname == 'redhat':
                return ["rhel67", "rhel72"]
            if self.dname == 'suse':
                return ["suse11", "suse12", "suse15"]
            if self.dname == 'ubuntu':
                return ["ubuntu1604", "ubuntu1804"]
            return []
        if arch == "arm64":
            if self.dname == 'ubuntu':
                return ["ubuntu1804", "ubuntu2004"]
        if arch == "aarch64":
            if self.dname == 'redhat':
                return ["rhel82"]
            if self.dname == 'amazon2':
                return ["amazon2"]
            return []

        if re.search("(redhat|fedora|centos)", self.dname):
            return ["rhel80", "rhel70", "rhel62", "rhel57"]
        return super(EnterpriseDistro, self).build_os(arch)
        # pylint: enable=too-many-return-statements


def main():
    """Execute Main program."""

    distros = [EnterpriseDistro(distro) for distro in DISTROS]

    args = packager.get_args(distros, ARCH_CHOICES)

    spec = EnterpriseSpec(args.server_version, args.metadata_gitspec, args.release_number)

    oldcwd = os.getcwd()
    srcdir = oldcwd + "/../"

    # Where to do all of our work. Use a randomly-created directory if one
    # is not passed in.
    prefix = args.prefix
    if prefix is None:
        prefix = tempfile.mkdtemp()

    print("Working in directory %s" % prefix)

    os.chdir(prefix)
    try:
        made_pkg = False
        # Build a package for each distro/spec/arch tuple, and
        # accumulate the repository-layout directories.
        for (distro, arch) in packager.crossproduct(distros, args.arches):

            for build_os in distro.build_os(arch):
                if build_os in args.distros or not args.distros:

                    filename = tarfile(build_os, arch, spec)
                    packager.ensure_dir(filename)
                    shutil.copyfile(args.tarball, filename)

                    repo = make_package(distro, build_os, arch, spec, srcdir)
                    make_repo(repo, distro, build_os)

                    made_pkg = True

        if not made_pkg:
            raise Exception("No valid combination of distro and arch selected")

    finally:
        os.chdir(oldcwd)


def tarfile(build_os, arch, spec):
    """Return the location where we store the downloaded tarball for this package."""
    return "dl/mongodb-linux-%s-enterprise-%s-%s.tar.gz" % (spec.version(), build_os, arch)


def setupdir(distro, build_os, arch, spec):
    """Return the setup directory name."""
    # The setupdir will be a directory containing all inputs to the
    # distro's packaging tools (e.g., package metadata files, init
    # scripts, etc, along with the already-built binaries).  In case
    # the following format string is unclear, an example setupdir
    # would be dst/x86_64/debian-sysvinit/wheezy/mongodb-org-unstable/
    # or dst/x86_64/redhat/rhel57/mongodb-org-unstable/
    return "dst/%s/%s/%s/%s%s-%s/" % (arch, distro.name(), build_os, distro.pkgbase(),
                                      spec.suffix(), spec.pversion(distro))


def unpack_binaries_into(build_os, arch, spec, where):
    """Unpack the tarfile for (build_os, arch, spec) into directory where."""
    rootdir = os.getcwd()
    packager.ensure_dir(where)
    # Note: POSIX tar doesn't require support for gtar's "-C" option,
    # and Python's tarfile module prior to Python 2.7 doesn't have the
    # features to make this detail easy.  So we'll just do the dumb
    # thing and chdir into where and run tar there.
    os.chdir(where)
    try:
        packager.sysassert(["tar", "xvzf", rootdir + "/" + tarfile(build_os, arch, spec)])
        release_dir = glob('mongodb-linux-*')[0]
        for releasefile in "bin", "snmp", "LICENSE-Enterprise.txt", "README", "THIRD-PARTY-NOTICES", "MPL-2":
            os.rename("%s/%s" % (release_dir, releasefile), releasefile)
        os.rmdir(release_dir)
    except Exception:
        exc = sys.exc_info()[1]
        os.chdir(rootdir)
        raise exc
    os.chdir(rootdir)


def make_package(distro, build_os, arch, spec, srcdir):
    """Construct the package for (arch, distro, spec).

    Get the packaging files from srcdir and any user-specified suffix from suffixes.
    """

    sdir = setupdir(distro, build_os, arch, spec)
    packager.ensure_dir(sdir)
    # Note that the RPM packages get their man pages from the debian
    # directory, so the debian directory is needed in all cases (and
    # innocuous in the debianoids' sdirs).
    for pkgdir in ["debian", "rpm"]:
        print("Copying packaging files from %s to %s" % ("%s/%s" % (srcdir, pkgdir), sdir))
        # FIXME: sh-dash-cee is bad. See if tarfile can do this.
        packager.sysassert([
            "sh", "-c",
            "(cd \"%s\" && git archive %s %s/ ) | (cd \"%s\" && tar xvf -)" %
            (srcdir, spec.metadata_gitspec(), pkgdir, sdir)
        ])
    # Splat the binaries and snmp files under sdir.  The "build" stages of the
    # packaging infrastructure will move the files to wherever they
    # need to go.
    unpack_binaries_into(build_os, arch, spec, sdir)

    return distro.make_pkg(build_os, arch, spec, srcdir)


def make_repo(repodir, distro, build_os):
    """Make the repo."""
    if re.search("(debian|ubuntu)", repodir):
        make_deb_repo(repodir, distro, build_os)
    elif re.search("(suse|centos|redhat|fedora|amazon)", repodir):
        packager.make_rpm_repo(repodir)
    else:
        raise Exception("BUG: unsupported platform?")


def make_deb_repo(repo, distro, build_os):
    """Make the Debian repo."""
    # Note: the Debian repository Packages files must be generated
    # very carefully in order to be usable.
    oldpwd = os.getcwd()
    os.chdir(repo + "../../../../../../")
    try:
        dirs = {
            os.path.dirname(deb)[2:]
            for deb in packager.backtick(["find", ".", "-name", "*.deb"]).decode('utf-8').split()
        }
        for directory in dirs:
            st = packager.backtick(["dpkg-scanpackages", directory, "/dev/null"])
            with open(directory + "/Packages", "wb") as fh:
                fh.write(st)
            bt = packager.backtick(["gzip", "-9c", directory + "/Packages"])
            with open(directory + "/Packages.gz", "wb") as fh:
                fh.write(bt)
    finally:
        os.chdir(oldpwd)
    # Notes: the Release{,.gpg} files must live in a special place,
    # and must be created after all the Packages.gz files have been
    # done.
    s1 = """Origin: mongodb
Label: mongodb
Suite: %s
Codename: %s/mongodb-enterprise
Architectures: amd64 ppc64el s390x arm64
Components: %s
Description: MongoDB packages
""" % (distro.repo_os_version(build_os), distro.repo_os_version(build_os), distro.repo_component())
    if os.path.exists(repo + "../../Release"):
        os.unlink(repo + "../../Release")
    if os.path.exists(repo + "../../Release.gpg"):
        os.unlink(repo + "../../Release.gpg")
    oldpwd = os.getcwd()
    os.chdir(repo + "../../")
    s2 = packager.backtick(["apt-ftparchive", "release", "."])
    try:
        with open("Release", 'wb') as fh:
            fh.write(s1.encode('utf-8'))
            fh.write(s2)
    finally:
        os.chdir(oldpwd)


def move_repos_into_place(src, dst):  # pylint: disable=too-many-branches
    """Move the repos into place."""
    # Find all the stuff in src/*, move it to a freshly-created
    # directory beside dst, then play some games with symlinks so that
    # dst is a name the new stuff and dst+".old" names the previous
    # one.  This feels like a lot of hooey for something so trivial.

    # First, make a crispy fresh new directory to put the stuff in.
    idx = 0
    while True:
        date_suffix = time.strftime("%Y-%m-%d")
        dname = dst + ".%s.%d" % (date_suffix, idx)
        try:
            os.mkdir(dname)
            break
        except OSError:
            exc = sys.exc_info()[1]
            if exc.errno == errno.EEXIST:
                pass
            else:
                raise exc
        idx = idx + 1

    # Put the stuff in our new directory.
    for src_file in os.listdir(src):
        packager.sysassert(["cp", "-rv", src + "/" + src_file, dname])

    # Make a symlink to the new directory; the symlink will be renamed
    # to dst shortly.
    idx = 0
    while True:
        tmpnam = dst + ".TMP.%d" % idx
        try:
            os.symlink(dname, tmpnam)
            break
        except OSError:  # as exc: # Python >2.5
            exc = sys.exc_info()[1]
            if exc.errno == errno.EEXIST:
                pass
            else:
                raise exc
        idx = idx + 1

    # Make a symlink to the old directory; this symlink will be
    # renamed shortly, too.
    oldnam = None
    if os.path.exists(dst):
        idx = 0
        while True:
            oldnam = dst + ".old.%d" % idx
            try:
                os.symlink(os.readlink(dst), oldnam)
                break
            except OSError:  # as exc: # Python >2.5
                exc = sys.exc_info()[1]
                if exc.errno == errno.EEXIST:
                    pass
                else:
                    raise exc

    os.rename(tmpnam, dst)
    if oldnam:
        os.rename(oldnam, dst + ".old")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Command line utility for executing operations on remote hosts."""

import os.path
import sys

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pylint: disable=wrong-import-position
import buildscripts.powercycle_setup.cli as cli

cli.main(sys.argv)

#!/usr/bin/env python3
"""Powercycle tasks sentinel.

Error out when any powercycle task on the same buildvariant runs for more than 2 hours.
"""
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import List

import click
import structlog
from evergreen import RetryingEvergreenApi, EvergreenApi

from buildscripts.util.read_config import read_config_file

LOGGER = structlog.getLogger(__name__)

EVERGREEN_HOST = "https://evergreen.mongodb.com"
EVERGREEN_CONFIG_LOCATIONS = (
    # Common for machines in Evergreen
    os.path.join(os.getcwd(), ".evergreen.yml"),
    # Common for local machines
    os.path.expanduser(os.path.join("~", ".evergreen.yml")),
)
POWERCYCLE_TASK_EXEC_TIMEOUT_SECS = 2 * 60 * 60
WATCH_INTERVAL_SECS = 5 * 60


def get_evergreen_api() -> EvergreenApi:
    """Return evergreen API."""
    # Pickup the first config file found in common locations.
    for file in EVERGREEN_CONFIG_LOCATIONS:
        if os.path.isfile(file):
            evg_api = RetryingEvergreenApi.get_api(config_file=file)
            return evg_api

    LOGGER.error("Evergreen config not found in locations.", locations=EVERGREEN_CONFIG_LOCATIONS)
    sys.exit(1)


def watch_tasks(task_ids: List[str], evg_api: EvergreenApi, watch_interval_secs: int) -> List[str]:
    """Watch tasks if they run longer than exec timeout."""
    watch_task_ids = task_ids[:]
    long_running_task_ids = []

    while watch_task_ids:
        LOGGER.info("Looking if powercycle tasks are still running on the current buildvariant.")
        powercycle_tasks = [evg_api.task_by_id(task_id) for task_id in watch_task_ids]
        for task in powercycle_tasks:
            if task.finish_time:
                watch_task_ids.remove(task.task_id)
            elif task.start_time and (datetime.now(timezone.utc) - task.start_time
                                      ).total_seconds() > POWERCYCLE_TASK_EXEC_TIMEOUT_SECS:
                long_running_task_ids.append(task.task_id)
                watch_task_ids.remove(task.task_id)
        if watch_task_ids:
            time.sleep(watch_interval_secs)

    return long_running_task_ids


def get_links(task_ids: List[str]) -> str:
    """Return evergreen task urls delimited by newline."""
    return "\n".join([f"{EVERGREEN_HOST}/task/{task_id}" for task_id in task_ids])


@click.command()
@click.argument("expansions_file", type=str, default="expansions.yml")
def main(expansions_file: str = "expansions.yml") -> None:
    """Implementation."""

    logging.basicConfig(
        format="[%(levelname)s] %(message)s",
        level=logging.INFO,
        stream=sys.stdout,
    )
    structlog.configure(logger_factory=structlog.stdlib.LoggerFactory())

    expansions = read_config_file(expansions_file)
    build_id = expansions["build_id"]
    current_task_id = expansions["task_id"]
    gen_task_name = expansions["gen_task"]

    evg_api = get_evergreen_api()

    build_tasks = evg_api.tasks_by_build(build_id)
    gen_task_id = [task.task_id for task in build_tasks if gen_task_name in task.task_id][0]
    gen_task_url = f"{EVERGREEN_HOST}/task/{gen_task_id}"

    while evg_api.task_by_id(gen_task_id).is_active():
        LOGGER.info(
            f"Waiting for '{gen_task_name}' task to generate powercycle tasks:\n{gen_task_url}")
        time.sleep(WATCH_INTERVAL_SECS)

    build_tasks = evg_api.tasks_by_build(build_id)
    powercycle_task_ids = [
        task.task_id for task in build_tasks
        if not task.display_only and task.task_id != current_task_id and task.task_id != gen_task_id
        and "powercycle" in task.task_id
    ]
    LOGGER.info(f"Watching powercycle tasks:\n{get_links(powercycle_task_ids)}")

    long_running_task_ids = watch_tasks(powercycle_task_ids, evg_api, WATCH_INTERVAL_SECS)
    if long_running_task_ids:
        LOGGER.error(
            f"Found powercycle tasks that are running for more than {POWERCYCLE_TASK_EXEC_TIMEOUT_SECS} "
            f"seconds and most likely something is going wrong in those tasks:\n{get_links(long_running_task_ids)}"
        )
        LOGGER.error(
            "Hopefully hosts from the tasks are still in run at the time you are seeing this "
            "and the Build team is able to check them to diagnose the issue.")
        sys.exit(1)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Convert silent test failures into non-silent failures.

Any test files with at least 2 executions in the report.json file that have a "silentfail" status,
this script will change the outputted report to have a "fail" status instead.
"""

import collections
import json
import optparse
import os
import sys

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from buildscripts.resmokelib.testing import report


def read_json_file(json_file):
    """Return contents of a JSON file."""
    with open(json_file) as json_data:
        return json.load(json_data)


def main():
    """Execute Main program."""

    usage = "usage: %prog [options] report.json"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option(
        "-o", "--output-file", dest="outfile", default="-",
        help=("If '-', then the report file is written to stdout."
              " Any other value is treated as the output file name. By default,"
              " output is written to stdout."))

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.error("Requires a single report.json file.")

    report_file_json = read_json_file(args[0])
    test_report = report.TestReport.from_dict(report_file_json)

    # Count number of "silentfail" per test file.
    status_dict = collections.defaultdict(int)
    for test_info in test_report.test_infos:
        if test_info.evergreen_status == "silentfail":
            status_dict[test_info.test_id] += 1

    # For test files with more than 1 "silentfail", convert status to "fail".
    for test_info in test_report.test_infos:
        if status_dict[test_info.test_id] >= 2:
            test_info.evergreen_status = "fail"

    result_report = test_report.as_dict()
    if options.outfile != "-":
        with open(options.outfile, "w") as fp:
            json.dump(result_report, fp)
    else:
        print(json.dumps(result_report))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Extensible script to run one or more Python Linters across a subset of files in parallel."""

import argparse
import logging
import os
import sys
from typing import Dict, List

import structlog

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.realpath(__file__)))))

# pylint: disable=wrong-import-position
from buildscripts.linter.filediff import gather_changed_files_for_lint
from buildscripts.linter import base
from buildscripts.linter import git
from buildscripts.linter import mypy
from buildscripts.linter import parallel
from buildscripts.linter import pydocstyle
from buildscripts.linter import pylint
from buildscripts.linter import runner
from buildscripts.linter import yapf
# pylint: enable=wrong-import-position

# List of supported linters
_LINTERS = [
    yapf.YapfLinter(),
    pylint.PyLintLinter(),
    pydocstyle.PyDocstyleLinter(),
    mypy.MypyLinter(),
]


def get_py_linter(linter_filter):
    # type: (str) -> List[base.LinterBase]
    """
    Get a list of linters to use.

    'all' or None - select all linters
    'a,b,c' - a comma delimited list is describes a list of linters to choose
    """
    if linter_filter is None or linter_filter == "all":
        return _LINTERS

    linter_list = linter_filter.split(",")

    linter_candidates = [linter for linter in _LINTERS if linter.cmd_name in linter_list]

    if not linter_candidates:
        raise ValueError("No linters found for filter '%s'" % (linter_filter))

    return linter_candidates


def is_interesting_file(file_name):
    # type: (str) -> bool
    """Return true if this file should be checked."""
    file_denylist = []  # type: List[str]
    directory_denylist = ["src/third_party"]
    if file_name in file_denylist or file_name.startswith(tuple(directory_denylist)):
        return False
    directory_list = ["buildscripts", "pytests"]
    return file_name.endswith(".py") and file_name.startswith(tuple(directory_list))


def _lint_files(linters, config_dict, file_names):
    # type: (str, Dict[str, str], List[str]) -> None
    """Lint a list of files with clang-format."""
    linter_list = get_py_linter(linters)

    lint_runner = runner.LintRunner()

    linter_instances = runner.find_linters(linter_list, config_dict)
    if not linter_instances:
        sys.exit(1)

    failed_lint = False

    for linter in linter_instances:
        run_fix = lambda param1: lint_runner.run_lint(linter, param1)  # pylint: disable=cell-var-from-loop
        lint_clean = parallel.parallel_process([os.path.abspath(f) for f in file_names], run_fix)

        if not lint_clean:
            failed_lint = True

    if failed_lint:
        print("ERROR: Code Style does not match coding style")
        sys.exit(1)


def lint_patch(linters, config_dict, file_name):
    # type: (str, Dict[str, str], List[str]) -> None
    """Lint patch command entry point."""
    file_names = git.get_files_to_check_from_patch(file_name, is_interesting_file)

    # Patch may have files that we do not want to check which is fine
    if file_names:
        _lint_files(linters, config_dict, file_names)


def lint_git_diff(linters, config_dict, _):
    # type: (str, Dict[str, str], List[str]) -> None
    """Lint git diff command entry point."""
    file_names = gather_changed_files_for_lint(is_interesting_file)

    # Patch may have files that we do not want to check which is fine
    if file_names:
        _lint_files(linters, config_dict, file_names)


def lint(linters, config_dict, file_names):
    # type: (str, Dict[str, str], List[str]) -> None
    """Lint files command entry point."""
    all_file_names = git.get_files_to_check(file_names, is_interesting_file)

    _lint_files(linters, config_dict, all_file_names)


def lint_all(linters, config_dict, file_names):
    # type: (str, Dict[str, str], List[str]) -> None
    # pylint: disable=unused-argument
    """Lint files command entry point based on working tree."""
    all_file_names = git.get_files_to_check_working_tree(is_interesting_file)

    _lint_files(linters, config_dict, all_file_names)


def _fix_files(linters, config_dict, file_names):
    # type: (str, Dict[str, str], List[str]) -> None
    """Fix a list of files with linters if possible."""
    linter_list = get_py_linter(linters)

    # Get a list of linters which return a valid command for get_fix_cmd()
    fix_list = [fixer for fixer in linter_list if fixer.get_fix_cmd_args("ignore")]

    if not fix_list:
        raise ValueError("Cannot find any linters '%s' that support fixing." % (linters))

    lint_runner = runner.LintRunner()

    linter_instances = runner.find_linters(fix_list, config_dict)
    if not linter_instances:
        sys.exit(1)

    for linter in linter_instances:
        run_linter = lambda param1: lint_runner.run(linter.cmd_path + linter.linter.  # pylint: disable=cell-var-from-loop
                                                    get_fix_cmd_args(param1))  # pylint: disable=cell-var-from-loop

        lint_clean = parallel.parallel_process([os.path.abspath(f) for f in file_names], run_linter)

        if not lint_clean:
            print("ERROR: Code Style does not match coding style")
            sys.exit(1)


def fix_func(linters, config_dict, file_names):
    # type: (str, Dict[str, str], List[str]) -> None
    """Fix files command entry point."""
    all_file_names = git.get_files_to_check(file_names, is_interesting_file)

    _fix_files(linters, config_dict, all_file_names)


def main():
    # type: () -> None
    """Execute Main entry point."""

    parser = argparse.ArgumentParser(description='PyLinter frontend.')

    linters = get_py_linter(None)

    dest_prefix = "linter_"
    for linter1 in linters:
        msg = 'Path to linter %s' % (linter1.cmd_name)
        parser.add_argument('--' + linter1.cmd_name, type=str, help=msg,
                            dest=dest_prefix + linter1.cmd_name)

    parser.add_argument('--linters', type=str,
                        help="Comma separated list of filters to use, defaults to 'all'",
                        default="all")

    parser.add_argument('-v', "--verbose", action='store_true', help="Enable verbose logging")

    sub = parser.add_subparsers(title="Linter subcommands", help="sub-command help")

    parser_lint = sub.add_parser('lint', help='Lint only Git files')
    parser_lint.add_argument("file_names", nargs="*", help="Globs of files to check")
    parser_lint.set_defaults(func=lint)

    parser_lint_all = sub.add_parser('lint-all', help='Lint All files')
    parser_lint_all.add_argument("file_names", nargs="*", help="Globs of files to check")
    parser_lint_all.set_defaults(func=lint_all)

    parser_lint_patch = sub.add_parser('lint-patch', help='Lint the files in a patch')
    parser_lint_patch.add_argument("file_names", nargs="*", help="Globs of files to check")
    parser_lint_patch.set_defaults(func=lint_patch)

    parser_lint_patch = sub.add_parser('lint-git-diff',
                                       help='Lint the files since the last git commit')
    parser_lint_patch.add_argument("file_names", nargs="*", help="Globs of files to check")
    parser_lint_patch.set_defaults(func=lint_git_diff)

    parser_fix = sub.add_parser('fix', help='Fix files if possible')
    parser_fix.add_argument("file_names", nargs="*", help="Globs of files to check")
    parser_fix.set_defaults(func=fix_func)

    args = parser.parse_args()

    # Create a dictionary of linter locations if the user needs to override the location of a
    # linter. This is common for mypy on Windows for instance.
    config_dict = {}
    for key in args.__dict__:
        if key.startswith("linter_"):
            name = key.replace(dest_prefix, "")
            config_dict[name] = args.__dict__[key]

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    structlog.configure(logger_factory=structlog.stdlib.LoggerFactory())

    args.func(args.linters, config_dict, args.file_names)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Extensible script to run one or more simple C++ Linters across a subset of files in parallel."""

import argparse
import logging
import os
import re
import sys
import threading
from typing import List

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.realpath(__file__)))))

from buildscripts.linter import git  # pylint: disable=wrong-import-position
from buildscripts.linter import parallel  # pylint: disable=wrong-import-position
from buildscripts.linter import simplecpplint  # pylint: disable=wrong-import-position

FILES_RE = re.compile('\\.(h|cpp)$')


def is_interesting_file(file_name: str) -> bool:
    """Return true if this file should be checked."""
    return (file_name.startswith("jstests")
            or file_name.startswith("src") and not file_name.startswith("src/third_party/")
            and not file_name.startswith("src/mongo/gotools/")
            # TODO SERVER-49805: These files should be generated at compile time.
            and not file_name == "src/mongo/db/cst/parser_gen.cpp") and FILES_RE.search(file_name)


def _lint_files(file_names: List[str]) -> None:
    """Lint a list of files with clang-format."""
    run_lint1 = lambda param1: simplecpplint.lint_file(param1) == 0
    if not parallel.parallel_process([os.path.abspath(f) for f in file_names], run_lint1):
        print("ERROR: Code Style does not match coding style")
        sys.exit(1)


def lint_patch(file_name: str) -> None:
    """Lint patch command entry point."""
    file_names = git.get_files_to_check_from_patch(file_name, is_interesting_file)

    # Patch may have files that we do not want to check which is fine
    if file_names:
        _lint_files(file_names)


def lint(file_names: List[str]) -> None:
    # type: (str, Dict[str, str], List[str]) -> None
    """Lint files command entry point."""
    all_file_names = git.get_files_to_check(file_names, is_interesting_file)

    _lint_files(all_file_names)


def lint_all(file_names: List[str]) -> None:
    # pylint: disable=unused-argument
    """Lint files command entry point based on working tree."""
    all_file_names = git.get_files_to_check_working_tree(is_interesting_file)

    _lint_files(all_file_names)


def lint_my(origin_branch: List[str]) -> None:
    # pylint: disable=unused-argument
    """Lint files command based on local changes."""
    files = git.get_my_files_to_check(is_interesting_file, origin_branch)
    files = [f for f in files if os.path.exists(f)]

    _lint_files(files)


def main() -> None:
    """Execute Main entry point."""

    parser = argparse.ArgumentParser(description='Quick C++ Lint frontend.')

    parser.add_argument('-v', "--verbose", action='store_true', help="Enable verbose logging")

    sub = parser.add_subparsers(title="Linter subcommands", help="sub-command help")

    parser_lint = sub.add_parser('lint', help='Lint only Git files')
    parser_lint.add_argument("file_names", nargs="*", help="Globs of files to check")
    parser_lint.set_defaults(func=lint)

    parser_lint_all = sub.add_parser('lint-all', help='Lint All files')
    parser_lint_all.add_argument("file_names", nargs="*", help="Globs of files to check")
    parser_lint_all.set_defaults(func=lint_all)

    parser_lint_patch = sub.add_parser('lint-patch', help='Lint the files in a patch')
    parser_lint_patch.add_argument("file_names", nargs="*", help="Globs of files to check")
    parser_lint_patch.set_defaults(func=lint_patch)

    parser_lint_my = sub.add_parser('lint-my', help='Lint my files')
    parser_lint_my.add_argument("--branch", dest="file_names", default="origin/master",
                                help="Branch to compare against")
    parser_lint_my.set_defaults(func=lint_my)

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    args.func(args.file_names)


if __name__ == "__main__":
    main()

# This file is for the convenience of users who previously relied on buildscripts/requirements.txt.
# For future use, please use etc/pip/dev-requirements.txt instead.
-r ../etc/pip/dev-requirements.txt

[resmoke]
install_dir = @install_dir@
#!/usr/bin/env python3
"""Command line utility for executing MongoDB tests of all kinds."""

import os.path
import sys

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pylint: disable=wrong-import-position
import buildscripts.resmokelib.cli as cli

cli.main(sys.argv)

#!/usr/bin/env python3
"""Scons module."""

import os
import sys

SCONS_VERSION = os.environ.get('SCONS_VERSION', "3.1.2")

MONGODB_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
SCONS_DIR = os.path.join(MONGODB_ROOT, 'src', 'third_party', 'scons-' + SCONS_VERSION,
                         'scons-local-' + SCONS_VERSION)

if not os.path.exists(SCONS_DIR):
    print("Could not find SCons in '%s'" % (SCONS_DIR))
    sys.exit(1)

SITE_TOOLS_DIR = os.path.join(MONGODB_ROOT, 'site_scons')

sys.path = [SCONS_DIR, SITE_TOOLS_DIR] + sys.path

# pylint: disable=C0413
from mongo.pip_requirements import verify_requirements, MissingRequirements

try:
    verify_requirements('etc/pip/compile-requirements.txt')
except MissingRequirements as ex:
    print(ex)
    sys.exit(1)

try:
    import SCons.Script
except ImportError as import_err:
    print("Could not import SCons from '%s'" % (SCONS_DIR))
    print("ImportError:", import_err)
    sys.exit(1)

if __name__ == '__main__':
    SCons.Script.main()

#!/USSR/bin/python
# encoding: utf-8
"""
Prune the scons cache.

This script, borrowed from some waf code, with a stand alone interface, provides a way to
remove files from the cache on an LRU (least recently used) basis to prevent the scons cache
from outgrowing the storage capacity.
"""

# Inspired by: https://github.com/krig/waf/blob/master/waflib/extras/lru_cache.py
# Thomas Nagy 2011

import argparse
import collections
import logging
import os
import shutil

LOGGER = logging.getLogger("scons.cache.prune.lru")  # type: ignore

GIGBYTES = 1024 * 1024 * 1024

CacheItem = collections.namedtuple("CacheContents", ["path", "time", "size"])


def get_cachefile_size(file_path):
    """Get the size of the cachefile."""

    if file_path.lower().endswith('.cksum') or file_path.lower().endswith('.cksum.del'):
        size = 0
        for cksum_path in os.listdir(file_path):
            cksum_path = os.path.join(file_path, cksum_path)
            size += os.stat(cksum_path).st_size
    else:
        size = os.stat(file_path).st_size
    return size


def collect_cache_contents(cache_path):
    """Collect the cache contents."""
    # map folder names to timestamps
    contents = []
    total = 0

    # collect names of directories and creation times
    for name in os.listdir(cache_path):
        path = os.path.join(cache_path, name)

        if os.path.isdir(path):
            for file_name in os.listdir(path):
                file_path = os.path.join(path, file_name)
                # Cache prune script is allowing only directories with this extension
                # which comes from the validate_cache_dir.py tool in scons, it must match
                # the extension set in that file.
                if os.path.isdir(file_path) and not file_path.lower().endswith(
                        '.cksum') and not file_path.lower().endswith('.cksum.del'):
                    LOGGER.warning(
                        "cache item %s is a directory and not a file. "
                        "The cache may be corrupt.", file_path)
                    continue

                try:

                    item = CacheItem(path=file_path, time=os.stat(file_path).st_atime,
                                     size=get_cachefile_size(file_path))

                    total += item.size

                    contents.append(item)
                except OSError as err:
                    LOGGER.warning("Ignoring error querying file %s : %s", file_path, err)

    return (total, contents)


def prune_cache(cache_path, cache_size_gb, clean_ratio):
    """Prune the cache."""
    # This function is taken as is from waf, with the interface cleaned up and some minor
    # stylistic changes.

    cache_size = cache_size_gb * GIGBYTES

    (total_size, contents) = collect_cache_contents(cache_path)

    LOGGER.info("cache size %d, quota %d", total_size, cache_size)

    if total_size >= cache_size:
        LOGGER.info("trimming the cache since %d > %d", total_size, cache_size)

        # make a list to sort the folders' by timestamp
        contents.sort(key=lambda x: x.time, reverse=True)  # sort by timestamp

        # now that the contents of things to delete is sorted by timestamp in reverse order, we
        # just delete things until the total_size falls below the target cache size ratio.
        while total_size >= cache_size * clean_ratio:
            if not contents:
                LOGGER.error("cache size is over quota, and there are no files in "
                             "the queue to delete.")
                return False

            cache_item = contents.pop()
            to_remove = cache_item.path + ".del"
            try:
                os.rename(cache_item.path, to_remove)
            except Exception as err:  # pylint: disable=broad-except
                # another process may have already cleared the file.
                LOGGER.warning("Unable to rename %s : %s", cache_item, err)
            else:
                try:
                    if os.path.isdir(to_remove):
                        shutil.rmtree(to_remove)
                    else:
                        os.remove(to_remove)
                    total_size -= cache_item.size
                except Exception as err:  # pylint: disable=broad-except
                    # this should not happen, but who knows?
                    LOGGER.error("error [%s, %s] removing file '%s', "
                                 "please report this error", err, type(err), to_remove)

        LOGGER.info("total cache size at the end of pruning: %d", total_size)
        return True
    LOGGER.info("cache size (%d) is currently within boundaries", total_size)
    return True


def main():
    """Execute Main entry."""

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="SCons cache pruning tool")

    parser.add_argument("--cache-dir", "-d", default=None, help="path to the cache directory.")
    parser.add_argument("--cache-size", "-s", default=200, type=int,
                        help="maximum size of cache in GB.")
    parser.add_argument(
        "--prune-ratio", "-p", default=0.8, type=float,
        help=("ratio (as 1.0 > x > 0) of total cache size to prune "
              "to when cache exceeds quota."))
    parser.add_argument("--print-cache-dir", default=False, action="store_true")

    args = parser.parse_args()

    if args.cache_dir is None or not os.path.isdir(args.cache_dir):
        LOGGER.error("must specify a valid cache path, [%s]", args.cache_dir)
        exit(1)

    ok = prune_cache(cache_path=args.cache_dir, cache_size_gb=args.cache_size,
                     clean_ratio=args.prune_ratio)

    if not ok:
        LOGGER.error("encountered error cleaning the cache. exiting.")
        exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Command line utility for determining what jstests should run for the given changed files."""
import os
import re
import sys
from datetime import datetime, timedelta
from functools import partial
from typing import Any, Dict, List, Set, Optional

import click
import inject
import structlog
from pydantic import BaseModel
from structlog.stdlib import LoggerFactory
from git import Repo
from evergreen.api import EvergreenApi, RetryingEvergreenApi

if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pylint: disable=wrong-import-position
# Get relative imports to work when the package is not installed on the PYTHONPATH.
from buildscripts.patch_builds.change_data import find_changed_files_in_repos
from buildscripts.patch_builds.evg_change_data import generate_revision_map_from_manifest
from buildscripts.patch_builds.selected_tests.selected_tests_client import SelectedTestsClient
from buildscripts.task_generation.evg_config_builder import EvgConfigBuilder
from buildscripts.task_generation.gen_config import GenerationConfiguration
from buildscripts.task_generation.generated_config import GeneratedConfiguration
from buildscripts.task_generation.resmoke_proxy import ResmokeProxyConfig
from buildscripts.task_generation.suite_split import SuiteSplitParameters, SuiteSplitConfig
from buildscripts.task_generation.suite_split_strategies import SplitStrategy, FallbackStrategy, \
    greedy_division, round_robin_fallback
from buildscripts.task_generation.task_types.gentask_options import GenTaskOptions
from buildscripts.task_generation.task_types.resmoke_tasks import ResmokeGenTaskParams
from buildscripts.util.cmdutils import enable_logging
from buildscripts.util.fileops import read_yaml_file
from buildscripts.burn_in_tests import DEFAULT_REPO_LOCATIONS, create_task_list_for_tests, \
    TaskInfo
from buildscripts.ciconfig.evergreen import (
    EvergreenProjectConfig,
    ResmokeArgs,
    Task,
    parse_evergreen_file,
    Variant,
)
from buildscripts.patch_builds.selected_tests.selected_tests_service import SelectedTestsService

structlog.configure(logger_factory=LoggerFactory())
LOGGER = structlog.getLogger(__name__)

DEFAULT_TEST_SUITE_DIR = os.path.join("buildscripts", "resmokeconfig", "suites")
TASK_ID_EXPANSION = "task_id"
EVERGREEN_FILE = "etc/evergreen.yml"
EVG_CONFIG_FILE = ".evergreen.yml"
SELECTED_TESTS_CONFIG_DIR = "generated_resmoke_config"
RELATION_THRESHOLD = 0
LOOKBACK_DURATION_DAYS = 14

COMPILE_TASK_PATTERN = re.compile(".*compile.*")
CONCURRENCY_TASK_PATTERN = re.compile("concurrency.*")
INTEGRATION_TASK_PATTERN = re.compile("integration.*")
FUZZER_TASK_PATTERN = re.compile(".*fuzz.*")
GENERATE_TASK_PATTERN = re.compile("burn_in.*")
MULTIVERSION_TASK_PATTERN = re.compile(".*multiversion.*")
LINT_TASK_PATTERN = re.compile("lint.*")
STITCH_TASK_PATTERN = re.compile("stitch.*")
EXCLUDE_TASK_PATTERNS = [
    COMPILE_TASK_PATTERN, CONCURRENCY_TASK_PATTERN, INTEGRATION_TASK_PATTERN, FUZZER_TASK_PATTERN,
    GENERATE_TASK_PATTERN, LINT_TASK_PATTERN, MULTIVERSION_TASK_PATTERN, STITCH_TASK_PATTERN
]

CPP_TASK_NAMES = [
    "dbtest",
    "idl_tests",
    "run_unittests",
]
PUBLISH_TASK_NAMES = [
    "package",
    "publish_packages",
    "push",
]
PYTHON_TESTS = ["buildscripts_test"]
EXCLUDE_TASK_LIST = [
    *CPP_TASK_NAMES,
    *PYTHON_TESTS,
    *PUBLISH_TASK_NAMES,
]
POSSIBLE_RUN_TASK_FUNCS = [
    "generate resmoke tasks",
    "generate randomized multiversion tasks",
    "run tests",
    "generate explicit multiversion tasks",
]


class EvgExpansions(BaseModel):
    """
    Evergreen expansions needed for selected tests.

    task_id: ID of task being run.
    task_name: Name of task being run.
    build_variant: Name of build variant being run on.
    build_id: ID of build being run.
    is_patch: Is this task run as part of a patch build.
    project: Evergreen project being run.
    revision: git revision being run against.
    version_id: ID of version being run.
    """

    task_id: str
    task_name: str
    build_variant: str
    build_id: str
    is_patch: Optional[bool] = None
    project: str
    revision: str
    version_id: str

    @classmethod
    def from_yaml_file(cls, path: str) -> "EvgExpansions":
        """Read the generation configuration from the given file."""
        return cls(**read_yaml_file(path))

    def build_gen_task_options(self) -> GenTaskOptions:
        """Build options needed to generate tasks."""
        return GenTaskOptions(create_misc_suite=False,
                              generated_config_dir=SELECTED_TESTS_CONFIG_DIR, is_patch=self.is_patch
                              or False, use_default_timeouts=False)

    def build_suite_split_config(self, start_date: datetime,
                                 end_date: datetime) -> SuiteSplitConfig:
        """
        Build options need to split suite into sub-suites.

        :param start_date: Start date to look at historic results.
        :param end_date: End date to look at historic results.
        :return: Options for splitting suites.
        """
        return SuiteSplitConfig(
            evg_project=self.project,
            target_resmoke_time=60,
            max_sub_suites=5,
            max_tests_per_suite=100,
            start_date=start_date,
            end_date=end_date,
            include_build_variant_in_name=True,
        )

    def get_config_location(self) -> str:
        """Get the location the generated configuration will be stored."""
        return f"{self.build_variant}/{self.revision}/generate_tasks/{self.task_name}-{self.build_id}.tgz"


class TaskConfigService:
    """Service for generating selected tests task configuration."""

    @staticmethod
    def get_evg_task_config(task: Task, build_variant_config: Variant) -> Dict[str, Any]:
        """
        Look up task config of the task to be generated.

        :param task: Task to get info for.
        :param build_variant_config: Config of build variant to collect task info from.
        :return: Task configuration values.
        """
        LOGGER.info("Calculating evg_task_config values for task", task=task.name)
        task_vars = {}
        for run_task_func in POSSIBLE_RUN_TASK_FUNCS:
            task_def = task.find_func_command(run_task_func)
            if task_def:
                task_vars = task_def["vars"]
                break

        suite_name = ResmokeArgs.get_arg(task_vars["resmoke_args"], "suites")
        if suite_name:
            task_vars.update({"suite": suite_name})

        # the suites argument will run all tests in a suite even when individual
        # tests are specified in resmoke_args, so we remove it
        resmoke_args_without_suites = ResmokeArgs.remove_arg(task_vars["resmoke_args"], "suites")
        task_vars["resmoke_args"] = resmoke_args_without_suites

        task_name = task.name[:-4] if task.name.endswith("_gen") else task.name
        return {
            "task_name": task_name,
            "build_variant": build_variant_config.name,
            **task_vars,
            "large_distro_name": build_variant_config.expansion("large_distro_name"),
        }

    def get_task_configs_for_test_mappings(self, tests_by_task: Dict[str, TaskInfo],
                                           build_variant_config: Variant) -> Dict[str, dict]:
        """
        For test mappings, generate a dict containing task names and their config settings.

        :param tests_by_task: Dictionary of tests and tasks to run.
        :param build_variant_config: Config of build variant to collect task info from.
        :return: Dict of task names and their config settings.
        """
        evg_task_configs = {}
        for task_name, test_list_info in tests_by_task.items():
            task = _find_task(build_variant_config, task_name)
            if task and not _exclude_task(task):
                evg_task_config = self.get_evg_task_config(task, build_variant_config)
                evg_task_config.update({"selected_tests_to_run": set(test_list_info.tests)})
                evg_task_configs[task.name] = evg_task_config

        return evg_task_configs

    def get_task_configs_for_task_mappings(self, related_tasks: List[str],
                                           build_variant_config: Variant) -> Dict[str, dict]:
        """
        For task mappings, generate a dict containing task names and their config settings.

        :param related_tasks: List of tasks to run.
        :param build_variant_config: Config of build variant to collect task info from.
        :return: Dict of task names and their config settings.
        """
        evg_task_configs = {}
        for task_name in related_tasks:
            task = _find_task(build_variant_config, task_name)
            if task and not _exclude_task(task):
                evg_task_config = self.get_evg_task_config(task, build_variant_config)
                evg_task_configs[task.name] = evg_task_config

        return evg_task_configs


def _exclude_task(task: Task) -> bool:
    """
    Check whether a task should be excluded.

    :param task: Task to get info for.
    :return: True if this task should be excluded.
    """
    if task.name in EXCLUDE_TASK_LIST or any(
            regex.match(task.name) for regex in EXCLUDE_TASK_PATTERNS):
        LOGGER.debug("Excluding task from analysis because it is not a jstest", task=task.name)
        return True
    return False


def _find_task(build_variant_config: Variant, task_name: str) -> Task:
    """
    Look up shrub config for task.

    :param build_variant_config: Config of build variant to collect task info from.
    :param task_name: Name of task to get info for.
    :return: Task configuration.
    """
    task = build_variant_config.get_task(task_name)
    if not task:
        task = build_variant_config.get_task(task_name + "_gen")
    return task


def _remove_repo_path_prefix(file_path: str) -> str:
    """
    Remove the repo path prefix from the filepath.

    :param file_path: Path of the changed file.
    :return: Path of the changed file without prefix.
    """
    for repo_path in DEFAULT_REPO_LOCATIONS:
        if repo_path != ".":
            if repo_path.startswith("./"):
                repo_path = repo_path[2:]
                file_path = re.sub(repo_path + "/", '', file_path)
    return file_path


def filter_set(item: str, input_set: Set[str]) -> bool:
    """
    Filter to determine if the given item is in the given set.

    :param item: Item to search for.
    :param input_set: Set to search.
    :return: True if the item is contained in the list.
    """
    return item in input_set


class SelectedTestsOrchestrator:
    """Orchestrator for generating selected test builds."""

    # pylint: disable=too-many-arguments
    @inject.autoparams()
    def __init__(self, evg_api: EvergreenApi, evg_conf: EvergreenProjectConfig,
                 selected_tests_service: SelectedTestsService,
                 task_config_service: TaskConfigService, evg_expansions: EvgExpansions) -> None:
        """
        Initialize the orchestrator.

        :param evg_api: Evergreen API client.
        :param evg_conf: Evergreen Project configuration.
        :param selected_tests_service: Selected tests service.
        :param task_config_service: Task Config service.
        :param evg_expansions: Evergreen expansions.
        """
        self.evg_api = evg_api
        self.evg_conf = evg_conf
        self.selected_tests_service = selected_tests_service
        self.task_config_service = task_config_service
        self.evg_expansions = evg_expansions

    def find_changed_files(self, repos: List[Repo], task_id: str) -> Set[str]:
        """
        Determine what files have changed in the given repos.

        :param repos: List of git repos to query.
        :param task_id: ID of task being run.
        :return: Set of files that contain changes.
        """
        revision_map = generate_revision_map_from_manifest(repos, task_id, self.evg_api)
        changed_files = find_changed_files_in_repos(repos, revision_map)
        changed_files = {_remove_repo_path_prefix(file_path) for file_path in changed_files}
        changed_files = {
            file_path
            for file_path in changed_files if not file_path.startswith("src/third_party")
        }
        LOGGER.info("Found changed files", files=changed_files)
        return changed_files

    def get_task_config(self, build_variant_config: Variant,
                        changed_files: Set[str]) -> Dict[str, Dict]:
        """
        Get task configurations for the tasks to be generated.

        :param build_variant_config: Config of build variant to collect task info from.
        :param changed_files: Set of changed_files.
        :return: Task configurations.
        """
        existing_tasks = self.get_existing_tasks(self.evg_expansions.version_id,
                                                 build_variant_config.name)
        task_configs = {}

        related_test_files = self.selected_tests_service.find_selected_test_files(changed_files)
        LOGGER.info("related test files found", related_test_files=related_test_files,
                    variant=build_variant_config.name)

        if related_test_files:
            tests_by_task = create_task_list_for_tests(related_test_files,
                                                       build_variant_config.name, self.evg_conf)
            LOGGER.info("tests and tasks found", tests_by_task=tests_by_task)
            tests_by_task = {
                task: tests
                for task, tests in tests_by_task.items() if task not in existing_tasks
            }

            test_mapping_task_configs = self.task_config_service.get_task_configs_for_test_mappings(
                tests_by_task, build_variant_config)
            task_configs.update(test_mapping_task_configs)

        related_tasks = self.selected_tests_service.find_selected_tasks(changed_files)
        LOGGER.info("related tasks found", related_tasks=related_tasks,
                    variant=build_variant_config.name)
        related_tasks = {task for task in related_tasks if task not in existing_tasks}
        if related_tasks:
            task_mapping_task_configs = self.task_config_service.get_task_configs_for_task_mappings(
                list(related_tasks), build_variant_config)
            # task_mapping_task_configs will overwrite test_mapping_task_configs
            # because task_mapping_task_configs will run all tests rather than a subset of tests
            # and we should err on the side of running all tests
            task_configs.update(task_mapping_task_configs)

        return task_configs

    def get_existing_tasks(self, version_id: str, build_variant: str) -> Set[str]:
        """
        Get the set of tasks that already exist in the given build.

        :param version_id: ID of version to query.
        :param build_variant: Name of build variant to query.
        :return: Set of task names that already exist in the specified build.
        """
        version = self.evg_api.version_by_id(version_id)

        try:
            build = version.build_by_variant(build_variant)
        except KeyError:
            LOGGER.debug("No build exists on this build variant for this version yet",
                         variant=build_variant)
            return set()

        if build:
            tasks_already_in_build = build.get_tasks()
            return {task.display_name for task in tasks_already_in_build}

        return set()

    def generate_build_variant(self, build_variant_config: Variant, changed_files: Set[str],
                               builder: EvgConfigBuilder) -> None:
        """
        Generate the selected tasks on the specified build variant.

        :param build_variant_config: Configuration of build variant to generate.
        :param changed_files: List of file changes to determine what to run.
        :param builder: Builder to create new configuration.
        """
        build_variant_name = build_variant_config.name
        LOGGER.info("Generating build variant", build_variant=build_variant_name)
        task_configs = self.get_task_config(build_variant_config, changed_files)

        for task_config in task_configs.values():
            test_filter = None
            if "selected_tests_to_run" in task_config:
                test_filter = partial(filter_set, input_set=task_config["selected_tests_to_run"])
            split_params = SuiteSplitParameters(
                build_variant=build_variant_name,
                task_name=task_config["task_name"],
                suite_name=task_config.get("suite", task_config["task_name"]),
                filename=task_config.get("suite", task_config["task_name"]),
                test_file_filter=test_filter,
                is_asan=build_variant_config.is_asan_build(),
            )
            gen_params = ResmokeGenTaskParams(
                use_large_distro=task_config.get("use_large_distro", False),
                large_distro_name=task_config.get("large_distro_name"),
                require_multiversion=task_config.get("require_multiversion"),
                repeat_suites=task_config.get("repeat_suites", 1),
                resmoke_args=task_config["resmoke_args"],
                resmoke_jobs_max=task_config.get("resmoke_jobs_max"),
                config_location=self.evg_expansions.get_config_location(),
            )
            builder.generate_suite(split_params, gen_params)

    def generate(self, repos: List[Repo], task_id: str) -> None:
        """
        Build and generate the configuration to create selected tests.

        :param repos: List of git repos containing changes to check.
        :param task_id: ID of task being run.
        """
        changed_files = self.find_changed_files(repos, task_id)
        generated_config = self.generate_version(changed_files)
        generated_config.write_all_to_dir(SELECTED_TESTS_CONFIG_DIR)

    def generate_version(self, changed_files: Set[str]) -> GeneratedConfiguration:
        """
        Generate selected tests configuration for the given file changes.

        :param changed_files: Set of files that contain changes.
        :return: Configuration to generate selected-tests tasks.
        """
        builder = EvgConfigBuilder()  # pylint: disable=no-value-for-parameter
        for build_variant_config in self.evg_conf.get_required_variants():
            self.generate_build_variant(build_variant_config, changed_files, builder)

        return builder.build("selected_tests_config.json")


@click.command()
@click.option("--verbose", "verbose", default=False, is_flag=True, help="Enable extra logging.")
@click.option(
    "--expansion-file",
    "expansion_file",
    type=str,
    required=True,
    help="Location of expansions file generated by evergreen.",
)
@click.option(
    "--evg-api-config",
    "evg_api_config",
    default=EVG_CONFIG_FILE,
    metavar="FILE",
    help="Configuration file with connection info for Evergreen API.",
)
@click.option(
    "--selected-tests-config",
    "selected_tests_config",
    required=True,
    metavar="FILE",
    help="Configuration file with connection info for selected tests service.",
)
def main(
        verbose: bool,
        expansion_file: str,
        evg_api_config: str,
        selected_tests_config: str,
):
    """
    Select tasks to be run based on changed files in a patch build.

    :param verbose: Log extra debug information.
    :param expansion_file: Configuration file.
    :param evg_api_config: Location of configuration file to connect to evergreen.
    :param selected_tests_config: Location of config file to connect to elected-tests service.
    """
    enable_logging(verbose)

    end_date = datetime.utcnow().replace(microsecond=0)
    start_date = end_date - timedelta(days=LOOKBACK_DURATION_DAYS)

    evg_expansions = EvgExpansions.from_yaml_file(expansion_file)

    def dependencies(binder: inject.Binder) -> None:
        binder.bind(EvgExpansions, evg_expansions)
        binder.bind(EvergreenApi, RetryingEvergreenApi.get_api(config_file=evg_api_config))
        binder.bind(EvergreenProjectConfig, parse_evergreen_file(EVERGREEN_FILE))
        binder.bind(SelectedTestsClient, SelectedTestsClient.from_file(selected_tests_config))
        binder.bind(SuiteSplitConfig, evg_expansions.build_suite_split_config(start_date, end_date))
        binder.bind(SplitStrategy, greedy_division)
        binder.bind(FallbackStrategy, round_robin_fallback)
        binder.bind(GenTaskOptions, evg_expansions.build_gen_task_options())
        binder.bind(GenerationConfiguration, GenerationConfiguration.from_yaml_file())
        binder.bind(ResmokeProxyConfig,
                    ResmokeProxyConfig(resmoke_suite_dir=DEFAULT_TEST_SUITE_DIR))

    inject.configure(dependencies)

    repos = [Repo(x) for x in DEFAULT_REPO_LOCATIONS if os.path.isdir(x)]
    selected_tests = SelectedTestsOrchestrator()  # pylint: disable=no-value-for-parameter
    selected_tests.generate(repos, evg_expansions.task_id)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

#!/usr/bin/env python3
"""Stub file pointing users to the new invocation."""

if __name__ == "__main__":
    print(
        "Hello! It seems you've executed 'buildscripts/setup_multiversion_mongodb.py'. We have\n"
        "repackaged the setup multiversion as a subcommand of resmoke. It can now be invoked as\n"
        "'./buildscripts/resmoke.py setup-multiversion' with all of the same arguments as before.\n"
        "Please use './buildscripts/resmoke.py setup-multiversion --help' for more details.")

#!/bin/bash

cd $HOME # workaround EVG-12829

# Communicate to users that logged in before the script started that nothing is ready.
wall "The setup_spawnhost_coredump script has just started setting up the debugging environment."

# Write this file that gets cat'ed on login to communicate to users logging in if this setup script is still running.
echo '+-----------------------------------------------------------------+' > ~/.setup_spawnhost_coredump_progress
echo '| The setup script is still setting up data files for inspection. |' >> ~/.setup_spawnhost_coredump_progress
echo '+-----------------------------------------------------------------+' >> ~/.setup_spawnhost_coredump_progress

cat >> ~/.profile <<EOF
cat ~/.setup_spawnhost_coredump_progress
# Coredumps generated by a toolchain built mongodb can be problematic when examined with the system
# gdb.
export PATH=/opt/mongodbtoolchain/gdb/bin:$PATH
# As per below, put the user into the appropriate directory. This is where gdb is expected to be
# invoked from.
cd debug
echo "Debuggable binaries:"
ls -l mongo* | grep -v debug$
for item in "mongo" "mongod" "mongos"; do
    echo "\${item} core dumps:"
    ls -l dump_\${item}.*
done

echo "Core dumps from unknown processes (crashed processes typically found here):"
ls -l dump_* | grep -v mongo

echo
echo "To examine a core dump, type 'gdb ./<binary> ./<core file>'"
cat ~/.setup_spawnhost_coredump_progress
EOF

export PATH=/opt/mongodbtoolchain/gdb/bin:$PATH
echo 'if [ -f ~/.profile ]; then
    . ~/.profile
fi' >> .bash_profile

# Make a directory on the larger EBS volume. Soft-link it under the home directory. The smaller home
# volume can have trouble particularly with coredumps from sharded timeouts.
mkdir /data/debug
ln -s /data/debug .
cd debug

# As the name suggests, pretty printers. Primarily for boost::optional<T>
git clone git@github.com:ruediger/Boost-Pretty-Printer.git &

# Discover and unarchive necessary files and source code. This will put mongo binaries and their
# partner .debug files in the same top-level (`debug`) directory. Shared library files and their
# debug symbols will be dumped into a `debug/lib` directory for tidiness. The mongo
# `<reporoot>/src/` directory is soft linked as `debug/src`. The .gdbinit file assumes gdb is being
# run from the `debug` directory.
BIN_ARCHIVE=`ls /data/mci/artifacts-*archive_dist_test*/mongo-*.tgz`
tar --wildcards --strip-components=2 -xzf $BIN_ARCHIVE '*/bin/mongod' '*/bin/mongos' '*/bin/mongo' '*/bin/mongobridge' &
tar --wildcards --strip-components=1 -xzf $BIN_ARCHIVE '*/lib/*' &
DBG_ARCHIVE=`ls /data/mci/artifacts-*archive_dist_test_debug/debugsymbols-*.tgz`
tar --wildcards --strip-components=2 -xzf $DBG_ARCHIVE '*/bin/mongod.debug' '*/bin/mongos.debug' '*/bin/mongo.debug' '*/bin/mongobridge.debug' &
tar --wildcards --strip-components=1 -xzf $DBG_ARCHIVE '*/lib/*' &
UNITTEST_ARCHIVE=`ls /data/mci/artifacts-*run_unittests/mongo-unittests-*.tgz`
tar --wildcards --strip-components=1 -xzf $UNITTEST_ARCHIVE 'bin/*' &
tar --wildcards -xzf $UNITTEST_ARCHIVE 'lib/*' &

SRC_DIR=`find /data/mci/ -maxdepth 1 | grep source`
ln -s $SRC_DIR/.gdbinit .
ln -s $SRC_DIR/src src
ln -s $SRC_DIR/buildscripts buildscripts

# Install pymongo to get the bson library for pretty-printers.
/opt/mongodbtoolchain/v3/bin/pip3 install -r $SRC_DIR/etc/pip/dev-requirements.txt &

COREDUMP_ARCHIVE=`ls /data/mci/artifacts-*/mongo-coredumps-*.tgz`
tar -xzf $COREDUMP_ARCHIVE &
echo "Waiting for background processes to complete."
wait

cat >> ~/.gdbinit <<EOF
set auto-load safe-path /
set solib-search-path ./lib/
set pagination off
set print object on
set print static-members off
set print pretty on

python
import sys
sys.path.insert(0, './Boost-Pretty-Printer')
import boost
boost.register_printers(boost_version=(1,70,0))
end
EOF

echo "dir $HOME/debug" >> ~/.gdbinit

# Empty out the progress script that warns users about the set script still running when users log in.
echo "" > ~/.setup_spawnhost_coredump_progress
# Alert currently logged in users that this setup script has completed. Logging back in will ensure any
# paths/environment variables will be set as intended.
wall "The setup_spawnhost_coredump script has completed, please relogin to ensure the right environment variables are set."

#!/bin/bash
set +o errexit

if ! command -v shfmt &>/dev/null; then
  echo "Could not find the 'shfmt' command"
  echo ""
  echo "Install via"
  echo ""
  echo "    brew install shfmt"
  echo ""
  exit 1
fi

lint_dirs="evergreen"

if [ "$1" = "fix" ]; then
  shfmt -w -i 2 -bn -sr "$lint_dirs"
fi

output_file="shfmt_output.txt"
exit_code=0

shfmt -d -i 2 -bn -sr "$lint_dirs" >"$output_file"
if [ -s "$output_file" ]; then
  echo "ERROR: Found formatting errors in shell script files in directories: $lint_dirs"
  echo ""
  cat "$output_file"
  echo ""
  echo "To fix formatting errors run"
  echo ""
  echo "    ./buildscripts/shellscripts-linters.sh fix"
  echo ""
  exit_code=1
fi
rm -rf "$output_file"

exit "$exit_code"

#!/usr/bin/env python3
"""Check for TODOs in the source code."""
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Callable, Optional, NamedTuple, Dict, List

import click
from evergreen import RetryingEvergreenApi

EVG_CONFIG_FILE = "./.evergreen.yml"
BASE_SEARCH_DIR = "."
IGNORED_PATHS = [".git"]
ISSUE_RE = re.compile('(BUILD|SERVER|WT|PM|TOOLS|TIG|PERF|BF)-[0-9]+')


class Todo(NamedTuple):
    """
    A TODO comment found the in the code.

    file_name: Name of file comment was found in.
    line_number: Line number comment was found in.
    line: Content of line comment was found in.
    ticket: Jira ticket associated with comment.
    """

    file_name: str
    line_number: int
    line: str
    ticket: Optional[str] = None

    @classmethod
    def from_line(cls, file_name: str, line_number: int, line: str) -> "Todo":
        """
        Create a found todo from the given line of code.

        :param file_name: File name comment was found in.
        :param line_number: Line number comment was found in.
        :param line: Content of line.
        :return: Todo representation of found comment.
        """
        issue_key = cls.get_issue_key_from_line(line)
        return cls(file_name, line_number, line.strip(), issue_key)

    @staticmethod
    def get_issue_key_from_line(line: str) -> Optional[str]:
        """
        Check if the given line appears to reference a jira ticket.

        :param line: Content of line.
        :return: Jira ticket if one was found.
        """
        match = ISSUE_RE.search(line.upper())
        if match:
            return match.group(0)
        return None


@dataclass
class FoundTodos:
    """
    Collection of TODO comments found in the code base.

    no_tickets: TODO comments found without any Jira ticket references.
    with_tickets: Dictionary of Jira tickets references with a list of references.
    by_file: All the references found mapped by the files they were found in.
    """

    no_tickets: List[Todo]
    with_tickets: Dict[str, List[Todo]]
    by_file: Dict[str, List[Todo]]


class TodoChecker:
    """A tool to find and track TODO references."""

    def __init__(self) -> None:
        """Initialize a new TODO checker."""
        self.found_todos = FoundTodos(no_tickets=[], with_tickets=defaultdict(list),
                                      by_file=defaultdict(list))

    def check_file(self, file_name: str, file_contents: Iterable[str]) -> None:
        """
        Check the given file for TODO references.

        Any TODOs will be added to `found_todos`.

        :param file_name: Name of file being checked.
        :param file_contents: Iterable of the file contents.
        """
        for i, line in enumerate(file_contents):
            if "todo" in line.lower():
                todo = Todo.from_line(file_name, i + 1, line)
                if todo.ticket is not None:
                    self.found_todos.with_tickets[todo.ticket].append(todo)
                else:
                    self.found_todos.no_tickets.append(todo)
                self.found_todos.by_file[file_name].append(todo)

    def check_all_files(self, base_dir: str) -> None:
        """
        Check all files under the base directory for TODO references.

        :param base_dir: Base directory to start searching.
        """
        walk_fs(base_dir, self.check_file)

    @staticmethod
    def print_todo_list(todo_list: List[Todo]) -> None:
        """Display all the TODOs in the given list."""
        last_file = None
        for todo in todo_list:
            if last_file != todo.file_name:
                print(f"{todo.file_name}")
            print(f"\t{todo.line_number}: {todo.line}")

    def report_on_ticket(self, ticket: str) -> bool:
        """
        Report on any TODOs found referencing the given ticket.

        Any found references will be printed to stdout.

        :param ticket: Jira ticket to search for.
        :return: True if any TODOs were found.
        """
        todo_list = self.found_todos.with_tickets.get(ticket)
        if todo_list:
            print(f"{ticket}")
            self.print_todo_list(todo_list)
            return True
        return False

    def report_on_all_tickets(self) -> bool:
        """
        Report on all TODOs found that reference a Jira ticket.

        Any found references will be printed to stdout.

        :return: True if any TODOs were found.
        """
        if not self.found_todos.with_tickets:
            return False

        for ticket in self.found_todos.with_tickets.keys():
            self.report_on_ticket(ticket)

        return True

    def validate_commit_queue(self, commit_message: str) -> bool:
        """
        Check that the given commit message does not reference TODO comments.

        :param commit_message: Commit message to check.
        :return: True if any TODOs were found.
        """
        print("*" * 80)
        print("Checking for TODOs associated with Jira key in commit message.")
        if "revert" in commit_message.lower():
            print("Skipping checks since it looks like this is a revert.")
            # Reverts are a special case and we shouldn't fail them.
            return False

        found_any = False
        ticket = Todo.get_issue_key_from_line(commit_message)
        while ticket:
            found_any = self.report_on_ticket(ticket) or found_any
            rest_index = commit_message.find(ticket)
            commit_message = commit_message[rest_index + len(ticket):]
            ticket = Todo.get_issue_key_from_line(commit_message)

        print(f"Checking complete - todos found: {found_any}")
        print("*" * 80)
        return found_any


def walk_fs(root: str, action: Callable[[str, Iterable[str]], None]) -> None:
    """
    Walk the file system and perform the given action on each file.

    :param root: Base to start walking the filesystem.
    :param action: Action to perform on each file.
    """
    for base, _, files in os.walk(root):
        for file_name in files:
            try:
                file_path = os.path.join(base, file_name)
                if any(ignore in file_path for ignore in IGNORED_PATHS):
                    continue

                with open(file_path) as search_file:
                    action(file_path, search_file)
            except UnicodeDecodeError:
                # If we try to read any non-text files.
                continue


def get_summary_for_patch(version_id: str) -> str:
    """
    Get the description provided for the given patch build.

    :param version_id: Version ID of the patch build to query.
    :return: Description provided for the patch build.
    """
    evg_api = RetryingEvergreenApi.get_api(config_file=EVG_CONFIG_FILE)
    return evg_api.version_by_id(version_id).message


@click.command()
@click.option("--ticket", help="Only report on TODOs associated with given Jira ticket.")
@click.option("--base-dir", default=BASE_SEARCH_DIR, help="Base directory to search in.")
@click.option("--commit-message",
              help="For commit-queue execution only, ensure no TODOs for this commit")
@click.option("--patch-build", type=str,
              help="For patch build execution only, check for any TODOs from patch description")
def main(ticket: Optional[str], base_dir: str, commit_message: Optional[str],
         patch_build: Optional[str]):
    """
    Search for and report on TODO comments in the code base.

    Based on the arguments given, there are two types of searches you can perform:

    \b
    * By default, search for all TODO comments that reference any Jira ticket.
    * Search for references to a specific Jira ticket with the `--ticket` option.

    \b
    Examples
    --------

        \b
        Search all TODO comments with Jira references:
        ```
        > python buildscripts/todo_check.py
        SERVER-12345
        ./src/mongo/db/file.cpp
            140: // TODO: SERVER-12345: Need to fix this.
            183: // TODO: SERVER-12345: Need to fix this as well.
        SERVER-54321
        ./src/mongo/db/other_file.h
            728: // TODO: SERVER-54321 an update is needed here
        ```

        \b
        Search for any TODO references to a given ticket.
        ```
        > python buildscripts/todo_check.py --ticket SERVER-1234
        SERVER-1234
        ./src/mongo/db/file.cpp
            140: // TODO: SERVER-1234: Need to fix this.
        ```

    \b
    Exit Code
    ---------
        In any mode, if any TODO comments are found a non-zero exit code will be returned.
    \f
    :param ticket: Only report on TODOs associated with this jira ticket.
    :param base_dir: Search files in this base directory.
    :param commit_message: Commit message if running in the commit-queue.
    :param patch_build: Version ID of patch build to check.

    """
    if commit_message and ticket is not None:
        raise click.UsageError("--ticket cannot be used in commit queue.")

    if patch_build:
        if ticket is not None:
            raise click.UsageError("--ticket cannot be used in patch builds.")
        commit_message = get_summary_for_patch(patch_build)

    todo_checker = TodoChecker()
    todo_checker.check_all_files(base_dir)

    if commit_message:
        found_todos = todo_checker.validate_commit_queue(commit_message)
    elif ticket:
        found_todos = todo_checker.report_on_ticket(ticket)
    else:
        found_todos = todo_checker.report_on_all_tickets()

    if found_todos:
        print("TODOs that reference a Jira ticket associated with the current commit should not "
              "remain in the code after the commit is merged. A TODO referencing a ticket that has "
              "been closed and that solved the TODO's purpose can be confusing.")
        print("To fix this error resolve any TODOs that reference the Jira ticket this commit is"
              "associated with.")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

"""Various utilities that are handy."""

import codecs
import re
import os
import os.path
import subprocess
import sys


def get_git_branch():
    """Return the git branch version."""
    if not os.path.exists(".git") or not os.path.isdir(".git"):
        return None

    version = open(".git/HEAD", "r").read().strip()
    if not version.startswith("ref: "):
        return version
    version = version.split("/")
    version = version[len(version) - 1]
    return version


def get_git_branch_string(prefix="", postfix=""):
    """Return the git branch name."""
    tt = re.compile(r"[/\\]").split(os.getcwd())
    if len(tt) > 2 and tt[len(tt) - 1] == "mongo":
        par = tt[len(tt) - 2]
        mt = re.compile(r".*_([vV]\d+\.\d+)$").match(par)
        if mt is not None:
            return prefix + mt.group(1).lower() + postfix
        if par.find("Nightly") > 0:
            return ""

    branch = get_git_branch()
    if branch is None or branch == "master":
        return ""
    return prefix + branch + postfix


def get_git_version():
    """Return the git version."""
    if not os.path.exists(".git") or not os.path.isdir(".git"):
        return "nogitversion"

    version = open(".git/HEAD", "r").read().strip()
    if not version.startswith("ref: "):
        return version
    version = version[5:]
    git_ver = ".git/" + version
    if not os.path.exists(git_ver):
        return version
    return open(git_ver, "r").read().strip()


def get_git_describe():
    """Return 'git describe --abbrev=7'."""
    with open(os.devnull, "r+") as devnull:
        proc = subprocess.Popen("git describe --abbrev=7", stdout=subprocess.PIPE, stderr=devnull,
                                stdin=devnull, shell=True)
        return proc.communicate()[0].strip().decode('utf-8')


def execsys(args):
    """Execute a subprocess of 'args'."""
    if isinstance(args, str):
        rc = re.compile(r"\s+")
        args = rc.split(args)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res = proc.communicate()
    return res


def which(executable):
    """Return full path of 'executable'."""
    if sys.platform == "win32":
        paths = os.environ.get("Path", "").split(";")
    else:
        paths = os.environ.get("PATH", "").split(":")

    for path in paths:
        path = os.path.expandvars(path)
        path = os.path.expanduser(path)
        path = os.path.abspath(path)
        executable_path = os.path.join(path, executable)
        if os.path.exists(executable_path):
            return executable_path

    return executable


def replace_with_repr(unicode_error):
    """Codec error handler replacement."""
    # Unicode is a pain, some strings cannot be unicode()'d
    # but we want to just preserve the bytes in a human-readable
    # fashion. This codec error handler will substitute the
    # repr() of the offending bytes into the decoded string
    # at the position they occurred
    offender = unicode_error.object[unicode_error.start:unicode_error.end]
    return (str(repr(offender).strip("'").strip('"')), unicode_error.end)


codecs.register_error("repr", replace_with_repr)

#!/usr/bin/env python3
# Copyright (C) 2019-present MongoDB, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the Server Side Public License, version 1,
# as published by MongoDB, Inc.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# Server Side Public License for more details.
#
# You should have received a copy of the Server Side Public License
# along with this program. If not, see
# <http://www.mongodb.com/licensing/server-side-public-license>.
#
# As a special exception, the copyright holders give permission to link the
# code of portions of this program with the OpenSSL library under certain
# conditions as described in each individual source file and distribute
# linked combinations including the program with the OpenSSL library. You
# must comply with the Server Side Public License in all respects for
# all of the code used other than as permitted herein. If you modify file(s)
# with this exception, you may extend this exception to your version of the
# file(s), but you are not obligated to do so. If you do not wish to do so,
# delete this exception statement from your version. If you delete this
# exception statement from all source files in the program, then also delete
# it in the license file.
#
"""Validate that the commit message is ok."""
import argparse
import re
import sys
import logging
from evergreen import RetryingEvergreenApi

EVG_CONFIG_FILE = "./.evergreen.yml"
LOGGER = logging.getLogger(__name__)

COMMON_PUBLIC_PATTERN = r'''
    ((?P<revert>Revert)\s+[\"\']?)?                         # Revert (optional)
    ((?P<ticket>(?:EVG|SERVER|WT)-[0-9]+)[\"\']?\s*)               # ticket identifier
    (?P<body>(?:(?!\(cherry\spicked\sfrom).)*)?             # To also capture the body
    (?P<backport>\(cherry\spicked\sfrom.*)?                 # back port (optional)
    '''
"""Common Public pattern format."""

COMMON_LINT_PATTERN = r'(?P<lint>Fix\slint)'
"""Common Lint pattern format."""

COMMON_IMPORT_PATTERN = r'(?P<imported>Import\s(wiredtiger|tools):\s.*)'
"""Common Import pattern format."""

COMMON_REVERT_IMPORT_PATTERN = r'Revert\s+[\"\']?(?P<imported>Import\s(wiredtiger|tools):\s.*)'
"""Common revert Import pattern format."""

COMMON_PRIVATE_PATTERN = r'''
    ((?P<revert>Revert)\s+[\"\']?)?                                     # Revert (optional)
    ((?P<ticket>[A-Z]+-[0-9]+)[\"\']?\s*)                               # ticket identifier
    (?P<body>(?:(?!('\s(into\s'(([^/]+))/(([^:]+)):(([^']+))'))).)*)?   # To also capture the body
'''
"""Common Private pattern format."""

STATUS_OK = 0
STATUS_ERROR = 1

GIT_SHOW_COMMAND = ["git", "show", "-1", "-s", "--format=%s"]


def new_patch_description(pattern: str) -> str:
    """
    Wrap the pattern to conform to the new commit queue patch description format.

    Add the commit queue prefix and suffix to the pattern. The format looks like:

    Commit Queue Merge: '<commit message>' into '<owner>/<repo>:<branch>'

    :param pattern: The pattern to wrap.
    :return: A pattern to match the new format for the patch description.
    """
    return (r"""^((?P<commitqueue>Commit\sQueue\sMerge:)\s')"""
            f'{pattern}'
            # r"""('\s(?P<into>into\s'((?P<owner>[^/]+))/((?P<repo>[^:]+)):((?P<branch>[^']+))'))"""
            )


def old_patch_description(pattern: str) -> str:
    """
    Wrap the pattern to conform to the new commit queue patch description format.

    Just add a start anchor. The format looks like:

    <commit message>

    :param pattern: The pattern to wrap.
    :return: A pattern to match the old format for the patch description.
    """
    return r'^' f'{pattern}'


# NOTE: re.VERBOSE is for visibility / debugging. As such significant white space must be
# escaped (e.g ' ' to \s).
VALID_PATTERNS = [
    re.compile(new_patch_description(COMMON_PUBLIC_PATTERN), re.MULTILINE | re.DOTALL | re.VERBOSE),
    re.compile(old_patch_description(COMMON_PUBLIC_PATTERN), re.MULTILINE | re.DOTALL | re.VERBOSE),
    re.compile(new_patch_description(COMMON_LINT_PATTERN), re.MULTILINE | re.DOTALL | re.VERBOSE),
    re.compile(old_patch_description(COMMON_LINT_PATTERN), re.MULTILINE | re.DOTALL | re.VERBOSE),
    re.compile(new_patch_description(COMMON_IMPORT_PATTERN), re.MULTILINE | re.DOTALL | re.VERBOSE),
    re.compile(old_patch_description(COMMON_IMPORT_PATTERN), re.MULTILINE | re.DOTALL | re.VERBOSE),
    re.compile(
        new_patch_description(COMMON_REVERT_IMPORT_PATTERN), re.MULTILINE | re.DOTALL | re.VERBOSE),
    re.compile(
        old_patch_description(COMMON_REVERT_IMPORT_PATTERN), re.MULTILINE | re.DOTALL | re.VERBOSE),
]
"""valid public patterns."""

PRIVATE_PATTERNS = [
    re.compile(
        new_patch_description(COMMON_PRIVATE_PATTERN), re.MULTILINE | re.DOTALL | re.VERBOSE),
    re.compile(
        old_patch_description(COMMON_PRIVATE_PATTERN), re.MULTILINE | re.DOTALL | re.VERBOSE),
]
"""private patterns."""


def main(argv=None):
    """Execute Main function to validate commit messages."""
    parser = argparse.ArgumentParser(
        usage="Validate the commit message. "
        "It validates the latest message when no arguments are provided.")
    parser.add_argument(
        "version_id",
        metavar="version id",
        help="The id of the version to validate",
    )
    args = parser.parse_args(argv)
    evg_api = RetryingEvergreenApi.get_api(config_file=EVG_CONFIG_FILE)

    code_changes = evg_api.patch_by_id(args.version_id).module_code_changes

    for change in code_changes:
        for message in change.commit_messages:
            if any(valid_pattern.match(message) for valid_pattern in VALID_PATTERNS):
                continue
            elif any(private_pattern.match(message) for private_pattern in PRIVATE_PATTERNS):
                error_type = "Found a reference to a private project"
            else:
                error_type = "Found a commit without a ticket"
            if error_type:
                LOGGER.error(f"{error_type}\n{message}")  # pylint: disable=logging-fstring-interpolation
                return STATUS_ERROR

    return STATUS_OK


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

# Copyright (C) 2019-present MongoDB, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the Server Side Public License, version 1,
# as published by MongoDB, Inc.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# Server Side Public License for more details.
#
# You should have received a copy of the Server Side Public License
# along with this program. If not, see
# <http://www.mongodb.com/licensing/server-side-public-license>.
#
# As a special exception, the copyright holders give permission to link the
# code of portions of this program with the OpenSSL library under certain
# conditions as described in each individual source file and distribute
# linked combinations including the program with the OpenSSL library. You
# must comply with the Server Side Public License in all respects for
# all of the code used other than as permitted herein. If you modify file(s)
# with this exception, you may extend this exception to your version of the
# file(s), but you are not obligated to do so. If you do not wish to do so,
# delete this exception statement from your version. If you delete this
# exception statement from all source files in the program, then also delete
# it in the license file.
#
"""Validate that mongocryptd push tasks are correct in etc/evergreen.yml."""
from __future__ import absolute_import, print_function, unicode_literals

import argparse
import os
import sys
import yaml

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pylint: disable=wrong-import-position
from buildscripts.ciconfig.evergreen import parse_evergreen_file
# pylint: enable=wrong-import-position

# Name of map to search for in the variables map in evergreen.yml
MONGOCRYPTD_VARIANTS = "mongocryptd_variants"
PUSH_TASK_NAME = "push"


def can_validation_be_skipped(evg_config, variant):
    """
    Determine if the given build variant needs to be validated.

    A build variant does not need to be validated if it does not run the 'push' task or
    if it does not exist in the configuration (it is dynamically created).

    :param evg_config: Evergreen configuration.
    :param variant: Build variant to check.
    :return: True if validation can be skipped.
    """
    variant_config = evg_config.get_variant(variant)
    if not variant_config:
        return True

    if PUSH_TASK_NAME not in variant_config.task_names:
        return True

    return False


def read_variable_from_yml(filename, variable_name):
    """
    Read the given variable from the given yaml file.

    :param filename: Yaml file to read from.
    :param variable_name: Variable to read from file.
    :return: Value of variable or None.
    """
    with open(filename, 'r') as fh:
        nodes = yaml.safe_load(fh)

    variables = nodes["variables"]

    for var in variables:
        if variable_name in var:
            return var[variable_name]
    return None


def main():
    # type: () -> None
    """Execute Main Entry point."""

    parser = argparse.ArgumentParser(description='MongoDB CryptD Check Tool.')

    parser.add_argument('file', type=str, help="etc/evergreen.yml file")
    parser.add_argument('--variant', type=str, help="Build variant to check for")

    args = parser.parse_args()

    expected_variants = read_variable_from_yml(args.file, MONGOCRYPTD_VARIANTS)
    if not expected_variants:
        print("ERROR: Could not find node %s in file '%s'" % (MONGOCRYPTD_VARIANTS, args.file),
              file=sys.stderr)
        sys.exit(1)

    evg_config = parse_evergreen_file(args.file)
    if can_validation_be_skipped(evg_config, args.variant):
        print(f"Skipping validation on buildvariant {args.variant}")
        sys.exit(0)

    if args.variant not in expected_variants:
        print("ERROR: Expected to find variant %s in list %s" % (args.variant, expected_variants),
              file=sys.stderr)
        print(
            "ERROR:  Please add the build variant %s to the %s list in '%s'" %
            (args.variant, MONGOCRYPTD_VARIANTS, args.file), file=sys.stderr)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
