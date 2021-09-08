DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

"build/stitch-support-lib-${version}/bin/stitch_support_test"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

activate_venv

set -o verbose
set -o errexit

# Since `commit_message` is an evergreen expansion, we need a way to ensure we
# properly deal with any special characters that could cause issues (like "). To
# do this, we will write it out to a file, then read that file into a variable.
if [ "${is_commit_queue}" == "true" ]; then
  cat > commit_message.txt << END_OF_COMMIT_MSG
${commit_message}
END_OF_COMMIT_MSG

  commit_message_content=$(cat commit_message.txt)
  rm commit_message.txt

  $python buildscripts/todo_check.py --commit-message "$commit_message_content"
elif [ "${is_patch}" == "true" ]; then
  $python buildscripts/todo_check.py --patch-build ${version_id}
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

set -o errexit
set -o verbose

activate_venv
$python -c 'import json; print(json.dumps([{
  "name": "*** How to use UndoDB Recordings instead of Core Dumps or Log Files ***",
  "link": "https://wiki.corp.mongodb.com/display/COREENG/Time+Travel+Debugging+in+MongoDB",
  "visibility": "public",
  "ignore_for_fetch": True
}]))' > undo_wiki_page_location.json

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit

activate_venv
git clone git@github.com:10gen/coredb-patchbuild-optimizer.git

pushd coredb-patchbuild-optimizer
# Copy the Evergreen config file into the working directory.
cp ../.evergreen.yml .

$python -m pip install tornado==6.1 motor==2.4
# Reusing bfsuggestion's password here to avoid having to
# go through the process of adding a new Evergreen project expansion.
$python -m patchbuild_optimizer "${bfsuggestion_password}"
popd

proc="resmoke.py"
if [ "Windows_NT" = "$OS" ]; then
  check_resmoke() {
    resmoke_info=$(wmic process | grep resmoke.py)
  }
  while [ 1 ]; do
    check_resmoke
    if ! [[ "$resmoke_info" =~ .*"$proc".* ]]; then
      break
    fi
    sleep 5
  done
else
  get_pids() { proc_pids=$(pgrep -f $1); }
  while [ 1 ]; do
    get_pids $proc
    if [ -z "$proc_pids" ]; then
      break
    fi
    sleep 5
  done
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

set -o errexit
set -o verbose

activate_venv
$python -c 'import json; print(json.dumps([{
  "name": "Wiki: Running Tests from Evergreen Tasks Locally",
  "link": "https://github.com/mongodb/mongo/wiki/Running-Tests-from-Evergreen-Tasks-Locally",
  "visibility": "public",
  "ignore_for_fetch": True
}]))' > wiki_page_location.json

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

# activate the virtualenv if it has been set up
activate_venv

additional_args=
if [ "$branch_name" != "master" ]; then
  additional_args="--vulnerabilities_only"
fi

python buildscripts/blackduck_hub.py -v scan_and_report --build_logger=mci.buildlogger --build_logger_task_id=${task_id} --report_file=report.json $additional_args

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

# Create the Evergreen API credentials
cat > .restconfig.json << END_OF_CREDS
{
"baseurl": "${blackduck_url}",
"username": "${blackduck_username}",
"password": "${blackduck_password}",
"debug": false,
"insecure" : false
}
END_OF_CREDS

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose
activate_venv

# Multiversion exclusions can be used when selecting tests.
PATH="$PATH:/data/multiversion"
$python buildscripts/resmoke.py generate-multiversion-exclude-tags --oldBinVersion=last_continuous --excludeTagsFilePath=multiversion_exclude_tags.yml

# Capture a list of new and modified tests. The expansion macro burn_in_tests_build_variant
# is used to for finding the associated tasks from a different build varaint than the
# burn_in_tests_gen task executes on.
build_variant_opts="--build-variant=${build_variant}"
if [ -n "${burn_in_tests_build_variant}" ]; then
  build_variant_opts="--build-variant=${burn_in_tests_build_variant} --run-build-variant=${build_variant}"
fi
burn_in_args="$burn_in_args --repeat-tests-min=2 --repeat-tests-max=1000 --repeat-tests-secs=600"
# Evergreen executable is in $HOME.
PATH="$PATH:$HOME" eval $python buildscripts/evergreen_burn_in_tests.py --project=${project} $build_variant_opts --distro=${distro_id} --generate-tasks-file=burn_in_tests_gen.json --task_id ${task_id} $burn_in_args --verbose

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit

activate_venv

# Multiversion exclusions can be used when selecting tests.
PATH="$PATH:/data/multiversion"
$python buildscripts/resmoke.py generate-multiversion-exclude-tags --oldBinVersion=last_continuous --excludeTagsFilePath=multiversion_exclude_tags.yml

PATH=$PATH:$HOME $python buildscripts/burn_in_tags.py --expansion-file ../expansions.yml

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

activate_venv
# Capture a list of new and modified tests. The expansion macro burn_in_tests_build_variant
# is used for finding the associated tasks from a different build variant than the
# burn_in_tests_multiversion_gen task executes on.
build_variant_opts="--build-variant=${build_variant}"
if [ -n "${burn_in_tests_build_variant}" ]; then
  build_variant_opts="--build-variant=${burn_in_tests_build_variant} --run-build-variant=${build_variant}"
fi

burn_in_args="$burn_in_args"
# Evergreen executable is in $HOME.
PATH="$PATH:$HOME" eval $python buildscripts/burn_in_tests_multiversion.py --task_id=${task_id} --project=${project} $build_variant_opts --distro=${distro_id} --generate-tasks-file=burn_in_tests_multiversion_gen.json $burn_in_args --verbose --revision=${revision} --build-id=${build_id}
PATH="$PATH:/data/multiversion"
$python buildscripts/resmoke.py generate-multiversion-exclude-tags --oldBinVersion=last_continuous

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose
activate_venv

$python buildscripts/idl/check_versioned_api_commands_have_idl_definitions.py -v --include src --include src/mongo/db/modules/enterprise/src --installDir dist-test/bin 1
$python buildscripts/idl/checkout_idl_files_from_past_releases.py -v idls
find idls -maxdepth 1 -mindepth 1 -type d | while read dir; do
  echo "Performing idl check compatibility with release: $dir:"
  $python buildscripts/idl/idl_check_compatibility.py -v --include src --include src/mongo/db/modules/enterprise/src "$dir/src" src
done

set -o verbose
cd src
if [ -f run_tests_infrastructure_failure ]; then
  exit $(cat run_tests_infrastructure_failure)
fi

set -o verbose

rm -rf /data/db/* mongo-diskstats* mongo-*.tgz ~/.aws ~/.boto ../mongodb-mongo-venv
exit 0

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o verbose
set -o errexit
if [ "${is_commit_queue}" = "true" ]; then
  activate_venv
  $python buildscripts/validate_commit_message.py ${version_id}
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

activate_venv
if [ "Windows_NT" = "$OS" ]; then
  vcvars="$(vswhere -latest -property installationPath | tr '\\' '/' | dos2unix.exe)/VC/Auxiliary/Build/"
  cd "$vcvars" && cmd /K "vcvarsall.bat amd64 && cd ${workdir}\src"
fi
python -m pip install ninja
ninja install-core

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

activate_venv

rm -rf /data/install dist-test/bin

edition="${multiversion_edition}"
platform="${multiversion_platform}"
architecture="${multiversion_architecture}"

if [ ! -z "${multiversion_edition_42_or_later}" ]; then
  edition="${multiversion_edition_42_or_later}"
fi
if [ ! -z "${multiversion_platform_42_or_later}" ]; then
  platform="${multiversion_platform_42_or_later}"
fi
if [ ! -z "${multiversion_architecture_42_or_later}" ]; then
  architecture="${multiversion_architecture_42_or_later}"
fi

if [ ! -z "${multiversion_edition_44_or_later}" ]; then
  edition="${multiversion_edition_44_or_later}"
fi
if [ ! -z "${multiversion_platform_44_or_later}" ]; then
  platform="${multiversion_platform_44_or_later}"
fi
if [ ! -z "${multiversion_architecture_44_or_later}" ]; then
  architecture="${multiversion_architecture_44_or_later}"
fi

# This is primarily for tests for infrastructure which don't always need the latest
# binaries.
$python buildscripts/resmoke.py setup-multiversion \
  --installDir /data/install \
  --linkDir dist-test/bin \
  --edition $edition \
  --platform $platform \
  --architecture $architecture \
  --useLatest master

set -o errexit
set -o verbose

cd src
mkdir -p snmpconf
cp -f src/mongo/db/modules/enterprise/docs/mongod.conf.master snmpconf/mongod.conf

set -o errexit
set -o verbose

cd src
./jstests/watchdog/charybdefs_setup.sh

set -o errexit
set -o verbose

cd src/src/mongo/tla_plus
./download-tlc.sh

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

set -o errexit
set -o verbose

VERSION=${version}
WORKDIR=${workdir}

# build in a different directory then we run tests so that we can verify that the linking
# of tests are not relying any built in absolute paths
FINAL_PREFIX=$WORKDIR/src/build/mongo-embedded-sdk-$VERSION
BUILD_PREFIX=$FINAL_PREFIX-tmp

rm -rf mongo-c-driver

# NOTE: If you change the C Driver version here, also change the substitution in the CocoaPod podspec below in the apple builder.
git clone --branch r1.13 --depth 1 https://github.com/mongodb/mongo-c-driver.git
cd mongo-c-driver

# Fixup VERSION so we don't end up with -dev on it. Remove this once we are building a stable version and CDRIVER-2861 is resolved.
cp -f VERSION_RELEASED VERSION_CURRENT

trap "cat CMakeFiles/CMakeOutput.log" EXIT
export ${compile_env}
eval ${cmake_path} -DCMAKE_INSTALL_PREFIX=$BUILD_PREFIX -DENABLE_SHM_COUNTERS=OFF -DENABLE_SNAPPY=OFF -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF -DENABLE_TESTS=OFF -DENABLE_EXAMPLES=OFF -DENABLE_STATIC=OFF -DCMAKE_OSX_DEPLOYMENT_TARGET=${cdriver_cmake_osx_deployment_target} ${cdriver_cmake_flags}
trap - EXIT # cancel the previous trap '...' EXIT
make install VERBOSE=1

# TODO: Remove this when we upgrade to a version of the C driver that has CDRIVER-2854 fixed.
mkdir -p $BUILD_PREFIX/share/doc/mongo-c-driver
cp COPYING $BUILD_PREFIX/share/doc/mongo-c-driver
cp THIRD_PARTY_NOTICES $BUILD_PREFIX/share/doc/mongo-c-driver

mv $BUILD_PREFIX $FINAL_PREFIX

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o verbose
set -o errexit

activate_venv
"build/mongo-embedded-sdk-${version}/bin/mongo_embedded_test"
"build/mongo-embedded-sdk-${version}/bin/mongoc_embedded_test"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src/build

set -o verbose
set -o errexit

# If this is a patch build, blow away the file so our subsequent and optional s3.put
# doesn't run. That way, we won't overwrite the latest part in our patches.
if [ "${is_patch}" = "true" ]; then
  rm -f src/build/embedded-sdk.tgz
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src/build

# Not using archive.targz_pack here because I can't get it to work.
set -o errexit
set -o verbose

cat << EOF > mongo-embedded-sdk-${version}/README-Licenses.txt
The software accompanying this file is Copyright (C) 2018 MongoDB, Inc. and
is licensed to you on the terms set forth in the following files:
  - mongo-c-driver: share/doc/mongo-c-driver/COPYING
  - mongo_embedded: share/doc/mongo_embedded/LICENSE-Embedded.txt
  - mongoc_embedded: share/doc/mongo_embedded/LICENSE-Embedded.txt
EOF

tar cfvz embedded-sdk.tgz mongo-embedded-sdk-${version}

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src/build

# Not using archive.targz_pack here because I can't get it to work.
set -o errexit
set -o verbose

tar cfvz embedded-sdk-tests.tgz mongo-embedded-sdk-${version}

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src/build

set -o errexit
set -o verbose

tar cfvz embedded-sdk-tests.tgz mongo-embedded-sdk-${version}

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

set -o errexit
set -o verbose
activate_venv

# Not all git get project calls clone into ${workdir}/src so we allow
# callers to tell us where the pip requirements files are.
pip_dir="${pip_dir}"
if [[ -z $pip_dir ]]; then
  # Default to most common location
  pip_dir="${workdir}/src/etc/pip"
fi

# Same as above we have to use quotes to preserve the
# Windows path separator
external_auth_txt="$pip_dir/components/aws.req"
python -m pip install -r "$external_auth_txt"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
cat << EOF > aws_e2e_setup.json
{
    "iam_auth_ecs_account" : "${iam_auth_ecs_account}",
    "iam_auth_ecs_secret_access_key" : "${iam_auth_ecs_secret_access_key}",
    "iam_auth_ecs_account_arn": "arn:aws:iam::557821124784:user/authtest_fargate_user",
    "iam_auth_ecs_cluster": "${iam_auth_ecs_cluster}",
    "iam_auth_ecs_task_definition": "${iam_auth_ecs_task_definition}",
    "iam_auth_ecs_subnet_a": "${iam_auth_ecs_subnet_a}",
    "iam_auth_ecs_subnet_b": "${iam_auth_ecs_subnet_b}",
    "iam_auth_ecs_security_group": "${iam_auth_ecs_security_group}",

    "iam_auth_assume_aws_account" : "${iam_auth_assume_aws_account}",
    "iam_auth_assume_aws_secret_access_key" : "${iam_auth_assume_aws_secret_access_key}",
    "iam_auth_assume_role_name" : "${iam_auth_assume_role_name}",

    "iam_auth_ec2_instance_account" : "${iam_auth_ec2_instance_account}",
    "iam_auth_ec2_instance_secret_access_key" : "${iam_auth_ec2_instance_secret_access_key}",
    "iam_auth_ec2_instance_profile" : "${iam_auth_ec2_instance_profile}"
}
EOF

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

set -o errexit
set -o verbose
activate_venv

# Not all git get project calls clone into ${workdir}/src so we allow
# callers to tell us where the pip requirements files are.
pip_dir="${pip_dir}"
if [[ -z $pip_dir ]]; then
  # Default to most common location
  pip_dir="${workdir}/src/etc/pip"
fi

# Same as above we have to use quotes to preserve the
# Windows path separator
external_auth_txt="$pip_dir/external-auth-requirements.txt"
python -m pip install -r "$external_auth_txt"

set -o verbose
set -o errexit

target_dir="src/generated_resmoke_config"
mkdir -p $target_dir
mv generate_tasks_config.tgz $target_dir

cd $target_dir
tar xzf generate_tasks_config.tgz

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -eou pipefail

# Only run on unit test tasks so we don't target mongod binaries from cores.
if [ "${task_name}" != "run_unittests" ] && [ "${task_name}" != "run_dbtest" ] && [ "${task_name}" != "run_unittests_with_recording" ]; then
  exit 0
fi

unittest_bin_dir=dist-unittests/bin
mkdir -p $unittest_bin_dir || true

# Find all core files
core_files=$(/usr/bin/find -H . \( -name "dump_*.core" -o -name "*.mdmp" \) 2> /dev/null)
for core_file in $core_files; do
  # A core file name does not always have the executable name that generated it.
  # See http://stackoverflow.com/questions/34801353/core-dump-filename-gets-thread-name-instead-of-executable-name-with-core-pattern
  # On platforms with GDB, we get the binary name from core file
  gdb=/opt/mongodbtoolchain/gdb/bin/gdb
  if [ -f $gdb ]; then
    binary_file=$($gdb -batch --quiet -ex "core $core_file" 2> /dev/null | grep "Core was generated" | cut -f2 -d "\`" | cut -f1 -d "'" | cut -f1 -d " ")
    binary_file_locations=$binary_file
  else
    # Find the base file name from the core file name, note it may be truncated.
    # Remove leading 'dump_' and trailing '.<pid>.core' or '.<pid or time>.mdmp'
    binary_file=$(echo "$core_file" | sed "s/.*\///;s/dump_//;s/\..*\.core//;s/\..*\.mdmp//")
    # Locate the binary file. Since the base file name might be truncated, the find
    # may return more than 1 file.
    binary_file_locations=$(/usr/bin/find -H . -executable -name "$binary_file*${exe}" 2> /dev/null)
  fi

  if [ -z "$binary_file_locations" ]; then
    echo "Cannot locate the unittest binary file ($binary_file) that generated the core file $core_file"
  fi

  for binary_file_location in $binary_file_locations; do
    new_binary_file=$unittest_bin_dir/$(echo "$binary_file_location" | sed "s/.*\///")
    if [ -f "$binary_file_location" ] && [ ! -f "$new_binary_file" ]; then
      cp "$binary_file_location" "$new_binary_file"
    fi

    # On Windows if a .pdb symbol file exists, include it in the archive.
    pdb_file=$(echo "$binary_file_location" | sed "s/\.exe/.pdb/")
    if [ -f "$pdb_file" ]; then
      new_pdb_file=$unittest_bin_dir/$(echo "$pdb_file" | sed "s/.*\///")
      cp "$pdb_file" "$new_pdb_file"
    fi

    # On binutils platforms, if a .debug symbol file exists, include it
    # in the archive
    debug_file=$binary_file_location.debug
    if [ -f "$debug_file" ]; then
      cp "$debug_file" "$unittest_bin_dir"
    fi

    # On macOS, these are called .dSYM and they are directories
    dsym_dir=$binary_file_location.dSYM
    if [ -d "$dsym_dir" ]; then
      cp -r "$dsym_dir" "$unittest_bin_dir"
    fi

  done
done

# For recorded tests, use the text file to copy them over instead of relying on core dumps.
has_recorded_failures=""
if [[ -f "failed_recorded_tests.txt" ]]; then
  while read -r line; do
    cp "$line" .
  done < "failed_recorded_tests.txt"

  has_recorded_failures="true"
fi

# Copy debug symbols for dynamic builds
lib_dir=build/install/lib
if [ -d "$lib_dir" ] && [[ -n "$core_files" || -n "$has_recorded_failures" ]]; then
  cp -r "$lib_dir" dist-unittests
fi

#!/bin/bash

# For FCV testing only.
# Tag the local branch with the new tag before running tests.

set -o errexit
set -o verbose

cd src
git config user.name "Evergreen patch build"
git config user.email "evergreen@mongodb.com"
git tag -a r5.1.0-alpha -m 5.1.0-alpha
git describe

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

activate_venv

set -o verbose
set -o errexit

enterprise_path="src/mongo/db/modules/enterprise"
diff_file_name="with_base_upstream.diff"

# get the list of feature flags from the patched version
$python buildscripts/idl/gen_all_feature_flag_list.py --import-dir src --import-dir "$enterprise_path"/src
mv all_feature_flags.txt patch_all_feature_flags.txt

# get the list of feature flags from the base commit
git --no-pager diff "$(git merge-base origin/${branch_name} HEAD)" --output="$diff_file_name"
if [ -s "$diff_file_name" ]; then
  git apply -R "$diff_file_name"
fi

# This script has to be run on an Evergreen variant or local repo with the enterprise module.
pushd "$enterprise_path"
git --no-pager diff "$(git merge-base origin/${branch_name} HEAD)" --output="$diff_file_name"
if [ -s "$diff_file_name" ]; then
  git apply -R "$diff_file_name"
fi
popd

$python buildscripts/idl/gen_all_feature_flag_list.py --import-dir src --import-dir "$enterprise_path"/src
mv all_feature_flags.txt base_all_feature_flags.txt

# print out the list of tests that previously had feature flag tag, that was
# enabled by default in the current patch, and currently don't have requires
# latests FCV tag
$python buildscripts/feature_flag_tags_check.py --diff-file-name="$diff_file_name" --enterprise-path="$enterprise_path"

cd src
# Find all core files and move to src
core_files=$(/usr/bin/find -H .. \( -name "*.core" -o -name "*.mdmp" \) 2> /dev/null)
for core_file in $core_files; do
  base_name=$(echo $core_file | sed "s/.*\///")
  # Move file if it does not already exist
  if [ ! -f $base_name ]; then
    mv $core_file .
  fi
done

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose
activate_venv
$python buildscripts/idl/gen_all_feature_flag_list.py --import-dir src --import-dir src/mongo/db/modules/enterprise/src

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

# Only generate tags for query patch variants.
if [[ "${build_variant}" != *"query-patch-only"* ]]; then
  exit 0
fi

activate_venv
git clone git@github.com:10gen/coredb-patchbuild-optimizer.git

pushd coredb-patchbuild-optimizer
# Reusing bfsuggestion's password here to avoid having to
# go through the process of adding a new Evergreen project expansion.
$python tagfilegenerator.py "${bfsuggestion_password}"
mv failedtesttags ..
popd

tar -cvzf patch_test_tags.tgz failedtesttags

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit

activate_venv
$python buildscripts/evergreen_activate_gen_tasks.py --expansion-file ../expansions.yml --verbose

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit

activate_venv
PATH=$PATH:$HOME $python buildscripts/evergreen_gen_build_variant.py \
  --expansion-file ../expansions.yml \
  --evg-api-config ./.evergreen.yml \
  --output-file ${build_variant}.json \
  --verbose \
  $@

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o verbose

# Set what processes to look for. For most tasks, we rely on resmoke to figure out its subprocesses
# and run the hang analyzer on those. For non-resmoke tasks, we enumerate the process list here.
if [[ ${task_name} == *"jepsen"* ]]; then
  hang_analyzer_option="-o file -o stdout -p dbtest,java,mongo,mongod,mongos,python,_test"
else
  hang_analyzer_option="-o file -o stdout -m exact -p python"
fi

activate_venv
echo "Calling the hang analyzer: PATH=\"/opt/mongodbtoolchain/gdb/bin:$PATH\" $python buildscripts/resmoke.py hang-analyzer $hang_analyzer_option"
PATH="/opt/mongodbtoolchain/gdb/bin:$PATH" $python buildscripts/resmoke.py hang-analyzer $hang_analyzer_option

# Call hang analyzer for tasks that are running remote mongo processes
if [ -n "${private_ip_address}" ]; then
  $python buildscripts/resmoke.py powercycle remote-hang-analyzer
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

activate_venv
$python buildscripts/idl/run_tests.py

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit

activate_venv
PATH="$PATH:/data/multiversion"

if [ -n "${require_multiversion}" ]; then
  $python buildscripts/resmoke.py generate-multiversion-exclude-tags --oldBinVersion=last_continuous
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src/jepsen-mongodb

set -o verbose
# Jepsen system failure if file exists.
if [ -f jepsen_system_failure_${task_name}_${execution} ]; then
  exit $(cat jepsen_system_failure_${task_name}_${execution})
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src/jepsen-mongodb

set -o verbose

# Set the TMPDIR environment variable to be a directory in the task's working
# directory so that temporary files created by processes spawned by jepsen get
# cleaned up after the task completes. This also ensures the spawned processes
# aren't impacted by limited space in the mount point for the /tmp directory.
# We also need to set the _JAVA_OPTIONS environment variable so that lein will
# recognize this as the default temp directory.
export TMPDIR="${workdir}/tmp"
mkdir -p $TMPDIR
export _JAVA_OPTIONS=-Djava.io.tmpdir=$TMPDIR

start_time=$(date +%s)
lein run test --test ${jepsen_test_name} \
  --mongodb-dir ../ \
  --working-dir ${workdir}/src/jepsen-workdir \
  --clock-skew faketime \
  --libfaketime-path ${workdir}/src/libfaketime/build/libfaketime.so.1 \
  --mongod-conf mongod_verbose.conf \
  --virtualization none \
  --nodes-file ../nodes.txt \
  ${jepsen_key_time_limit} \
  ${jepsen_protocol_version} \
  ${jepsen_read_concern} \
  ${jepsen_read_with_find_and_modify} \
  ${jepsen_storage_engine} \
  ${jepsen_time_limit} \
  ${jepsen_write_concern} \
  2>&1 \
  | tee jepsen_${task_name}_${execution}.log
end_time=$(date +%s)
elapsed_secs=$((end_time - start_time))
# Since we cannot use PIPESTATUS to get the exit code from the "lein run ..." pipe in dash shell,
# we will check the output for success, failure or setup error. Note that 'grep' returns with exit code
# 0 if it finds a match, and exit code 1 if no match is found.
grep -q "Everything looks good" jepsen_${task_name}_${execution}.log
grep_exit_code=$?
if [ $grep_exit_code -eq 0 ]; then
  status='"pass"'
  failures=0
  final_exit_code=0
else
  grep -q "Analysis invalid" jepsen_${task_name}_${execution}.log
  grep_exit_code=$?
  if [ $grep_exit_code -eq 0 ]; then
    status='"fail"'
    failures=1
    final_exit_code=1
  else
    # If the failure is due to setup, then this is considered a system failure.
    echo $grep_exit_code > jepsen_system_failure_${task_name}_${execution}
    exit 0
  fi
fi
# Create report.json
echo "{\"failures\": $failures, \"results\": [{\"status\": $status, \"exit_code\": $final_exit_code, \"test_file\": \"${task_name}\", \"start\": $start_time, \"end\": $end_time, \"elapsed\": $elapsed_secs}]}" > ../report.json
exit $final_exit_code

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src/jstestfuzz

set -o errexit
set -o verbose

add_nodejs_to_path

eval npm run ${npm_command} -- ${jstestfuzz_vars} --branch ${branch_name}

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

add_nodejs_to_path

git clone git@github.com:10gen/jstestfuzz.git

pushd jstestfuzz
npm install
npm run prepare
popd

process_kill_list="(^cl\.exe$|java|lein|lldb|mongo|python|_test$|_test\.exe$)"
# Exclude Evergreen agent processes and other system daemons
process_exclude_list="(main|tuned|evergreen)"

if [ "Windows_NT" = "$OS" ]; then
  # Get the list of Windows tasks (tasklist list format):
  # - Transpose the Image Name and PID
  # - The first column has the process ID
  # - The second column (and beyond) has task name
  # - Grep for the task names of interest while ignoring any names that are in the exclude list

  processes=$(tasklist /fo:csv | awk -F'","' '{x=$1; gsub("\"","",x); print $2, x}' | grep -iE "$process_kill_list" | grep -ivE "$process_exclude_list")

  # Kill the Windows process by process ID with force (/f)
  kill_process() {
    pid=$(echo $1 | cut -f1 -d ' ')
    echo "Killing process $1"
    taskkill /pid "$pid" /f
  }
else
  # Get the list of Unix tasks (pgrep full & long):
  # - Grep for the task names of interest while ignoring any names that are in the exclude list
  # - The first column has the process ID
  # - The second column (and beyond) has task name

  # There are 2 "styles" of pgrep, figure out which one works.
  # Due to https://bugs.launchpad.net/ubuntu/+source/procps/+bug/1501916
  # we cannot rely on the return status ($?) to detect if the option is supported.
  pgrep -f --list-full ".*" 2>&1 | grep -qE "(illegal|invalid|unrecognized) option"
  if [ $? -ne 0 ]; then
    pgrep_list=$(pgrep -f --list-full "$process_kill_list")
  else
    pgrep_list=$(pgrep -f -l "$process_kill_list")
  fi

  # Since a process name might have a CR or LF in it, we need to delete any lines from
  # pgrep which do not start with space(s) and 1 digit and trim any leading spaces.
  processes=$(echo "$pgrep_list" | grep -ivE "$process_exclude_list" | sed -e '/^ *[0-9]/!d; s/^ *//; s/[[:cntrl:]]//g;')

  # Kill the Unix process ID with signal KILL (9)
  kill_process() {
    pid=$(echo $1 | cut -f1 -d ' ')
    echo "Killing process $1"
    kill -9 $pid
  }
fi
# Since a full process name can have spaces, the IFS (internal field separator)
# should not include a space, just a LF & CR
IFS=$(printf "\n\r")
for process in $processes; do
  kill_process "$process"
done

exit 0

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src/buildscripts/package_test

set -o errexit

export KITCHEN_ARTIFACTS_URL="https://s3.amazonaws.com/mciuploads/${project}/${build_variant}/${revision}/artifacts/${build_id}-packages.tgz"
export KITCHEN_SECURITY_GROUP="${kitchen_security_group}"
export KITCHEN_SSH_KEY_ID="${kitchen_ssh_key_id}"
export KITCHEN_SUBNET="${kitchen_subnet}"
export KITCHEN_VPC="${kitchen_vpc}"

if [[ "${packager_arch}" == "aarch64" || "${packager_arch}" == "arm64" ]]; then
  kitchen_packager_distro="${packager_distro}-arm64"
else
  kitchen_packager_distro="${packager_distro}-x86-64"
fi

activate_venv
# set expiration tag 2 hours in the future, since no test should take this long
export KITCHEN_EXPIRE="$($python -c 'import datetime; print((datetime.datetime.utcnow() + datetime.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"))')"

for i in {1..3}; do
  if ! kitchen verify $kitchen_packager_distro; then
    verified="false"
    kitchen destroy $kitchen_packager_distro || true
    sleep 30
  else
    verified="true"
    break
  fi
done

kitchen destroy $kitchen_packager_distro || true
test "$verified" = "true"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

activate_venv
GRAPH_FILE=$(find build -name "libdeps.graphml")
python buildscripts/libdeps/gacli.py --graph-file $GRAPH_FILE > results.txt
gzip $GRAPH_FILE
mv $GRAPH_FILE.gz .

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

activate_venv
python -m pip install -r etc/pip/libdeps-requirements.txt

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -eo pipefail
set -o verbose

add_nodejs_to_path

# Run parse-jsfiles on 50 files at a time with 32 processes in parallel.
find "$PWD/jstests" "$PWD/src/mongo/db/modules/enterprise" -name "*.js" -print | xargs -P 32 -L 50 npm run --prefix jstestfuzz parse-jsfiles --

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -eo pipefail
set -o verbose

add_nodejs_to_path

mkdir -p jstestfuzzinput jstestfuzzoutput

indir="$(pwd)/jstestfuzzinput"
outdir="$(pwd)/jstestfuzzoutput"

# Grep all the js files from modified_and_created_patch_files.txt and put them into $indir.
(grep -v "\.tpl\.js$" modified_and_created_patch_files.txt | grep ".*jstests/.*\.js$" | xargs -I {} cp {} $indir || true)

# Count the number of files in $indir.
if [[ "$(ls -A $indir)" ]]; then
  num_files=$(ls -A $indir | wc -l)

  # Only fetch 50 files to generate jsfuzz testing files.
  if [[ $num_files -gt 50 ]]; then
    num_files=50
  fi

  npm run --prefix jstestfuzz jstestfuzz -- --jsTestsDir $indir --out $outdir --numSourceFiles $num_files --numGeneratedFiles 50

  # Run parse-jsfiles on 50 files at a time with 32 processes in parallel.
  ls -1 -d $outdir/* | xargs -P 32 -L 50 npm run --prefix jstestfuzz parse-jsfiles --
fi

cd src

PATH="/opt/shfmt/v3.2.4/bin:$PATH"
./buildscripts/shellscripts-linters.sh

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

activate_venv
./buildscripts/yamllinters.sh

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

client_logs=$(ls crud*.log fsm*.log 2> /dev/null)
if [ ! -z "$client_logs" ]; then
  ${tar} czf client-logs.tgz $client_logs
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

activate_venv

rm -rf /data/install /data/multiversion

edition="${multiversion_edition}"
platform="${multiversion_platform}"
architecture="${multiversion_architecture}"

$python buildscripts/resmoke.py setup-multiversion \
  --installDir /data/install \
  --linkDir /data/multiversion \
  --edition $edition \
  --platform $platform \
  --architecture $architecture \
  --useLatest 4.0

# The platform and architecture for how some of the binaries are reported in
# https://downloads.mongodb.org/full.json changed between MongoDB 4.0 and MongoDB 4.2.
# Certain build variants define additional multiversion_*_42_or_later expansions in order to
# be able to fetch a complete set of versions.

if [ ! -z "${multiversion_edition_42_or_later}" ]; then
  edition="${multiversion_edition_42_or_later}"
fi

if [ ! -z "${multiversion_platform_42_or_later}" ]; then
  platform="${multiversion_platform_42_or_later}"
fi

if [ ! -z "${multiversion_architecture_42_or_later}" ]; then
  architecture="${multiversion_architecture_42_or_later}"
fi

$python buildscripts/resmoke.py setup-multiversion \
  --installDir /data/install \
  --linkDir /data/multiversion \
  --edition $edition \
  --platform $platform \
  --architecture $architecture \
  --useLatest 4.2

# The platform and architecture for how some of the binaries are reported in
# https://downloads.mongodb.org/full.json changed between MongoDB 4.2 and MongoDB 4.4.
# Certain build variants define additional multiversion_*_44_or_later expansions in order to
# be able to fetch a complete set of versions.

if [ ! -z "${multiversion_edition_44_or_later}" ]; then
  edition="${multiversion_edition_44_or_later}"
fi

if [ ! -z "${multiversion_platform_44_or_later}" ]; then
  platform="${multiversion_platform_44_or_later}"
fi

if [ ! -z "${multiversion_architecture_44_or_later}" ]; then
  architecture="${multiversion_architecture_44_or_later}"
fi

$python buildscripts/resmoke.py setup-multiversion \
  --installDir /data/install \
  --linkDir /data/multiversion \
  --edition $edition \
  --platform $platform \
  --architecture $architecture \
  --useLatest 4.4 5.0

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose
activate_venv
python -m pip install ninja
if [ "Windows_NT" = "$OS" ]; then
  vcvars="$(vswhere -latest -property installationPath | tr '\\' '/' | dos2unix.exe)/VC/Auxiliary/Build/"
  echo "call \"$vcvars/vcvarsall.bat\" amd64" > msvc.bat
  echo "ninja install-core" >> msvc.bat
  cmd /C msvc.bat
else
  ninja install-core
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

. ./notary_env.sh

set -o errexit
set -o verbose

long_ext=${ext}
if [ "$long_ext" == "tgz" ]; then
  long_ext="tar.gz"
fi

mv mongo-binaries.tgz mongodb-${push_name}-${push_arch}-${suffix}.${ext}
mv mongo-shell.tgz mongodb-shell-${push_name}-${push_arch}-${suffix}.${ext}
mv mongo-cryptd.tgz mongodb-cryptd-${push_name}-${push_arch}-${suffix}.${ext} || true
mv mh.tgz mh-${push_name}-${push_arch}-${suffix}.${ext} || true
mv mongo-debugsymbols.tgz mongodb-${push_name}-${push_arch}-debugsymbols-${suffix}.${ext} || true
mv distsrc.${ext} mongodb-src-${src_suffix}.${long_ext} || true
/usr/bin/find build/ -type f | grep msi$ | xargs -I original_filename cp original_filename mongodb-${push_name}-${push_arch}-${suffix}.msi || true

/usr/local/bin/notary-client.py --key-name "server-5.0" --auth-token-file ${workdir}/src/signing_auth_token --comment "Evergreen Automatic Signing ${revision} - ${build_variant} - ${branch_name}" --notary-url http://notary-service.build.10gen.cc:5000 --skip-missing mongodb-${push_name}-${push_arch}-${suffix}.${ext} mongodb-shell-${push_name}-${push_arch}-${suffix}.${ext} mongodb-${push_name}-${push_arch}-debugsymbols-${suffix}.${ext} mongodb-${push_name}-${push_arch}-${suffix}.msi mongodb-src-${src_suffix}.${long_ext} mongodb-cryptd-${push_name}-${push_arch}-${suffix}.${ext}

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

if [ $(find . -name mongocryptd${exe} | wc -l) -eq 1 ]; then
  # Validate that this build_variant is listed as a known enterprise task for mongocryptd
  eval PATH=$PATH:$HOME $python ../buildscripts/validate_mongocryptd.py --variant "${build_variant}" ../etc/evergreen.yml
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

activate_venv
if [ "${has_packages}" = "true" ]; then
  cd buildscripts
  $python ${packager_script} --prefix $(pwd)/.. --distros ${packager_distro} --tarball $(pwd)/../mongodb-dist.tgz -s ${version} -m HEAD -a ${packager_arch}
  cd ..
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

. ./notary_env.sh

set -o errexit
set -o verbose

CURATOR_RELEASE=${curator_release}
curl -L -O http://boxes.10gen.com/build/curator/curator-dist-rhel70-$CURATOR_RELEASE.tar.gz
tar -zxvf curator-dist-rhel70-$CURATOR_RELEASE.tar.gz
./curator repo submit --service ${barque_url} --config ./etc/repo_config.yaml --distro ${packager_distro} --edition ${repo_edition} --version ${version} --arch ${packager_arch} --packages https://s3.amazonaws.com/mciuploads/${project}/${build_variant}/${revision}/artifacts/${build_id}-packages.tgz

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

if [ "Windows_NT" = "$OS" ]; then
  user=Administrator
else
  user=$USER
fi
hostname=$(tr -d '"[]{}' < src/hosts.yml | cut -d , -f 1 | awk -F : '{print $2}')

# To add the hostname to expansions.
echo "private_ip_address: $hostname" >> src/powercycle_ip_address.yml

echo $hostname
echo $user

attempts=0
connection_attempts=60

# Check for remote connectivity
while ! ssh \
  -i ${private_key_file} \
  -o ConnectTimeout=10 \
  -o ForwardAgent=yes \
  -o IdentitiesOnly=yes \
  -o StrictHostKeyChecking=no \
  "$(printf "%s@%s" "$user" "$hostname")" \
  exit 2> /dev/null; do
  [ "$attempts" -ge "$connection_attempts" ] && exit 1
  ((attempts++))
  printf "SSH connection attempt %d/%d failed. Retrying...\n" "$attempts" "$connection_attempts"
  # sleep for Permission denied (publickey) errors
  sleep 10
done

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

# Test exits from here with specified exit_code.
if [ -n "${exit_code}" ]; then
  # Python program saved exit_code
  exit_code=${exit_code}
elif [ -f error_exit.txt ]; then
  # Bash trap exit_code
  exit_code=$(cat error_exit.txt)
else
  exit_code=0
fi
echo "Exiting powercycle with code $exit_code"
exit $exit_code

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

if [ "Windows_NT" = "$OS" ]; then
  user=Administrator
else
  user=$USER
fi

activate_venv
# Set an exit trap so we can save the real exit status (see SERVER-34033).
trap 'echo $? > error_exit.txt; exit 0' EXIT
set +o errexit
eval $python -u buildscripts/resmoke.py powercycle run \
  "--sshUserHost=$(printf "%s@%s" "$user" "${private_ip_address}") \
  --sshConnection=\"-i powercycle.pem\" \
  --taskName=${task_name} \
  ${run_powercycle_args}"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o verbose

if [ ! -f powercycle_ip_address.yml ]; then
  exit 0
fi

activate_venv
$python buildscripts/resmoke.py powercycle save-diagnostics

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

activate_venv
$python buildscripts/powercycle_sentinel.py ../expansions.yml

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o verbose
set -o errexit

activate_venv
$python buildscripts/resmoke.py powercycle setup-host

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

# Trigger a system failure if powercycle failed due to ssh access.
if [ -n "${ec2_ssh_failure}" ]; then
  echo "ec2_ssh_failure detected - $(cat powercycle_exit.yml)"
  exit ${exit_code}
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

activate_venv
$python buildscripts/evergreen_gen_powercycle_tasks.py ../expansions.yml ../powercycle_tasks.json

if [[ "$0" == *"/evergreen/prelude.sh" ]]; then
  echo "ERROR: do not execute this script. source it instead. i.e.: . prelude.sh"
  exit 1
fi
set -o errexit

# path the directory that contains this script.
evergreen_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"

. "$evergreen_dir/prelude_workdir.sh"
. "$evergreen_dir/prelude_python.sh"
. "$evergreen_dir/prelude_venv.sh"

expansions_yaml="$evergreen_dir/../../expansions.yml"
expansions_default_yaml="$evergreen_dir/../etc/expansions.default.yml"
script="$evergreen_dir/../buildscripts/evergreen_expansions2bash.py"
if [ "Windows_NT" = "$OS" ]; then
  expansions_yaml=$(cygpath -w "$expansions_yaml")
  expansions_default_yaml=$(cygpath -w "$expansions_default_yaml")
  script=$(cygpath -w "$script")
fi

eval $(activate_venv > /dev/null && $python "$script" "$expansions_yaml" "$expansions_default_yaml")
if [ -n "$___expansions_error" ]; then
  echo $___expansions_error
  exit 1
fi
unset expansions_yaml
unset expansions_default_yaml
unset script
unset evergreen_dir

function add_nodejs_to_path {
  # Add node and npm binaries to PATH
  if [ "Windows_NT" = "$OS" ]; then
    # An "npm" directory might not have been created in %APPDATA% by the Windows installer.
    # Work around the issue by specifying a different %APPDATA% path.
    # See: https://github.com/nodejs/node-v0.x-archive/issues/8141
    export APPDATA=${workdir}/npm-app-data
    export PATH="$PATH:/cygdrive/c/Program Files (x86)/nodejs" # Windows location
    # TODO: this is to work around BUILD-8652
    cd "$(pwd -P | sed 's,cygdrive/c/,cygdrive/z/,')"
  else
    export PATH="$PATH:/opt/node/bin"
  fi
}

function posix_workdir {
  if [ "Windows_NT" = "$OS" ]; then
    echo $(cygpath -u "${workdir}")
  else
    echo ${workdir}
  fi
}

function set_sudo {
  set -o > /tmp/settings.log
  set +o errexit
  grep errexit /tmp/settings.log | grep on
  errexit_on=$?
  # Set errexit "off".
  set +o errexit
  sudo=
  # Use sudo, if it is supported.
  sudo date > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    sudo=sudo
  fi
  # Set errexit "on", if previously enabled.
  if [ $errexit_on -eq 0 ]; then
    set -o errexit
  fi
}
set +o errexit

if [ "Windows_NT" = "$OS" ]; then
  python='/cygdrive/c/python/python37/python.exe'
else
  if [ -f /opt/mongodbtoolchain/v3/bin/python3 ]; then
    python="/opt/mongodbtoolchain/v3/bin/python3"
  elif [ -f "$(which python3)" ]; then
    echo "Could not find mongodbtoolchain python, using system python $(which python3)" > 2
    python=$(which python3)
  else
    echo "Could not find python3." > 2
    return 1
  fi
fi

function activate_venv {
  # check if virtualenv is set up
  if [ -d "${workdir}/../mongodb-mongo-venv" ]; then
    if [ "Windows_NT" = "$OS" ]; then
      # Need to quote the path on Windows to preserve the separator.
      . "${workdir}/../mongodb-mongo-venv/Scripts/activate" 2> /tmp/activate_error.log
    else
      . ${workdir}/../mongodb-mongo-venv/bin/activate 2> /tmp/activate_error.log
    fi
    if [ $? -ne 0 ]; then
      echo "Failed to activate virtualenv: $(cat /tmp/activate_error.log)"
      exit 1
    fi
    python=python
  else
    if [ -z "$python" ]; then
      echo "\$python is unset. This should never happen"
      exit 1
    fi
    python=${python}
  fi

  if [ "Windows_NT" = "$OS" ]; then
    export PYTHONPATH="$PYTHONPATH;$(cygpath -w ${workdir}/src)"
  else
    export PYTHONPATH="$PYTHONPATH:${workdir}/src"
  fi

  echo "python set to $(which $python)"
}

calculated_workdir=$(cd "$evergreen_dir/../.." && echo "$PWD")
pwd_cygpath="$PWD"
if [ "Windows_NT" = "$OS" ]; then
  calculated_workdir=$(cygpath -w "$calculated_workdir")
  pwd_cygpath=$(cygpath -w "$pwd_cygpath")
fi
if [ -z "$workdir" ]; then
  workdir="$calculated_workdir"

# skip this test on Windows. The directories will never match due to the many
# different path types present on Windows+Cygwin
elif [ "$workdir" != "$calculated_workdir" ] && [ "Windows_NT" != "$OS" ]; then
  # if you move the checkout directory (ex: simple project config project),
  # then this assertion will fail in the future. You need to update
  # calculated_workdir, and all the relative directories in this file.
  echo "\$workdir was specified, but didn't match \$calculated_workdir. Did the directory structure change? Update prelude.sh"
  echo "\$workdir: $workdir"
  echo "\$calculated_workdir: $calculated_workdir"
  exit 1
fi
if [ "$pwd_cygpath" != "$calculated_workdir" ]; then
  echo "ERROR: Your script changed directory before loading prelude.sh. Don't do that"
  echo "\$PWD: $PWD"
  echo "\$calculated_workdir: $calculated_workdir"
  exit 1
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit

activate_venv
$python buildscripts/evergreen_generate_resmoke_tasks.py --expansion-file ../expansions.yml --verbose

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

# Export these before verbose is set to avoid sharing sensitive info.
export CEDAR_USERNAME=${cedar_user}
export CEDAR_API_KEY=${cedar_api_key}

set -o errexit
set -o verbose

if [[ ${disable_unit_tests} = "false" && ! -f ${skip_tests} ]]; then

  # activate the virtualenv if it has been set up
  activate_venv

  if [[ -f "patch_test_tags.tgz" ]]; then
    tags_build_variant="${build_variant}"

    if [[ "${build_variant}" =~ .*"-query-patch-only" ]]; then
      # Use the RHEL 8 all feature flags variant for the classic engine variant. The original
      # classic engine variant is not a required builder and therefore not captured in patch
      # test failure history.
      tags_build_variant="enterprise-rhel-80-64-bit-dynamic-all-feature-flags-required"
    fi

    $python buildscripts/testmatrix/getdisplaytaskname.py "${task_name}" "${build_variant}" > display_task_name.txt
    display_task_name=$(cat display_task_name.txt)

    tar -xzf patch_test_tags.tgz

    calculated_tags_file_path="failedtesttags/${tags_build_variant}/${display_task_name}.yml"

    if [[ -f $calculated_tags_file_path ]]; then
      extra_args="$extra_args --tagFile=failedtesttags/${tags_build_variant}/${display_task_name}.yml --includeWithAllTags=recent_failure"
    else
      echo "calculated tags file does not exist: $calculated_tags_file_path"
    fi
  fi

  # Set the TMPDIR environment variable to be a directory in the task's working
  # directory so that temporary files created by processes spawned by resmoke.py get
  # cleaned up after the task completes. This also ensures the spawned processes
  # aren't impacted by limited space in the mount point for the /tmp directory.
  export TMPDIR="${workdir}/tmp"
  mkdir -p $TMPDIR

  if [ -f /proc/self/coredump_filter ]; then
    # Set the shell process (and its children processes) to dump ELF headers (bit 4),
    # anonymous shared mappings (bit 1), and anonymous private mappings (bit 0).
    echo 0x13 > /proc/self/coredump_filter

    if [ -f /sbin/sysctl ]; then
      # Check that the core pattern is set explicitly on our distro image instead
      # of being the OS's default value. This ensures that coredump names are consistent
      # across distros and can be picked up by Evergreen.
      core_pattern=$(/sbin/sysctl -n "kernel.core_pattern")
      if [ "$core_pattern" = "dump_%e.%p.core" ]; then
        echo "Enabling coredumps"
        ulimit -c unlimited
      fi
    fi
  fi

  if [ $(uname -s) == "Darwin" ]; then
    core_pattern_mac=$(/usr/sbin/sysctl -n "kern.corefile")
    if [ "$core_pattern_mac" = "dump_%N.%P.core" ]; then
      echo "Enabling coredumps"
      ulimit -c unlimited
    fi
  fi

  extra_args="$extra_args --jobs=${resmoke_jobs}"

  if [ ${should_shuffle} = true ]; then
    extra_args="$extra_args --shuffle"
  fi

  if [ ${continue_on_failure} = true ]; then
    extra_args="$extra_args --continueOnFailure"
  fi

  # We reduce the storage engine's cache size to reduce the likelihood of a mongod process
  # being killed by the OOM killer. The --storageEngineCacheSizeGB command line option is only
  # filled in with a default value here if one hasn't already been specified in the task's
  # definition or build variant's definition.
  set +o errexit
  echo "${resmoke_args} ${test_flags}" | grep -q storageEngineCacheSizeGB
  if [ $? -eq 1 ]; then
    echo "${resmoke_args} ${test_flags}" | grep -q "\-\-storageEngine=inMemory"
    if [ $? -eq 0 ]; then
      # We use a default of 4GB for the InMemory storage engine.
      extra_args="$extra_args --storageEngineCacheSizeGB=4"
    else
      # We use a default of 1GB for all other storage engines.
      extra_args="$extra_args --storageEngineCacheSizeGB=1"
    fi
  fi
  set -o errexit

  # Reduce the JSHeapLimit for the serial_run task task on Code Coverage builder variant.
  if [[ "${build_variant}" = "enterprise-rhel-80-64-bit-coverage" && "${task_name}" = "serial_run" ]]; then
    extra_args="$extra_args --mongodSetParameter \"{'jsHeapLimitMB':10}\""
  fi

  spawn_using=${spawn_resmoke_using}
  if [[ -z "$spawn_using" ]]; then
    spawn_using="python"
  fi

  path_value="$PATH:/data/multiversion"

  # The "resmoke_wrapper" expansion is used by the 'burn_in_tests' task to wrap the resmoke.py
  # invocation. It doesn't set any environment variables and should therefore come last in
  # this list of expansions.
  set +o errexit
  PATH="$path_value" \
    AWS_PROFILE=${aws_profile_remote} \
    eval \
    ${gcov_environment} \
    ${lang_environment} \
    ${san_options} \
    ${snmp_config_path} \
    ${resmoke_wrapper} \
    $python buildscripts/resmoke.py run \
    ${record_with} \
    ${resmoke_args} \
    $extra_args \
    ${test_flags} \
    --log=buildlogger \
    --staggerJobs=on \
    --installDir=${install_dir} \
    --buildId=${build_id} \
    --distroId=${distro_id} \
    --executionNumber=${execution} \
    --projectName=${project} \
    --gitRevision=${revision} \
    --revisionOrderId=${revision_order_id} \
    --taskId=${task_id} \
    --taskName=${task_name} \
    --variantName=${build_variant} \
    --versionId=${version_id} \
    --spawnUsing=$spawn_using \
    --reportFile=report.json \
    --perfReportFile=perf.json
  resmoke_exit_code=$?
  set -o errexit

  if [[ -n "${record_with}" ]]; then
    recording_size=$( (du -ch ./*.undo ./*.undo.tokeep || true) | grep total)
    echo "UndoDB produced recordings that were $recording_size (uncompressed) on disk"
    # Unittests recordings are renamed so there's never a need to store any .undo files.
    if [[ $resmoke_exit_code = 0 || "${task_name}" == "run_unittests_with_recording" ]]; then
      echo "Removing UndoDB recordings of successful tests."
      rm *.undo || true
    fi
  fi

  # 74 is exit code for IOError on POSIX systems, which is raised when the machine is
  # shutting down.
  #
  # 75 is exit code resmoke.py uses when the log output would be incomplete due to failing
  # to communicate with logkeeper.
  if [[ $resmoke_exit_code = 74 || $resmoke_exit_code = 75 ]]; then
    echo $resmoke_exit_code > run_tests_infrastructure_failure
    exit 0
  elif [ $resmoke_exit_code != 0 ]; then
    # On failure save the resmoke exit code.
    echo $resmoke_exit_code > resmoke_error_code
  elif [ $resmoke_exit_code = 0 ]; then
    # On success delete core files.
    core_files=$(/usr/bin/find -H .. \( -name "*.core" -o -name "*.mdmp" \) 2> /dev/null)
    rm -rf $core_files
  fi

  exit $resmoke_exit_code
fi # end if [[ ${disable_unit_tests} && ! -f ${skip_tests|/dev/null} ]]

set -o errexit
set -o verbose

cd src
# TODO SERVER-49884 Remove this when we no longer check in generated Bison.
# Here we use the -header-filter option to instruct clang-tidy to scan our header files. The
# regex instructs clang-tidy to scan headers in our source directory with the mongo/* regex, and
# the build directory to analyze generated headers with the build/* regex
BISON_GENERATED_PATTERN=parser_gen\.cpp
jq -r '.[] | .file' compile_commands.json \
  | grep src/mongo \
  | grep -v $BISON_GENERATED_PATTERN \
  | xargs -n 32 -P $(grep -c ^processor /proc/cpuinfo) -t \
    /opt/mongodbtoolchain/v3/bin/clang-tidy \
    -p ./compile_commands.json \
    -header-filter='(mongo/.*|build/.*)' \
    --checks="-*,bugprone-unused-raii,bugprone-use-after-move,readability-const-return-type,readability-avoid-const-params-in-decls" \
    -warnings-as-errors="*"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

rm -rf ${install_directory}

# Use hardlinks to reduce the disk space impact of installing
# all of the binaries and associated debug info.

# The expansion here is a workaround to let us set a different install-action
# for tasks that don't support the one we set here. A better plan would be
# to support install-action for Ninja builds directly.
# TODO: https://jira.mongodb.org/browse/SERVER-48203
extra_args="--install-action=${task_install_action}"

# By default, limit link jobs to one quarter of our overall -j
# concurrency unless locally overridden. We do this because in
# static link environments, the memory consumption of each
# link job is so high that without constraining the number of
# links we are likely to OOM or thrash the machine. Dynamic
# builds, where htis is not a concern, override this value.
echo "Changing SCons to run with --jlink=${num_scons_link_jobs_available}"
extra_args="$extra_args --jlink=${num_scons_link_jobs_available} --separate-debug=${separate_debug}"

if [ "${scons_cache_scope}" = "shared" ]; then
  extra_args="$extra_args --cache-debug=scons_cache.log"
fi

# Conditionally enable scons time debugging
if [ "${show_scons_timings}" = "true" ]; then
  extra_args="$extra_args --debug=time"
fi

# Build packages where the upload tasks expect them
if [ -n "${git_project_directory}" ]; then
  extra_args="$extra_args PKGDIR='${git_project_directory}'"
else
  extra_args="$extra_args PKGDIR='${workdir}/src'"
fi

# If we are doing a patch build or we are building a non-push
# build on the waterfall, then we don't need the --release
# flag. Otherwise, this is potentially a build that "leaves
# the building", so we do want that flag. The non --release
# case should auto enale the faster decider when
# applicable. Furthermore, for the non --release cases we can
# accelerate the build slightly for situations where we invoke
# SCons multiple times on the same machine by allowing SCons
# to assume that implicit dependencies are cacheable across
# runs.
if [ "${is_patch}" = "true" ] || [ -z "${push_bucket}" ] || [ "${compiling_for_test}" = "true" ]; then
  extra_args="$extra_args --implicit-cache --build-fast-and-loose=on"
else
  extra_args="$extra_args --release"
fi

if [ "${generating_for_ninja}" = "true" ] && [ "Windows_NT" = "$OS" ]; then
  vcvars="$(vswhere -latest -property installationPath | tr '\\' '/' | dos2unix.exe)/VC/Auxiliary/Build/"
  export PATH="$(echo "$(cd "$vcvars" && cmd /C "vcvarsall.bat amd64 && C:/cygwin/bin/bash -c 'echo \$PATH'")" | tail -n +6)":$PATH
fi
activate_venv

eval ${compile_env} $python ./buildscripts/scons.py \
  ${compile_flags} ${task_compile_flags} ${task_compile_flags_extra} \
  ${scons_cache_args} $extra_args \
  ${targets} MONGO_VERSION=${version} ${patch_compile_flags} || exit_status=$?

# If compile fails we do not run any tests
if [[ $exit_status -ne 0 ]]; then
  touch ${skip_tests}
fi
exit $exit_status

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

activate_venv
export MYPY="$(
  if which cygpath 2> /dev/null; then
    PATH+=":$(cypath "${workdir}")/venv_3/Scripts"
  else
    PATH+=":${workdir}/venv_3/bin"
  fi
  PATH+=':/opt/mongodbtoolchain/v3/bin'
  which mypy
)"
echo "Found mypy executable at '$MYPY'"
export extra_flags=""
eval ${compile_env} python3 ./buildscripts/scons.py ${compile_flags} $extra_flags --stack-size=1024 GITDIFFFLAGS="${revision}" REVISION="${revision}" ENTERPRISE_REV="${enterprise_rev}" ${targets}

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

curator --level warning splunk --json --url=${scons_splunk_server} --token=${scons_splunk_token} --annotation=project:${project} --annotation=task_id:${task_id} --annotation=build_variant:${build_variant} --annotation=git_revision:${revision} pipe < src/scons_cache.log.json

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src

set -o errexit
set -o verbose

# Only run on master branch
if [ "${project}" == "mongodb-mongo-master" -a "${is_patch}" == "true" ]; then
  activate_venv
  PATH=$PATH:$HOME $python buildscripts/selected_tests.py --expansion-file ../expansions.yml --selected-tests-config .selected_tests.yml
fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
. "$DIR/prelude.sh"

cd src/build

set -o errexit
set -o verbose

tar cfvz stitch-support.tgz stitch-support-lib-${version}
