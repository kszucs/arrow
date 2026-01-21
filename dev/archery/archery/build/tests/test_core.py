# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import tempfile
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest

from archery.build.core import (
    Builds,
    BuildError,
    Job,
    Workflow,
    _expand_matrix,
)


class TestJob:
    def test_init(self):
        job = Job(id='test-job')
        assert job.id == 'test-job'
        assert job.params == {}

    def test_init_with_params(self):
        job = Job(id='test-job', params={'key': 'value'})
        assert job.id == 'test-job'
        assert job.params == {'key': 'value'}


class TestWorkflow:
    def test_init(self):
        workflow = Workflow(
            name='Test Workflow',
            filename='test.yml',
            path=Path('/tmp/test.yml'),
            jobs=[]
        )
        assert workflow.name == 'Test Workflow'
        assert workflow.filename == 'test.yml'
        assert workflow.jobs == []

    def test_job_ids(self):
        workflow = Workflow(
            name='Test',
            filename='test.yml',
            path=Path('/tmp/test.yml'),
            jobs=[
                Job(id='job-a'),
                Job(id='job-b'),
                Job(id='job-c'),
            ]
        )
        assert workflow.job_ids() == ['job-a', 'job-b', 'job-c']

    def test_match_jobs_exact(self):
        workflow = Workflow(
            name='Test',
            filename='test.yml',
            path=Path('/tmp/test.yml'),
            jobs=[
                Job(id='build-linux'),
                Job(id='build-macos'),
                Job(id='test-linux'),
            ]
        )
        matched = workflow.match_jobs(['build-linux'])
        assert matched == ['build-linux']

    def test_match_jobs_glob_prefix(self):
        workflow = Workflow(
            name='Test',
            filename='test.yml',
            path=Path('/tmp/test.yml'),
            jobs=[
                Job(id='build-linux'),
                Job(id='build-macos'),
                Job(id='test-linux'),
            ]
        )
        matched = workflow.match_jobs(['build-*'])
        assert matched == ['build-linux', 'build-macos']

    def test_match_jobs_glob_suffix(self):
        workflow = Workflow(
            name='Test',
            filename='test.yml',
            path=Path('/tmp/test.yml'),
            jobs=[
                Job(id='build-linux'),
                Job(id='build-macos'),
                Job(id='test-linux'),
            ]
        )
        matched = workflow.match_jobs(['*-linux'])
        assert matched == ['build-linux', 'test-linux']

    def test_match_jobs_glob_contains(self):
        workflow = Workflow(
            name='Test',
            filename='test.yml',
            path=Path('/tmp/test.yml'),
            jobs=[
                Job(id='example-python-build-fedora'),
                Job(id='example-python-build-ubuntu'),
                Job(id='example-cpp-build-fedora'),
            ]
        )
        matched = workflow.match_jobs(['*python*'])
        assert matched == ['example-python-build-fedora', 'example-python-build-ubuntu']

    def test_match_jobs_multiple_patterns(self):
        workflow = Workflow(
            name='Test',
            filename='test.yml',
            path=Path('/tmp/test.yml'),
            jobs=[
                Job(id='build-linux'),
                Job(id='build-macos'),
                Job(id='test-linux'),
            ]
        )
        matched = workflow.match_jobs(['build-linux', 'test-*'])
        assert matched == ['build-linux', 'test-linux']

    def test_match_jobs_no_match(self):
        workflow = Workflow(
            name='Test',
            filename='test.yml',
            path=Path('/tmp/test.yml'),
            jobs=[
                Job(id='build-linux'),
            ]
        )
        matched = workflow.match_jobs(['nonexistent-*'])
        assert matched == []


class TestWorkflowTrigger:
    def test_trigger_success(self):
        mock_workflow = mock.MagicMock()
        mock_repo = mock.MagicMock()
        mock_repo.get_workflow.return_value = mock_workflow
        mock_client = mock.MagicMock()
        mock_client.get_repo.return_value = mock_repo

        mock_github_module = mock.MagicMock()
        mock_github_module.Github.return_value = mock_client
        mock_github_module.Auth.Token.return_value = mock.MagicMock()

        workflow = Workflow('Test', 'test.yml', Path('test.yml'), [
            Job(id='job-a'),
        ])

        with mock.patch.dict('sys.modules', {'github': mock_github_module}):
            workflow.trigger(
                github_token='token',
                repo_name='owner/repo',
                ref='main',
                job_ids=['job-a']
            )

        mock_repo.get_workflow.assert_called_once_with('test.yml')
        mock_workflow.create_dispatch.assert_called_once_with(
            ref='main',
            inputs={'jobs': 'job-a'}
        )

    def test_trigger_all_jobs(self):
        mock_workflow = mock.MagicMock()
        mock_repo = mock.MagicMock()
        mock_repo.get_workflow.return_value = mock_workflow
        mock_client = mock.MagicMock()
        mock_client.get_repo.return_value = mock_repo

        mock_github_module = mock.MagicMock()
        mock_github_module.Github.return_value = mock_client
        mock_github_module.Auth.Token.return_value = mock.MagicMock()

        workflow = Workflow('Test', 'test.yml', Path('test.yml'), [
            Job(id='job-a'),
            Job(id='job-b'),
        ])

        with mock.patch.dict('sys.modules', {'github': mock_github_module}):
            workflow.trigger(
                github_token='token',
                repo_name='owner/repo',
                ref='main',
                job_ids=None
            )

        mock_workflow.create_dispatch.assert_called_once_with(
            ref='main',
            inputs={}
        )

    def test_trigger_github_error(self):
        mock_repo = mock.MagicMock()
        mock_repo.get_workflow.side_effect = Exception("API Error")
        mock_client = mock.MagicMock()
        mock_client.get_repo.return_value = mock_repo

        mock_github_module = mock.MagicMock()
        mock_github_module.GithubException = Exception
        mock_github_module.Github.return_value = mock_client
        mock_github_module.Auth.Token.return_value = mock.MagicMock()

        workflow = Workflow('Test', 'test.yml', Path('test.yml'), [
            Job(id='job-a'),
        ])

        with mock.patch.dict('sys.modules', {'github': mock_github_module}):
            with pytest.raises(BuildError, match="Failed to trigger"):
                workflow.trigger(
                    github_token='token',
                    repo_name='owner/repo',
                    ref='main',
                    job_ids=['job-a']
                )


class TestExpandMatrix:
    def test_expand_include(self):
        matrix = {
            'include': [
                {'id': 'job-a', 'os': 'linux'},
                {'id': 'job-b', 'os': 'macos'},
            ]
        }
        entries = _expand_matrix(matrix)
        assert len(entries) == 2
        assert entries[0]['id'] == 'job-a'
        assert entries[1]['id'] == 'job-b'

    def test_expand_include_without_id(self):
        matrix = {
            'include': [
                {'id': 'job-a', 'os': 'linux'},
                {'os': 'macos'},  # No id, should be filtered out
            ]
        }
        entries = _expand_matrix(matrix)
        assert len(entries) == 1
        assert entries[0]['id'] == 'job-a'

    def test_expand_empty_matrix(self):
        entries = _expand_matrix({})
        assert entries == []

    def test_expand_none_matrix(self):
        entries = _expand_matrix(None)
        assert entries == []


class TestParseWorkflow:
    def test_parse_valid_workflow(self):
        workflow_content = dedent("""
            name: Test Workflow

            on:
              workflow_dispatch:
                inputs:
                  jobs:
                    description: "Jobs to run"
                    required: false
                    default: ""

            jobs:
              build:
                runs-on: ubuntu-latest
                strategy:
                  matrix:
                    include:
                      - id: job-a
                        os: linux
                      - id: job-b
                        os: macos
                steps:
                  - run: echo "test"
        """)

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yml', delete=False
        ) as f:
            f.write(workflow_content)
            f.flush()

            workflow = Workflow.parse(Path(f.name))

            assert workflow is not None
            assert workflow.name == 'Test Workflow'
            assert len(workflow.jobs) == 2
            assert workflow.jobs[0].id == 'job-a'
            assert workflow.jobs[1].id == 'job-b'

    def test_parse_workflow_without_dispatch(self):
        workflow_content = dedent("""
            name: No Dispatch

            on:
              push:
                branches: [main]

            jobs:
              build:
                runs-on: ubuntu-latest
                steps:
                  - run: echo "test"
        """)

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yml', delete=False
        ) as f:
            f.write(workflow_content)
            f.flush()

            workflow = Workflow.parse(Path(f.name))

            assert workflow is None

    def test_parse_workflow_without_jobs_input(self):
        workflow_content = dedent("""
            name: No Jobs Input

            on:
              workflow_dispatch:
                inputs:
                  other_input:
                    description: "Some other input"

            jobs:
              build:
                runs-on: ubuntu-latest
                steps:
                  - run: echo "test"
        """)

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yml', delete=False
        ) as f:
            f.write(workflow_content)
            f.flush()

            workflow = Workflow.parse(Path(f.name))

            assert workflow is None


class TestBuilds:
    def test_discover_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            builds = Builds.discover(tmpdir)
            assert len(builds) == 0

    def test_discover_nonexistent_dir(self):
        builds = Builds.discover('/nonexistent/path')
        assert len(builds) == 0

    def test_discover_with_workflows(self):
        workflow_content = dedent("""
            name: Test Workflow

            on:
              workflow_dispatch:
                inputs:
                  jobs:
                    description: "Jobs to run"

            jobs:
              build:
                strategy:
                  matrix:
                    include:
                      - id: job-a
                      - id: job-b
                steps:
                  - run: echo "test"
        """)

        with tempfile.TemporaryDirectory() as tmpdir:
            workflow_path = Path(tmpdir) / 'test.yml'
            workflow_path.write_text(workflow_content)

            builds = Builds.discover(tmpdir)

            assert len(builds) == 1
            assert 'test.yml' in builds
            assert len(builds['test.yml'].jobs) == 2

    def test_len(self):
        builds = Builds(workflows={
            'a.yml': Workflow('A', 'a.yml', Path('a.yml'), []),
            'b.yml': Workflow('B', 'b.yml', Path('b.yml'), []),
        })
        assert len(builds) == 2

    def test_iter(self):
        wf_a = Workflow('A', 'a.yml', Path('a.yml'), [])
        wf_b = Workflow('B', 'b.yml', Path('b.yml'), [])
        builds = Builds(workflows={'a.yml': wf_a, 'b.yml': wf_b})

        workflows = list(builds)
        assert len(workflows) == 2
        assert wf_a in workflows
        assert wf_b in workflows

    def test_contains(self):
        builds = Builds(workflows={
            'test.yml': Workflow('Test', 'test.yml', Path('test.yml'), [])
        })
        assert 'test.yml' in builds
        assert 'other.yml' not in builds

    def test_getitem(self):
        wf = Workflow('Test', 'test.yml', Path('test.yml'), [])
        builds = Builds(workflows={'test.yml': wf})
        assert builds['test.yml'] is wf

    def test_get(self):
        wf = Workflow('Test', 'test.yml', Path('test.yml'), [])
        builds = Builds(workflows={'test.yml': wf})
        assert builds.get('test.yml') is wf
        assert builds.get('other.yml') is None
        assert builds.get('other.yml', 'default') == 'default'

    def test_filenames(self):
        builds = Builds(workflows={
            'a.yml': Workflow('A', 'a.yml', Path('a.yml'), []),
            'b.yml': Workflow('B', 'b.yml', Path('b.yml'), []),
        })
        assert set(builds.filenames()) == {'a.yml', 'b.yml'}

    def test_all_job_ids(self):
        builds = Builds(workflows={
            'a.yml': Workflow('A', 'a.yml', Path('a.yml'), [
                Job(id='a-job-1'),
                Job(id='a-job-2'),
            ]),
            'b.yml': Workflow('B', 'b.yml', Path('b.yml'), [
                Job(id='b-job-1'),
            ]),
        })
        job_ids = builds.all_job_ids()
        assert set(job_ids) == {'a-job-1', 'a-job-2', 'b-job-1'}

    def test_match_jobs_single_pattern(self):
        builds = Builds(workflows={
            'test.yml': Workflow('Test', 'test.yml', Path('test.yml'), [
                Job(id='build-linux'),
                Job(id='build-macos'),
                Job(id='test-linux'),
            ])
        })
        matched = builds.match_jobs(['build-*'])
        assert matched == {'test.yml': ['build-linux', 'build-macos']}

    def test_match_jobs_multiple_patterns(self):
        builds = Builds(workflows={
            'test.yml': Workflow('Test', 'test.yml', Path('test.yml'), [
                Job(id='build-linux'),
                Job(id='build-macos'),
                Job(id='test-linux'),
            ])
        })
        matched = builds.match_jobs(['build-linux', '*-macos'])
        assert matched == {'test.yml': ['build-linux', 'build-macos']}

    def test_match_jobs_with_workflow_prefix(self):
        builds = Builds(workflows={
            'a.yml': Workflow('A', 'a.yml', Path('a.yml'), [
                Job(id='job-1'),
            ]),
            'b.yml': Workflow('B', 'b.yml', Path('b.yml'), [
                Job(id='job-1'),
            ]),
        })
        matched = builds.match_jobs(['a.yml/*'])
        assert matched == {'a.yml': ['job-1']}
        assert 'b.yml' not in matched

    def test_match_jobs_workflow_glob(self):
        builds = Builds(workflows={
            'python_build.yml': Workflow('Python', 'python_build.yml',
                                         Path('python_build.yml'), [
                                             Job(id='job-1'),
                                         ]),
            'cpp_build.yml': Workflow('C++', 'cpp_build.yml',
                                      Path('cpp_build.yml'), [
                                          Job(id='job-1'),
                                      ]),
        })
        matched = builds.match_jobs(['*_build.yml/*'])
        assert 'python_build.yml' in matched
        assert 'cpp_build.yml' in matched

    def test_match_jobs_across_workflows(self):
        builds = Builds(workflows={
            'a.yml': Workflow('A', 'a.yml', Path('a.yml'), [
                Job(id='common-job'),
                Job(id='a-only'),
            ]),
            'b.yml': Workflow('B', 'b.yml', Path('b.yml'), [
                Job(id='common-job'),
                Job(id='b-only'),
            ]),
        })
        matched = builds.match_jobs(['common-job'])
        assert matched == {
            'a.yml': ['common-job'],
            'b.yml': ['common-job'],
        }

    def test_match_jobs_no_match(self):
        builds = Builds(workflows={
            'test.yml': Workflow('Test', 'test.yml', Path('test.yml'), [
                Job(id='build-linux'),
            ])
        })
        matched = builds.match_jobs(['nonexistent-*'])
        assert matched == {}


class TestBuildsIntegration:
    def test_trigger_no_match(self):
        builds = Builds(workflows={
            'test.yml': Workflow('Test', 'test.yml', Path('test.yml'), [
                Job(id='job-a'),
            ])
        })

        with pytest.raises(BuildError, match="No jobs matched"):
            builds.trigger(
                github_token='token',
                repo_name='owner/repo',
                job_patterns=['nonexistent-*'],
                ref='main'
            )

    def test_trigger_dry_run(self):
        builds = Builds(workflows={
            'test.yml': Workflow('Test', 'test.yml', Path('test.yml'), [
                Job(id='job-a'),
                Job(id='job-b'),
            ])
        })

        results = builds.trigger(
            github_token='token',
            repo_name='owner/repo',
            job_patterns=['job-a'],
            ref='main',
            dry_run=True
        )

        assert len(results) == 1
        assert results[0]['status'] == 'dry-run'
        assert results[0]['workflow'] == 'Test'
        assert results[0]['filename'] == 'test.yml'
        assert results[0]['jobs'] == ['job-a']
        assert results[0]['ref'] == 'main'

    def test_trigger_dry_run_multiple_workflows(self):
        builds = Builds(workflows={
            'a.yml': Workflow('A', 'a.yml', Path('a.yml'), [
                Job(id='common-job'),
            ]),
            'b.yml': Workflow('B', 'b.yml', Path('b.yml'), [
                Job(id='common-job'),
            ]),
        })

        results = builds.trigger(
            github_token='token',
            repo_name='owner/repo',
            job_patterns=['common-job'],
            ref='main',
            dry_run=True
        )

        assert len(results) == 2
        assert all(r['status'] == 'dry-run' for r in results)
        filenames = {r['filename'] for r in results}
        assert filenames == {'a.yml', 'b.yml'}

    def test_trigger_success(self):
        mock_workflow = mock.MagicMock()
        mock_repo = mock.MagicMock()
        mock_repo.get_workflow.return_value = mock_workflow
        mock_client = mock.MagicMock()
        mock_client.get_repo.return_value = mock_repo

        mock_github_module = mock.MagicMock()
        mock_github_module.Github.return_value = mock_client
        mock_github_module.Auth.Token.return_value = mock.MagicMock()

        builds = Builds(workflows={
            'test.yml': Workflow('Test', 'test.yml', Path('test.yml'), [
                Job(id='job-a'),
            ])
        })

        with mock.patch.dict('sys.modules', {'github': mock_github_module}):
            results = builds.trigger(
                github_token='token',
                repo_name='owner/repo',
                job_patterns=['job-a'],
                ref='main',
                dry_run=False
            )

        assert len(results) == 1
        assert results[0]['status'] == 'triggered'
        assert results[0]['jobs'] == ['job-a']

        mock_repo.get_workflow.assert_called_once_with('test.yml')
        mock_workflow.create_dispatch.assert_called_once_with(
            ref='main',
            inputs={'jobs': 'job-a'}
        )

    def test_trigger_multiple_jobs(self):
        mock_workflow = mock.MagicMock()
        mock_repo = mock.MagicMock()
        mock_repo.get_workflow.return_value = mock_workflow
        mock_client = mock.MagicMock()
        mock_client.get_repo.return_value = mock_repo

        mock_github_module = mock.MagicMock()
        mock_github_module.Github.return_value = mock_client
        mock_github_module.Auth.Token.return_value = mock.MagicMock()

        builds = Builds(workflows={
            'test.yml': Workflow('Test', 'test.yml', Path('test.yml'), [
                Job(id='job-a'),
                Job(id='job-b'),
                Job(id='job-c'),
            ])
        })

        with mock.patch.dict('sys.modules', {'github': mock_github_module}):
            results = builds.trigger(
                github_token='token',
                repo_name='owner/repo',
                job_patterns=['job-a', 'job-b'],
                ref='feature-branch',
                dry_run=False
            )

        assert len(results) == 1
        assert results[0]['status'] == 'triggered'
        assert set(results[0]['jobs']) == {'job-a', 'job-b'}

        mock_workflow.create_dispatch.assert_called_once()
        call_args = mock_workflow.create_dispatch.call_args
        assert call_args[1]['ref'] == 'feature-branch'
        # Jobs should be comma-separated
        jobs_input = call_args[1]['inputs']['jobs']
        assert 'job-a' in jobs_input
        assert 'job-b' in jobs_input

    def test_trigger_github_error(self):
        mock_repo = mock.MagicMock()
        mock_repo.get_workflow.side_effect = Exception("API Error")
        mock_client = mock.MagicMock()
        mock_client.get_repo.return_value = mock_repo

        mock_github_module = mock.MagicMock()
        mock_github_module.GithubException = Exception
        mock_github_module.Github.return_value = mock_client
        mock_github_module.Auth.Token.return_value = mock.MagicMock()

        builds = Builds(workflows={
            'test.yml': Workflow('Test', 'test.yml', Path('test.yml'), [
                Job(id='job-a'),
            ])
        })

        with mock.patch.dict('sys.modules', {'github': mock_github_module}):
            results = builds.trigger(
                github_token='token',
                repo_name='owner/repo',
                job_patterns=['job-a'],
                ref='main',
                dry_run=False
            )

        assert len(results) == 1
        assert results[0]['status'] == 'failed'
        assert 'error' in results[0]
