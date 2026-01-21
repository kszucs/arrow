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

"""
Build workflow definitions and GitHub Actions dispatcher.

This module parses GitHub Actions workflow files to discover available
jobs and provides functionality to trigger them via the GitHub API.
"""

import fnmatch
import itertools
from dataclasses import dataclass, field
from pathlib import Path

import yaml


class BuildError(Exception):
    pass


@dataclass
class Job:
    """A single job within a workflow, representing one matrix entry."""
    id: str
    params: dict = field(default_factory=dict)


def _expand_matrix(matrix_config):
    """
    Expand a GitHub Actions matrix configuration into individual job entries.

    Parameters
    ----------
    matrix_config : dict
        The matrix configuration from a workflow file.

    Returns
    -------
    list of dict
        List of expanded matrix entries, each containing an 'id' key.
    """
    if not matrix_config:
        return []

    # Handle 'include' entries directly
    includes = matrix_config.get('include', [])
    if includes:
        # If using include, each entry should have an 'id'
        return [entry for entry in includes if 'id' in entry]

    # Handle cartesian product of matrix dimensions
    # Filter out special keys
    special_keys = {'include', 'exclude'}
    dimensions = {k: v for k, v in matrix_config.items() if k not in special_keys}

    if not dimensions:
        return []

    # Generate cartesian product
    keys = list(dimensions.keys())
    values = [dimensions[k] if isinstance(dimensions[k], list) else [dimensions[k]]
              for k in keys]

    entries = []
    for combo in itertools.product(*values):
        entry = dict(zip(keys, combo))
        # If there's an 'id' dimension, use it; otherwise skip this entry
        if 'id' in entry:
            entries.append(entry)

    return entries


@dataclass
class Workflow:
    """
    A GitHub Actions workflow that can be triggered.

    Attributes
    ----------
    name : str
        Human-readable name of the workflow.
    filename : str
        The workflow filename (e.g., 'python_minimal.yml').
    path : Path
        Full path to the workflow file.
    jobs : list of Job
        List of jobs (matrix entries) defined in this workflow.
    """
    name: str
    filename: str
    path: Path
    jobs: list = field(default_factory=list)

    @classmethod
    def parse(cls, workflow_path):
        """
        Parse a GitHub Actions workflow file and extract job information.

        Parameters
        ----------
        workflow_path : Path
            Path to the workflow YAML file.

        Returns
        -------
        Workflow or None
            Parsed workflow with jobs, or None if not dispatchable.
        """
        workflow_path = Path(workflow_path)

        with open(workflow_path, 'r') as f:
            try:
                content = yaml.safe_load(f)
            except yaml.YAMLError:
                return None

        if not content:
            return None

        # Check if workflow has workflow_dispatch trigger
        on_config = content.get('on', content.get(
            True, {}))  # 'on' can be parsed as True
        if isinstance(on_config, str):
            on_config = {on_config: None}
        elif isinstance(on_config, list):
            on_config = {k: None for k in on_config}

        if not on_config or 'workflow_dispatch' not in on_config:
            return None

        # Check if workflow_dispatch has a 'jobs' input
        dispatch_config = on_config.get('workflow_dispatch') or {}
        inputs = dispatch_config.get('inputs', {})
        if 'jobs' not in inputs:
            return None

        # Get workflow name
        workflow_name = content.get('name', workflow_path.stem)

        # Parse jobs
        jobs_config = content.get('jobs', {})
        all_job_entries = []

        for job_name, job_config in jobs_config.items():
            if not isinstance(job_config, dict):
                continue

            strategy = job_config.get('strategy', {})
            matrix = strategy.get('matrix', {})

            # Expand matrix to get individual job entries
            entries = _expand_matrix(matrix)
            for entry in entries:
                job_id = entry.get('id')
                if job_id:
                    all_job_entries.append(Job(id=job_id, params=entry))

        if not all_job_entries:
            return None

        return cls(
            name=workflow_name,
            filename=workflow_path.name,
            path=workflow_path,
            jobs=all_job_entries
        )

    def job_ids(self):
        """Return list of job IDs."""
        return [job.id for job in self.jobs]

    def match_jobs(self, patterns):
        """
        Match jobs against glob patterns.

        Parameters
        ----------
        patterns : list of str
            Glob patterns to match job IDs against.

        Returns
        -------
        list of str
            List of matching job IDs.
        """
        job_ids = self.job_ids()
        matched = set()

        for pattern in patterns:
            matches = fnmatch.filter(job_ids, pattern)
            matched.update(matches)

        return sorted(matched)

    def trigger(self, github_token, repo_name, ref, job_ids=None):
        """
        Trigger this workflow run via GitHub API.

        Parameters
        ----------
        github_token : str
            GitHub OAuth token for authentication.
        repo_name : str
            Repository name in 'owner/repo' format.
        ref : str
            The git ref (branch or tag) to run the workflow on.
        job_ids : list of str, optional
            List of job IDs to run. If None, all jobs run.

        Raises
        ------
        BuildError
            If trigger fails.
        """
        try:
            import github as gh
        except ImportError:
            raise BuildError(
                "PyGithub is required for triggering workflows. "
                "Install with: pip install pygithub"
            )

        # Build inputs
        inputs = {}
        if job_ids:
            inputs['jobs'] = ','.join(job_ids)

        try:
            auth = gh.Auth.Token(github_token)
            client = gh.Github(auth=auth)
            repo = client.get_repo(repo_name)
            workflow = repo.get_workflow(self.filename)
            workflow.create_dispatch(ref=ref, inputs=inputs)
        except gh.GithubException as e:
            raise BuildError(
                f"Failed to trigger workflow '{self.filename}': {e}"
            )


class Builds:
    """
    Collection of dispatchable GitHub Actions workflows.

    This class discovers workflow files, matches job patterns,
    and triggers workflow runs via the GitHub API.
    """

    def __init__(self, workflows=None):
        """
        Initialize Builds with a dictionary of workflows.

        Parameters
        ----------
        workflows : dict, optional
            Dictionary mapping workflow filenames to Workflow objects.
        """
        self.workflows = workflows or {}

    @classmethod
    def discover(cls, workflows_dir):
        """
        Discover all dispatchable workflows in a directory.

        Parameters
        ----------
        workflows_dir : Path or str
            Path to the .github/workflows directory.

        Returns
        -------
        Builds
            A Builds instance containing discovered workflows.
        """
        workflows_dir = Path(workflows_dir)
        workflows = {}

        if not workflows_dir.exists():
            return cls(workflows)

        for workflow_path in workflows_dir.glob('*.yml'):
            workflow = Workflow.parse(workflow_path)
            if workflow:
                workflows[workflow.filename] = workflow

        # Also check .yaml extension
        for workflow_path in workflows_dir.glob('*.yaml'):
            workflow = Workflow.parse(workflow_path)
            if workflow:
                workflows[workflow.filename] = workflow

        return cls(workflows)

    def __len__(self):
        return len(self.workflows)

    def __iter__(self):
        return iter(self.workflows.values())

    def __contains__(self, filename):
        return filename in self.workflows

    def __getitem__(self, filename):
        return self.workflows[filename]

    def get(self, filename, default=None):
        return self.workflows.get(filename, default)

    def filenames(self):
        """Return list of workflow filenames."""
        return list(self.workflows.keys())

    def all_job_ids(self):
        """Return list of all job IDs across all workflows."""
        job_ids = []
        for workflow in self.workflows.values():
            job_ids.extend(workflow.job_ids())
        return job_ids

    def match_jobs(self, patterns):
        """
        Match job patterns across all workflows.

        Parameters
        ----------
        patterns : list of str
            Job ID patterns (glob patterns supported).
            Can be prefixed with workflow filename like 'python_minimal.yml/example-*'.

        Returns
        -------
        dict
            Dictionary mapping workflow filenames to lists of matched job IDs.
        """
        results = {}

        for pattern in patterns:
            if '/' in pattern:
                # Pattern includes workflow prefix
                workflow_pattern, job_pattern = pattern.split('/', 1)
                matching_workflows = fnmatch.filter(
                    self.workflows.keys(), workflow_pattern)
            else:
                # Match against all workflows
                job_pattern = pattern
                matching_workflows = self.workflows.keys()

            for wf_filename in matching_workflows:
                workflow = self.workflows[wf_filename]
                matched_jobs = workflow.match_jobs([job_pattern])

                if matched_jobs:
                    if wf_filename not in results:
                        results[wf_filename] = set()
                    results[wf_filename].update(matched_jobs)

        # Convert sets to sorted lists
        return {k: sorted(v) for k, v in results.items()}

    def trigger(self, github_token, repo_name, job_patterns, ref, dry_run=False):
        """
        Trigger workflows for jobs matching the given patterns.

        Parameters
        ----------
        github_token : str
            GitHub OAuth token for authentication.
        repo_name : str
            Repository name in 'owner/repo' format.
        job_patterns : list of str
            Job ID patterns to match and trigger.
        ref : str
            Git ref to run the workflows on.
        dry_run : bool
            If True, don't actually trigger, just show what would be done.

        Returns
        -------
        list of dict
            List of trigger results.

        Raises
        ------
        BuildError
            If no jobs match or trigger fails.
        """
        # Match patterns to workflows and jobs
        matched = self.match_jobs(job_patterns)

        if not matched:
            raise BuildError(
                f"No jobs matched patterns: {', '.join(job_patterns)}"
            )

        results = []

        for workflow_filename, job_ids in matched.items():
            workflow = self.workflows[workflow_filename]

            if dry_run:
                results.append({
                    'workflow': workflow.name,
                    'filename': workflow_filename,
                    'ref': ref,
                    'jobs': job_ids,
                    'status': 'dry-run'
                })
            else:
                try:
                    workflow.trigger(
                        github_token=github_token,
                        repo_name=repo_name,
                        ref=ref,
                        job_ids=job_ids
                    )
                    results.append({
                        'workflow': workflow.name,
                        'filename': workflow_filename,
                        'ref': ref,
                        'jobs': job_ids,
                        'status': 'triggered'
                    })
                except BuildError as e:
                    results.append({
                        'workflow': workflow.name,
                        'filename': workflow_filename,
                        'ref': ref,
                        'jobs': job_ids,
                        'status': 'failed',
                        'error': str(e)
                    })

        return results
