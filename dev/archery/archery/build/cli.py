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

import click

from .core import Builds, BuildError
from ..utils.source import ArrowSources


_default_arrow_path = ArrowSources.find().path
_default_workflows_dir = _default_arrow_path / ".github" / "workflows"


@click.group()
@click.option('--github-token', '-t', default=None,
              envvar=['CROSSBOW_GITHUB_TOKEN', 'GH_TOKEN'],
              help='OAuth token for GitHub authentication')
@click.option('--workflows-dir', '-w',
              type=click.Path(exists=True),
              default=_default_workflows_dir,
              help='Path to .github/workflows directory')
@click.pass_context
def build(ctx, github_token, workflows_dir):
    """
    Trigger build workflows on GitHub Actions.

    This command allows you to trigger GitHub Actions workflows
    directly, without the crossbow infrastructure.
    """
    ctx.ensure_object(dict)
    ctx.obj['github_token'] = github_token
    ctx.obj['workflows_dir'] = workflows_dir


@build.command()
@click.argument('jobs', nargs=-1, required=True)
@click.option('--repo', '-r', required=True,
              help='GitHub repository in owner/repo format '
                   '(e.g., kszucs/arrow)')
@click.option('--ref', '-b', default='main',
              help='Git ref (branch or tag) to run the workflow on')
@click.option('--dry-run/--no-dry-run', default=False,
              help='Show what would be triggered without actually triggering')
@click.pass_obj
def submit(obj, jobs, repo, ref, dry_run):
    """
    Submit build jobs to GitHub Actions.

    JOBS are job IDs or patterns matching the 'id' field in workflow matrices.
    You can use glob patterns like 'example-*-conda' or prefix with workflow
    filename like 'python_minimal.yml/example-*'.

    \b
    Examples:
        # Submit a single job
        archery build submit example-python-minimal-build-fedora-conda \\
            --repo kszucs/arrow --ref my-branch

        # Submit multiple jobs with pattern matching
        archery build submit 'example-python-minimal-build-*-conda' \\
            --repo kszucs/arrow --ref main

        # Submit jobs from a specific workflow
        archery build submit 'python_minimal.yml/*' \\
            --repo kszucs/arrow --ref main

        # Dry run to see what would be triggered
        archery build submit 'example-*' --repo kszucs/arrow --dry-run
    """
    github_token = obj['github_token']
    workflows_dir = obj['workflows_dir']

    if not github_token:
        raise click.ClickException(
            "GitHub token is required. Set GITHUB_TOKEN or GH_TOKEN "
            "environment variable, or use --github-token option."
        )

    # Discover workflows and trigger
    try:
        builds = Builds.discover(workflows_dir)
        results = builds.trigger(
            github_token=github_token,
            repo_name=repo,
            job_patterns=list(jobs),
            ref=ref,
            dry_run=dry_run
        )
    except BuildError as e:
        raise click.ClickException(str(e))

    # Display results
    for result in results:
        workflow = result['workflow']
        filename = result['filename']
        status = result['status']
        result_jobs = result.get('jobs', [])

        if status == 'triggered':
            click.echo(f"✓ Triggered workflow: {workflow}")
            click.echo(f"  File: {filename}")
            click.echo(f"  Ref: {ref}")
            click.echo(f"  Jobs: {', '.join(result_jobs)}")
        elif status == 'dry-run':
            click.echo(f"○ Would trigger workflow: {workflow} (dry-run)")
            click.echo(f"  File: {filename}")
            click.echo(f"  Ref: {ref}")
            click.echo(f"  Jobs: {', '.join(result_jobs)}")
        elif status == 'failed':
            error = result.get('error', 'unknown error')
            click.echo(f"✗ Failed to trigger: {workflow}")
            click.echo(f"  Error: {error}")

    # Summary
    click.echo()
    triggered = sum(1 for r in results if r['status'] == 'triggered')
    dry_runs = sum(1 for r in results if r['status'] == 'dry-run')
    failed = sum(1 for r in results if r['status'] == 'failed')

    if dry_run:
        click.echo(f"Dry run complete: {dry_runs} workflow(s) would be triggered")
    else:
        if failed:
            click.echo(f"Triggered: {triggered}, Failed: {failed}")
        else:
            click.echo(f"Successfully triggered {triggered} workflow(s)")

    if failed > 0:
        raise SystemExit(1)


@build.command('list')
@click.argument('patterns', nargs=-1, required=False)
@click.pass_obj
def list_jobs(obj, patterns):
    """
    List available workflows and jobs.

    If PATTERNS are given, only show matching jobs.

    \b
    Examples:
        # List all workflows and jobs
        archery build list

        # List jobs matching a pattern
        archery build list 'example-*-conda'

        # List jobs from a specific workflow
        archery build list 'python_minimal.yml/*'
    """
    workflows_dir = obj['workflows_dir']

    try:
        builds = Builds.discover(workflows_dir)
    except Exception as e:
        raise click.ClickException(f"Failed to discover workflows: {e}")

    if not builds:
        click.echo("No dispatchable workflows found.")
        click.echo(f"Looked in: {workflows_dir}")
        return

    if patterns:
        matched = builds.match_jobs(patterns)

        if not matched:
            click.echo("No jobs matched the given patterns.")
            return

        click.echo("Matched jobs:\n")
        for workflow_filename, job_ids in sorted(matched.items()):
            workflow = builds[workflow_filename]
            click.echo(f"  {workflow.name} ({workflow_filename})")
            for job_id in job_ids:
                click.echo(f"    - {job_id}")
            click.echo()
    else:
        # List all workflows and jobs
        click.echo("Available workflows and jobs:\n")
        for workflow in builds:
            click.echo(f"  {workflow.name} ({workflow.filename})")
            for job in workflow.jobs:
                click.echo(f"    - {job.id}")
            click.echo()
