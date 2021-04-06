import click
import functools
from io import StringIO
import textwrap

import toolz


# TODO(kszucs): use archery.report.JinjaReport instead
class Report:

    def __init__(self, job):
        self.job = job

    def show(self):
        raise NotImplementedError()


class ConsoleReport(Report):
    """Report the status of a Job to the console using click"""

    # output table's header template
    HEADER = '[{state:>7}] {branch:<52} {content:>16}'
    DETAILS = ' └ {url}'

    # output table's row template for assets
    ARTIFACT_NAME = '{artifact:>69} '
    ARTIFACT_STATE = '[{state:>7}]'

    # state color mapping to highlight console output
    COLORS = {
        # from CombinedStatus
        'error': 'red',
        'failure': 'red',
        'pending': 'yellow',
        'success': 'green',
        # custom state messages
        'ok': 'green',
        'missing': 'red'
    }

    def lead(self, state, branch, n_uploaded, n_expected):
        line = self.HEADER.format(
            state=state.upper(),
            branch=branch,
            content='uploaded {} / {}'.format(n_uploaded, n_expected)
        )
        return click.style(line, fg=self.COLORS[state.lower()])

    def header(self):
        header = self.HEADER.format(
            state='state',
            branch='Task / Branch',
            content='Artifacts'
        )
        delimiter = '-' * len(header)
        return '{}\n{}'.format(header, delimiter)

    def artifact(self, state, pattern, asset):
        if asset is None:
            artifact = pattern
            state = 'pending' if state == 'pending' else 'missing'
        else:
            artifact = asset.name
            state = 'ok'

        name_ = self.ARTIFACT_NAME.format(artifact=artifact)
        state_ = click.style(
            self.ARTIFACT_STATE.format(state=state.upper()),
            self.COLORS[state]
        )
        return name_ + state_

    def show(self, outstream, asset_callback=None):
        echo = functools.partial(click.echo, file=outstream)

        # write table's header
        echo(self.header())

        # write table's body
        for task_name, task in sorted(self.job.tasks.items()):
            # write summary of the uploaded vs total assets
            status = task.status()
            assets = task.assets()

            # mapping of artifact pattern to asset or None of not uploaded
            n_expected = len(task.artifacts)
            n_uploaded = len(assets.uploaded_assets())
            echo(self.lead(status.combined_state, task_name, n_uploaded,
                           n_expected))

            # show link to the actual build, some of the CI providers implement
            # the statuses API others implement the checks API, so display both
            for s in status.github_status.statuses:
                echo(self.DETAILS.format(url=s.target_url))
            for c in status.github_check_runs:
                echo(self.DETAILS.format(url=c.html_url))

            # write per asset status
            for artifact_pattern, asset in assets.items():
                if asset_callback is not None:
                    asset_callback(task_name, task, asset)
                echo(self.artifact(status.combined_state, artifact_pattern,
                                   asset))


class EmailReport(Report):

    HEADER = textwrap.dedent("""
        Arrow Build Report for Job {job_name}

        All tasks: {all_tasks_url}
    """)

    TASK = textwrap.dedent("""
          - {name}:
            URL: {url}
    """).strip()

    EMAIL = textwrap.dedent("""
        From: {sender_name} <{sender_email}>
        To: {recipient_email}
        Subject: {subject}

        {body}
    """).strip()

    STATUS_HEADERS = {
        # from CombinedStatus
        'error': 'Errored Tasks:',
        'failure': 'Failed Tasks:',
        'pending': 'Pending Tasks:',
        'success': 'Succeeded Tasks:',
    }

    def __init__(self, job, sender_name, sender_email, recipient_email):
        self.sender_name = sender_name
        self.sender_email = sender_email
        self.recipient_email = recipient_email
        super().__init__(job)

    def url(self, query):
        repo_url = self.job.queue.remote_url.strip('.git')
        return '{}/branches/all?query={}'.format(repo_url, query)

    def listing(self, tasks):
        return '\n'.join(
            sorted(
                self.TASK.format(name=task_name, url=self.url(task.branch))
                for task_name, task in tasks.items()
            )
        )

    def header(self):
        url = self.url(self.job.branch)
        return self.HEADER.format(job_name=self.job.branch, all_tasks_url=url)

    def subject(self):
        return (
            "[NIGHTLY] Arrow Build Report for Job {}".format(self.job.branch)
        )

    def body(self):
        buffer = StringIO()
        buffer.write(self.header())

        tasks_by_state = toolz.groupby(
            lambda name_task_pair: name_task_pair[1].status().combined_state,
            self.job.tasks.items()
        )

        for state in ('failure', 'error', 'pending', 'success'):
            if state in tasks_by_state:
                tasks = dict(tasks_by_state[state])
                buffer.write('\n')
                buffer.write(self.STATUS_HEADERS[state])
                buffer.write('\n')
                buffer.write(self.listing(tasks))
                buffer.write('\n')

        return buffer.getvalue()

    def email(self):
        return self.EMAIL.format(
            sender_name=self.sender_name,
            sender_email=self.sender_email,
            recipient_email=self.recipient_email,
            subject=self.subject(),
            body=self.body()
        )

    def show(self, outstream):
        outstream.write(self.email())

    def send(self, smtp_user, smtp_password, smtp_server, smtp_port):
        import smtplib

        email = self.email()

        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.ehlo()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, self.recipient_email, email)
        server.close()
