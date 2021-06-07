import datetime
import json
import os
import re

import dateutil.parser
from github import Github
from github.Issue import Issue
from github.Repository import Repository
import pprint

GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "osmlab/editor-layer-index"
CREATE_ISSUE_AFTER_DAYS = 5

ISSUE_OPEN = "open"
ISSUE_CLOSED = "closed"


def query_contributors(repo: Repository, filepath: str) -> set[str]:
    """Query github for all contributors of a source

    Parameters
    ----------
    repo : Repository
        The repository to query
    filepath : str
        Path in ELI that should be queried

    Returns
    -------
    set[str]
        Contributors of source
    """
    contributors = set()
    commits = repo.get_commits(path=filepath)
    for commit in commits:
        if "github-actions" not in commit.author.login:
            contributors.add(commit.author.login)
    return contributors


def create_github_issue(
    repo: Repository, imagery_filepath: str, imagery_name: str, reason: str, days: int
) -> None:
    """Create new github issue

    Parameters
    ----------
    repo : Repository
        The repository to query
    imagery_filepath : str
        The path to the file the issue is created for
    imagery_name : str
        The name of the imagery
    reason : str
        The reason why the imagery is broken
    days : int
        The number of days the source is broken
    """
    try:

        contributors = query_contributors(repo, imagery_filepath)

        title = f'[Watchdog] Imagery "{imagery_name}": {imagery_filepath} broken'
        body_lines = [
            f"Watchdog failed for {days} consecutive days for:",
            f'"{imagery_name}" -> [{imagery_filepath}](https://github.com/{GITHUB_REPO}/tree/gh-pages/{imagery_filepath})',
            "",
            "Reason:",
            f"{reason}",
            "",
            "CC contributors to this imagery:",
            f"{', '.join([f'@{contributor}' for contributor in contributors])}",
        ]
        body = "\n".join(body_lines)
        repo.create_issue(title=title, body=body, labels=["imagery"])
    except Exception as e:
        print(f"Failed to create issue: {e}")


def close_github_issue(repo: Repository, issue: Issue) -> None:
    """Close a github issue

    Parameters
    ----------
    repo : Repository
        The repository
    issue : Issue
        The issue
    """
    try:
        issue.create_comment(f"Imagery seems to work again.")
        issue.edit(state=ISSUE_CLOSED)
    except Exception as e:
        print(f"Failed to close issue: {e}")


def get_watchdog_issues(repo: Repository) -> dict[str, Issue]:
    """Query repository for watchdog issues

    Parameters
    ----------
    repo : Repository
        The repository to query

    Returns
    -------
    dict[str, Issue]
        The issues
    """
    watchdog_issues = {}
    issues = repo.get_issues(state="all", creator="rbuffat")
    for issue in issues:
        if "[Watchdog]" in issue.title:
            imagery_path = re.search(r"sources(.*?)geojson", issue.title).group(0)
            watchdog_issues[imagery_path] = issue
    return watchdog_issues


def notify_broken_imagery(data):
    broken_sources_db = "web/broken.json"

    # Github Settings -> Developer Settings -> Personal access tokens -> Enable repo / public_repo scope
    pa_token = os.environ["PA_TOKEN"]
    g = Github(pa_token)
    repo = g.get_repo(GITHUB_REPO)

    if os.path.exists(broken_sources_db):
        with open(broken_sources_db) as f:
            broken = json.load(f)

        # Get existing watchdog issues (open or closed)
        watchdog_issues = get_watchdog_issues(repo)
        open_watchdog_issues = {
            key: value
            for (key, value) in watchdog_issues.items()
            if value.state == ISSUE_OPEN
        }

        broken_imagery_paths = set()
        for d in data:
            if not d["imagery"]["status"] == "error":
                continue

            source_id = d["id"]
            if source_id not in broken:
                continue

            filepath = f"sources/{'/'.join(d['directory'])}/{d['filename']}"
            broken_imagery_paths.add(filepath)

            imagery_name = d["name"]

            broken_date = dateutil.parser.isoparse(broken[source_id]).date()
            days = (datetime.date.today() - broken_date).days + 1

            print(f"Imagery broken: {d['name']}: Days {days}")
            if days == CREATE_ISSUE_AFTER_DAYS:
                print(f"Imagery broken: Notify!")
                reason = "\n".join(
                    [
                        message
                        for message in d["imagery"]["message"]
                        if "Error" in message
                    ]
                )
                # TODO reopen existing issue instead of creating a new one
                print("Create new issue")
                #create_github_issue(repo, filepath, imagery_name, reason, days)

        print("broken_imagery_paths")
        pprint.pprint(broken_imagery_paths)
        # Close open issues for sources that aren't broken anymore
        for filepath, watchdog_issue in open_watchdog_issues.items():
            print(filepath, filepath not in broken_imagery_paths)
            if filepath not in broken_imagery_paths:
                print("Should close")
                #close_github_issue(repo, watchdog_issue)
