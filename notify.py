import json
import os

from github.Repository import Repository
from github import Github
import dateutil.parser
import datetime


GITHUB_API_URL = "https://api.github.com"
GITHUB_REPO = "rbuffat/editor-layer-index"


def query_contributors(repo: Repository, filepath: str) -> set[str]:
    """Query github for all contributors of a source

    Parameters
    ----------
    filepath : str
        Path in ELI that should be queried

    Returns
    -------
    set[str]
        Contributors of file
    """
    contributors = set()
    commits = repo.get_commits(path=filepath)
    for commit in commits:
        contributors.add(commit.author.login)
    return contributors


def create_github_issue(
    imagery_filepath: str, imagery_name: str, reason: str, days: int
) -> None:
    """Create new github issue

    Parameters
    ----------
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
        # Github Settings -> Developer Settings -> Personal access tokens -> Enable repo / public_repo scope
        pa_token = os.environ["PA_TOKEN"]
        print(PA_TOKEN, len(pa_token))
        g = Github(pa_token)
        repo = g.get_repo(GITHUB_REPO)

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
            f"{', '.join([f'{contributor}' for contributor in contributors])}",
        ]
        body = "\n".join(body_lines)

        repo.create_issue(title=title, body=body)
    except Exception as e:
        print(f"Failed to create issue: {e}")


def notify_broken_imagery(data):
    broken_sources_db = "web/broken.json"

    if os.path.exists(broken_sources_db):
        with open(broken_sources_db) as f:
            broken = json.load(f)

        for d in data:
            if not d["imagery"]["status"] == "error":
                continue

            source_id = d["id"]
            if source_id not in broken:
                continue

            imagery_name = d["name"]

            broken_date = dateutil.parser.isoparse(broken[source_id]).date()
            days = (datetime.date.today() - broken_date).days + 1

            print(f"Imagery broken: {d['name']}: Days {days}")

            if days > 0 and days < 5:  # TODO
                print(f"Imagery broken: Notify!")
                reason = "\n".join(
                    [
                        message
                        for message in d["imagery"]["message"]
                        if "Error" in message
                    ]
                )
                filepath = f"sources/{'/'.join(d['directory'])}/{d['filename']}"
                create_github_issue(filepath, imagery_name, reason, days)
