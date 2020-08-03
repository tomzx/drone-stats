import json
import logging
from argparse import ArgumentParser
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


@dataclass
class ApiInfo:
    url: str
    token: str


def drone_get(url: str, api_info: ApiInfo) -> Optional[Dict[str, Any]]:
    logger.debug(f"Querying {url}")

    hash = sha1()
    hash.update(url.encode("utf-8"))

    cache_file = Path("cache") / f"{hash.hexdigest()}.json"
    if cache_file.exists():
        logger.debug(f"Cache hit @ {cache_file}")
        with cache_file.open("r") as f:
            output = json.load(f)
    else:
        logger.debug("Querying API")
        headers = {"Authorization": f"Bearer {api_info.token}"}
        response = requests.get(f"{api_info.url}/api/{url}", headers=headers)

        cache_file.write_text(response.text)

        output = response.json()

    return output


def get_drone_builds(
    api_info: ApiInfo, organization: str, repository: str
) -> Optional[Dict[str, Any]]:
    return drone_get(f"repos/{organization}/{repository}/builds", api_info)


def get_drone_build(
    api_info: ApiInfo, organization: str, repository: str, build_id: int
) -> Optional[Dict[str, Any]]:
    return drone_get(f"repos/{organization}/{repository}/builds/{build_id}", api_info)


def get_drone_build_logs(
    api_info: ApiInfo, organization: str, repository: str, build_id: int, pid: int
) -> Optional[Dict[str, Any]]:
    return drone_get(
        f"repos/{organization}/{repository}/logs/{build_id}/{pid}", api_info
    )


logging.basicConfig(level=logging.DEBUG)

Path("cache").mkdir(exist_ok=True)

argument_parser = ArgumentParser()

argument_parser.add_argument(
    "organization", help="Organization (e.g., my-organization)"
)
argument_parser.add_argument("repository", help="Repository (e.g., my-repository)")
argument_parser.add_argument(
    "url", help="URL to the API (e.g., https://drone.yourcompany.com"
)
argument_parser.add_argument("token", help="Drone token")

args = argument_parser.parse_args()

organization = args.organization
repository = args.repository
api_info = ApiInfo(url=args.url, token=args.token)

# Get last drone build
drone_builds = get_drone_builds(api_info, organization, repository)

last_build_number = drone_builds[0]["number"]

logger.debug(f"Last build number is {last_build_number}")

builds = {}
for build_number in range(1, last_build_number + 1):
    builds[build_number] = get_drone_build(
        api_info, organization, repository, build_number
    )

# TODO(tom@tomrochette.com): Duration of build
build_info = {}
for build_number, build in builds.items():
    if "procs" not in build:
        continue

    if len(build["procs"]) == 0:
        continue

    if "children" not in build["procs"][0]:
        continue

    durations = {}
    for child in build["procs"][0]["children"]:
        pid_name = child["name"]
        if child["state"] == "skipped":
            duration = 0
        else:
            duration = child["end_time"] - child["start_time"]
        durations[pid_name] = duration

    build_data = {"build_number": build_number, "branch": build["branch"], **durations}

    build_info[build_number] = build_data

build_info = pd.DataFrame.from_records(list(build_info.values()))
output_filename = f"{repository}.csv"
build_info.to_csv(output_filename, index=False)
logger.info(f"Wrote {output_filename}")
