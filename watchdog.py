import renderer
from query import fetch
import argparse
import notify

parser = argparse.ArgumentParser()
parser.add_argument("path")
args = parser.parse_args()

eli_path = args.path

results = fetch(eli_path)
renderer.render(results)
notify.notify_broken_imagery(results)
