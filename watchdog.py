import renderer
from query import fetch
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("path")
args = parser.parse_args()

eli_path = args.path

results = fetch(eli_path)
renderer.render(results)
