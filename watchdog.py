import renderer
from query import fetch

if __name__ == "__main__":
    eli_path = "editor-layer-index/sources"
    results = fetch(eli_path)
    renderer.render(results)
