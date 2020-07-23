import glob
import json
import multiprocessing
import os
import certifi
import requests
from shapely.geometry import shape
import mercantile
from owslib.wms import WebMapService
from owslib.wmts import WebMapTileService
import warnings
import re

cpus = multiprocessing.cpu_count()

GOOD = 'good'


def get_status_code(url):
    """
    Check status code of url
    """
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 6.0; ELI Image check)'}
    try:
        r = requests.get(url,
                         headers=headers,
                         verify=certifi.where())
        if r.status_code == 200:
            return GOOD
        else:
            return "HTTP Code: {}".format(r.status_code)
    except:
        # Try without SSL verification
        try:
            r = requests.get(url,
                             headers=headers,
                             verify=False)
            if r.status_code == 200:
                return GOOD
            else:
                return "HTTP Code: {}".format(r.status_code)
        except Exception as e:
            return str(e)
    return None


def process_source(filename):
    result = {}
    path_split = filename.split(os.sep)
    sources_index = path_split.index("sources")

    result['filename'] = path_split[-1]
    result['directory'] = path_split[sources_index + 1:-1]

    with open(filename) as fp:
        source = json.load(fp)

    result['name'] = source['properties']['name']

    # Check licence url
    if 'license_url' not in source['properties']:
        result['license_url'] = "No license_url set!"
    else:
        licence_url = source['properties']['license_url']
        result['license_url'] = get_status_code(licence_url)

    # Check privacy url
    if 'privacy_policy_url' not in source['properties']:
        result['privacy_policy_url'] = "No privacy_policy_url set!"
    else:
        privacy_policy_url = source['properties']['privacy_policy_url']
        result['privacy_policy_url'] = get_status_code(privacy_policy_url)

    # Check imagery
    if source['geometry'] is not None:
        geom = shape(source['geometry'])
        centroid = geom.centroid

        try:
            # Check tms
            if source['properties']['type'] == 'tms':
                # TODO deal with {apikey}, {-y}

                try:
                    parameters = {}
                    tms_url = source['properties']['url']

                    if "switch:" in tms_url:
                        match = re.search(r'switch:?([^}]*)', tms_url)
                        switches = match.group(1).split(',')
                        tms_url = tms_url.replace(match.group(0), 'switch')
                        parameters['switch'] = switches[0]

                    zoom = 0
                    if 'min_zoom' in source['properties']:
                        zoom = source['properties']['min_zoom']
                    parameters['zoom'] = zoom

                    tile = mercantile.tile(centroid.x, centroid.y, zoom)
                    parameters['x'] = tile.x
                    parameters['y'] = tile.y

                    if '{-y}' in tms_url:
                        tms_url = tms_url.replace('{-y}', '{y}')

                    tms_url = tms_url.format(**parameters)
                    result['imagery'] = get_status_code(tms_url)
                except Exception as e:
                    result['imagery'] = str(e)

            # check wms
            elif source['properties']['type'] == 'wms':

                wms_url = source['properties']['url']
                wms_url_split = wms_url.rsplit('?', 1)
                wms_args_list = wms_url_split[1].split("&")
                wms_args = {}
                for wms_arg in wms_args_list:
                    k, v = wms_arg.split("=")
                    wms_args[k.lower()] = v

                get_capabilities_args = {'service': 'WMS',
                                         'request': 'GetCapabilities'}
                if 'map' in wms_args:
                    get_capabilities_args['map'] = wms_args['map']

                wms_base_url = wms_url_split[0]
                wms_args_str = "&".join(["{}={}".format(k, v) for k, v in get_capabilities_args.items()])
                wms_getcapabilites_url = "{}?{}".format(wms_base_url, wms_args_str)

                if not 'layers' in wms_args:
                    result['imagery'] = "WMS url contains no layers"
                else:

                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            try:
                                wms = WebMapService(wms_getcapabilites_url, version="1.3.0")
                            except:
                                wms = WebMapService(wms_getcapabilites_url, version="1.1.1")

                        layer_arg = wms_args['layers']
                        not_found_layers = []
                        for layer_name in layer_arg.split(","):
                            if not layer_name in wms.contents:
                                not_found_layers.append(layer_name)
                        if len(not_found_layers) > 0:
                            result['imagery'] = "Layers {layers} could not be found under url '{url}'.".format(
                                layers=",".join(not_found_layers),
                                url=wms_getcapabilites_url)
                        else:
                            result['imagery'] = GOOD

                    except Exception as e:
                        result['imagery'] = str(e)

            # check wms_endpoint
            elif source['properties']['type'] == 'wms_endpoint':
                # TODO assumptions
                wms_url = source['properties']['url']
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        wms = WebMapService(wms_url)
                        result['imagery'] = GOOD
                except Exception as e:
                    result['imagery'] = str(e)

            # check wmts
            elif source['properties']['type'] == 'wmts':
                try:
                    wmts_url = source['properties']['url']
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        wmts = WebMapTileService(wmts_url)
                        result['imagery'] = GOOD
                except Exception as e:
                    result['imagery'] = str(e)

            else:
                result['imagery'] = None
        except Exception as e:
            result['imagery'] = str(e)

    print(result)
    return result


def fetch(eli_path):
    jobs = []
    for filename in glob.glob(os.path.join(eli_path, '**', '*.geojson'), recursive=True):
        jobs.append(filename)

    with multiprocessing.Pool(processes=cpus) as pool:
        results = pool.map(process_source, jobs)
    return results
