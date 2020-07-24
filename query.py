import asyncio
import glob
import json
import os
import aiofiles
import aiohttp
from shapely.geometry import shape, Point
import mercantile
from owslib.wms import WebMapService
from owslib.wmts import WebMapTileService
import warnings
import re
from aiohttp import ClientSession

GOOD = 'good'


async def test_url(url: str, session: ClientSession, **kwargs):
    try:
        resp = await session.request(method="GET", url=url, **kwargs)
        status_code = resp.status
        if status_code == 200:
            return GOOD
        else:
            return "HTTP Code {} for {}".format(status_code, url)
    except asyncio.TimeoutError:
        return "Timeout for : {}".format(url)
    except Exception as e:
        return repr(e)


async def check_tms(source, session: ClientSession, **kwargs):
    # TODO deal with {apikey}, {-y}
    try:

        geom = shape(source['geometry'])
        centroid = geom.centroid

        tms_url = source['properties']['url']
        parameters = {}

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
        tms_url_status = await test_url(tms_url, session)
        return tms_url_status

    except Exception as e:
        return repr(e)


async def check_wms(source, session: ClientSession):
    wms_url = source['properties']['url']
    wms_url_split = wms_url.rsplit('?', 1)
    wms_args_list = wms_url_split[1].split("&")
    wms_args = {}
    for wms_arg in wms_args_list:
        k, v = wms_arg.split("=")
        wms_args[k.lower()] = v

    if 'layers' not in wms_args:
        return "No layers specified in: {}".format(wms_url)

    def get_GetCapabilitie_url(wmsversion):
        get_capabilities_args = {'service': 'WMS',
                                 'request': 'GetCapabilities',
                                 'version': wmsversion}
        if 'map' in wms_args:
            get_capabilities_args['map'] = wms_args['map']

        wms_base_url = wms_url_split[0]
        wms_args_str = "&".join(["{}={}".format(k, v) for k, v in get_capabilities_args.items()])
        return "{}?{}".format(wms_base_url, wms_args_str)

    wms = None
    for wmsversion in ['1.3.0', '1.1.1']:  # TODO 1.1.0 not supported by owslib
        try:
            wms_getcapabilites_url = get_GetCapabilitie_url(wmsversion)
            response = await session.request(method="GET", url=wms_getcapabilites_url)
            status_code = response.status
            if status_code == 200:
                xml = await response.text()
                xml = xml.encode('utf-8')
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    wms = WebMapService(wms_getcapabilites_url, xml=xml, version=wmsversion)
        except Exception as e:
            print(repr(e))
            continue

    if wms is None:
        return "Could not access GetCapabilities of {}".format(wms_url_split[0])


    layer_arg = wms_args['layers']
    not_found_layers = []
    for layer_name in layer_arg.split(","):
        if not layer_name in wms.contents:
            not_found_layers.append(layer_name)
    if len(not_found_layers) > 0:
        return "Layers {layers} could not be found under url '{url}'.".format(
            layers=",".join(not_found_layers),
            url=wms_getcapabilites_url)
    else:
        return GOOD


async def check_wms_endpoint(source, session: ClientSession):
    # TODO assumptions
    wms_url = source['properties']['url']
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            async with session.get(wms_url) as response:
                xml = await response.text()
            wms = WebMapService(wms_url, xml=xml.encode('utf-8'))
            return GOOD
    except Exception as e:
        return repr(e)


async def check_wmts(source, session):
    try:
        wmts_url = source['properties']['url']
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            async with session.get(wmts_url) as response:
                xml = await response.text()
                wmts = WebMapTileService(wmts_url, xml=xml.encode('utf-8'))
            return GOOD
    except Exception as e:
        return repr(e)


async def process_source(filename, session: ClientSession):
    result = {}
    path_split = filename.split(os.sep)
    sources_index = path_split.index("sources")

    result['filename'] = path_split[-1]
    result['directory'] = path_split[sources_index + 1:-1]

    async with aiofiles.open(filename, mode='r') as f:
        contents = await f.read()
        source = json.loads(contents)

    result['name'] = source['properties']['name']

    # Check licence url
    if 'license_url' not in source['properties']:
        result['license_url'] = "No license_url set!"
    else:
        licence_url = source['properties']['license_url']
        licence_url_status = await test_url(licence_url, session)
        result['license_url'] = licence_url_status

    # Check privacy url
    if 'privacy_policy_url' not in source['properties']:
        result['privacy_policy_url'] = "No privacy_policy_url set!"
    else:
        privacy_policy_url = source['properties']['privacy_policy_url']
        privacy_policy_url_status = await test_url(privacy_policy_url, session)
        result['privacy_policy_url'] = privacy_policy_url_status

    # Check imagery
    if source['geometry'] is not None:

        # Check tms
        if source['properties']['type'] == 'tms':
            result['imagery'] = await check_tms(source, session)

        # check wms
        elif source['properties']['type'] == 'wms':
            result['imagery'] = await check_wms(source, session)

        # check wms_endpoint
        elif source['properties']['type'] == 'wms_endpoint':
            result['imagery'] = await check_wms_endpoint(source, session)

        # check wmts
        elif source['properties']['type'] == 'wmts':
            result['imagery'] = await check_wmts(source, session)

    print(result)
    return result


async def process(eli_path):
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 6.0; ELI Watchdog)'}
    timeout = aiohttp.ClientTimeout(total=60)
    async with ClientSession(headers=headers, timeout=timeout) as session:
        jobs = []
        for filename in glob.glob(os.path.join(eli_path, '**', '*.geojson'), recursive=True):
            jobs.append(process_source(filename, session))
        result = await asyncio.gather(*jobs)
        return result


def fetch(eli_path):
    return asyncio.run(process(eli_path=eli_path))
