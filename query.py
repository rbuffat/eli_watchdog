import asyncio
import datetime
import glob
import json
import os
import aiofiles
import aiohttp
from shapely.geometry import shape
import mercantile
from owslib.wms import WebMapService
from owslib.wmts import WebMapTileService
import warnings
import re
from aiohttp import ClientSession
from urllib.parse import urlparse


class ResultStatus:
    GOOD = "good"
    WARNING = "warning"
    ERROR = "error"


def create_result(status, message):
    return {'status': status,
            'message': message}


response_cache = {}
domain_locks = {}
domain_lock = asyncio.Lock()


async def get_url(url: str, session: ClientSession, with_text=False):
    """ Ensure that only one request is sent to a domain at one point in time and that the same url is not
    queried mor than once.
    """
    o = urlparse(url)
    if len(o.netloc) == 0:
        return create_result(ResultStatus.ERROR, "Could not parse: {}".format(url))

    async with domain_lock:
        if o.netloc not in domain_locks:
            domain_locks[o.netloc] = asyncio.Lock()
        lock = domain_locks[o.netloc]

    async with lock:
        if url not in response_cache:
            try:
                print("GET {}".format(url))
                async with session.request(method="GET", url=url, ssl=False) as response:
                    status = response.status
                    if with_text:
                        text = await response.text()
                        response_cache[url] = (status, text)
                    else:
                        response_cache[url] = (status, None)
            except asyncio.TimeoutError:
                print("Timeout for: {}".format(url))
                response_cache[url] = create_result(ResultStatus.ERROR, "Timeout for: {}".format(url))
            except Exception as e:
                print("Error for: {} ({})".format(url, str(e)))
                response_cache[url] = create_result(ResultStatus.ERROR, "{} for {}".format(repr(e), url))
        else:
            print("Cached {}".format(url))

        return response_cache[url]


async def test_url(url: str, session: ClientSession, **kwargs):
    """
    Test if a url is reachable

    Parameters
    ----------
    url:  str
        Url to test
    session: ClientSession
        aiohttp ClientSession object
    kwargs: kwargs

    Returns
    -------
    dict:
        Result dict created by create_result()
    """
    resp = await get_url(url, session)
    if isinstance(resp, dict):
        return resp
    else:
        status_code = resp[0]
        if status_code == 200:
            status = ResultStatus.GOOD
        else:
            status = ResultStatus.ERROR
        message = "HTTP Code {} for {}".format(status_code, url)
        return create_result(status, message)


async def check_tms(source, session: ClientSession, **kwargs):
    """
    Check TMS source

    Parameters
    ----------
    source : dict
        Source dictionary
    session : ClientSession
        aiohttp ClientSession object
    kwargs

    Returns
    -------
    dict:
        Result dict created by create_result()
    """
    # TODO deal with {apikey}
    # TODO check zoom levels
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

        zoom = 16
        if 'min_zoom' in source['properties']:
            zoom = max(zoom, source['properties']['min_zoom'])
        if 'max_zoom' in source['properties']:
            zoom = min(zoom, source['properties']['max_zoom'])
        parameters['zoom'] = zoom

        tile = mercantile.tile(centroid.x, centroid.y, zoom)
        parameters['x'] = tile.x
        parameters['y'] = tile.y

        if '{-y}' in tms_url:
            tms_url = tms_url.replace('{-y}', '{y}')
            parameters['y'] = 2 ** parameters['zoom'] - 1 - tile.y
        elif '{!y}' in tms_url:
            tms_url = tms_url.replace('{!y}', '{y}')
            parameters['y'] = 2 ** (parameters['zoom'] - 1) - 1 - tile.y
        else:
            parameters['y'] = tile.y

        tms_url = tms_url.format(**parameters)
        tms_url_status = await test_url(tms_url, session)
        return tms_url_status

    except Exception as e:
        return create_result(ResultStatus.ERROR, repr(e))


async def check_wms(source, session: ClientSession):
    """
    Check WMS source

    Parameters
    ----------
    source : dict
        Source dictionary
    session : ClientSession
        aiohttp ClientSession object
    kwargs

    Returns
    -------
    dict:
        Result dict created by create_result()
    """
    wms_url = source['properties']['url']
    wms_url_split = wms_url.rsplit('?', 1)
    wms_args_list = wms_url_split[1].split("&")
    wms_args = {}
    for wms_arg in wms_args_list:
        k, v = wms_arg.split("=")
        wms_args[k.lower()] = v

    if 'layers' not in wms_args:
        return create_result(ResultStatus.ERROR, "No layers specified in: {}".format(wms_url))

    def get_getcapabilitie_url(wmsversion):
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
            wms_getcapabilites_url = get_getcapabilitie_url(wmsversion)
            response = await get_url(wms_getcapabilites_url, session, with_text=True)
            if isinstance(response, dict):
                continue
            xml = response[1]
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                wms = WebMapService(wms_getcapabilites_url, xml=xml.encode('utf-8'), version=wmsversion)
        except Exception as e:
            continue

    if wms is None:
        return create_result(ResultStatus.ERROR, "Could not access GetCapabilities of {}".format(wms_url_split[0]))

    # TODO check styles
    layer_arg = wms_args['layers']
    not_found_layers = []
    for layer_name in layer_arg.split(","):
        if layer_name not in wms.contents:
            not_found_layers.append(layer_name)
    if len(not_found_layers) > 0:
        return create_result(ResultStatus.ERROR, "Layers {layers} could not be found under url '{url}'.".format(
            layers=",".join(not_found_layers),
            url=wms_getcapabilites_url))
    else:
        return create_result(ResultStatus.GOOD, "Found layers")


async def check_wms_endpoint(source, session: ClientSession):
    """
    Check WMS Endpoint source

    Parameters
    ----------
    source : dict
        Source dictionary
    session : ClientSession
        aiohttp ClientSession object
    kwargs

    Returns
    -------
    dict:
        Result dict created by create_result()
    """
    # TODO assumptions
    wms_url = source['properties']['url']
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            response = await get_url(wms_url, session, with_text=True)
            if isinstance(response, dict):
                return response
            xml = response[1]
            wms = WebMapService(wms_url, xml=xml.encode('utf-8'))
            return create_result(ResultStatus.GOOD, "")
    except Exception as e:
        return create_result(ResultStatus.ERROR, repr(e))


async def check_wmts(source, session):
    """
    Check WMTS source

    Parameters
    ----------
    source : dict
        Source dictionary
    session : ClientSession
        aiohttp ClientSession object
    kwargs

    Returns
    -------
    dict:
        Result dict created by create_result()
    """
    try:
        wmts_url = source['properties']['url']
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            response = await get_url(wmts_url, session, with_text=True)
            if isinstance(response, dict):
                return response
            xml = response[1]
            wmts = WebMapTileService(wmts_url, xml=xml.encode('utf-8'))
            return create_result(ResultStatus.GOOD, "")
    except Exception as e:
        return create_result(ResultStatus.ERROR, repr(e))


async def process_source(filename, session: ClientSession):
    """
    Process single source file

    Parameters
    ----------
    filename : str
        Path to source file
    session : ClientSession
        aiohttp ClientSession object

    Returns
    -------
    dict:
        Result dict

    """
    result = {}
    path_split = filename.split(os.sep)
    sources_index = path_split.index("sources")

    result['filename'] = path_split[-1]
    result['directory'] = path_split[sources_index + 1:-1]

    async with aiofiles.open(filename, mode='r') as f:
        contents = await f.read()
        source = json.loads(contents)

    result['name'] = source['properties']['name']
    result['type'] = source['properties']['type']

    # Check licence url
    if 'license_url' not in source['properties']:
        result['license_url'] = create_result(ResultStatus.ERROR, "No license_url set!")
    else:
        licence_url = source['properties']['license_url']
        licence_url_status = await test_url(licence_url, session)
        result['license_url'] = licence_url_status

    # Check privacy url
    if 'privacy_policy_url' not in source['properties']:
        result['privacy_policy_url'] = create_result(ResultStatus.ERROR, "No privacy_policy_url set!")
    else:
        privacy_policy_url = source['properties']['privacy_policy_url']
        privacy_policy_url_status = await test_url(privacy_policy_url, session)
        result['privacy_policy_url'] = privacy_policy_url_status

    # Check imagery
    if source['geometry'] is not None:

        # Check imagery only for recent imagery
        if 'end_date' in source['properties']:
            age = datetime.date.today().year - int(source['properties']['end_date'].split("-")[0])
            if age > 20:
                result['imagery'] = create_result(ResultStatus.WARNING,
                                                  "Not checked due to age: {} years".format(age))
        if not 'imagery' in result:
            # Check tms
            if source['properties']['type'] == 'tms':
                result['imagery'] = await check_tms(source, session)

            # check wms
            elif source['properties']['type'] == 'wms':
                # result['imagery'] = create_result(ResultStatus.WARNING, "disabled")
                result['imagery'] = await check_wms(source, session)

            # check wms_endpoint
            elif source['properties']['type'] == 'wms_endpoint':
                result['imagery'] = await check_wms_endpoint(source, session)

            # check wmts
            elif source['properties']['type'] == 'wmts':
                result['imagery'] = await check_wmts(source, session)

    if 'license_url' not in result:
        result['license_url'] = create_result(ResultStatus.WARNING, "Not checked")
    if 'privacy_policy_url' not in result:
        result['privacy_policy_url'] = create_result(ResultStatus.WARNING, "Not checked")
    if 'imagery' not in result:
        result['imagery'] = create_result(ResultStatus.WARNING, "Not checked")

    return result


async def process(eli_path):
    """ Search for all sources files and setup of processing chain

    Parameters
    ----------
    eli_path : str
        Path to the 'sources' directory of the editor-layer-index
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MSIE 6.0; ELI Watchdog https://github.com/rbuffat/eli_watchdog )'}
    timeout = aiohttp.ClientTimeout(total=300)

    async with ClientSession(headers=headers, timeout=timeout) as session:
        jobs = []
        for filename in glob.glob(os.path.join(eli_path, '**', '*.geojson'), recursive=True):
            jobs.append(process_source(filename, session))
        result = await asyncio.gather(*jobs)
        return result


def fetch(eli_path):
    """ Fetch results of all sources

    Parameters
    ----------
    eli_path : str
        Path to the 'sources' directory of the editor-layer-index

    Returns
    -------
    list of dict
        A list with all results

    """
    return asyncio.run(process(eli_path=eli_path))
