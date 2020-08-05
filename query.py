import asyncio
import datetime
import glob
import json
import os
from collections import namedtuple
from io import StringIO
import aiofiles
import aiohttp
from shapely.geometry import shape
import mercantile
from owslib.wmts import WebMapTileService
import warnings
import re
from aiohttp import ClientSession
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import xml.etree.ElementTree as ET


class ResultStatus:
    GOOD = "good"
    WARNING = "warning"
    ERROR = "error"


def create_result(status, message):
    return {'status': status,
            'message': message}


RequestResult = namedtuple('RequestResultCache',
                           ['status', 'text', 'exception'],
                           defaults=[None, None, None])

response_cache = {}
domain_locks = {}
domain_lock = asyncio.Lock()


async def get_url(url: str, session: ClientSession, with_text=False):
    """ Ensure that only one request is sent to a domain at one point in time and that the same url is not
    queried more than once.
    """
    o = urlparse(url)
    if len(o.netloc) == 0:
        return RequestResult(exception="Could not parse URL: {}".format(url))

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
                        response_cache[url] = RequestResult(status=status, text=text)
                    else:
                        response_cache[url] = RequestResult(status=status)
            except asyncio.TimeoutError:
                response_cache[url] = RequestResult(exception="Timeout for: {}".format(url))
            except Exception as e:
                print("Error for: {} ({})".format(url, str(e)))
                response_cache[url] = RequestResult(exception="Exception {} for: {}".format(str(e), url))
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
    if resp.exception is not None:
        return create_result(ResultStatus.ERROR, resp.exception)
    else:
        status_code = resp[0]
        if status_code == 200:
            status = ResultStatus.GOOD
        else:
            status = ResultStatus.ERROR
        message = "HTTP Code {} for {}".format(status_code, url)
        return create_result(status, message)


def parse_wms(xml):
    """ Rudimentary parsing of WMS Layers from GetCapabilites Request
        owslib.wms seems to have problems parsing some weird not relevant metadata.
        This function aims at only parsing relevant layer metadata
    """
    wms = {}
    try:
        it = ET.iterparse(StringIO(xml))
        for _, el in it:
            prefix, has_namespace, postfix = el.tag.partition('}')
            if has_namespace:
                el.tag = postfix
        root = it.root
    except:
        raise RuntimeError("Could not parse XML.")

    root_tag = root.tag.rpartition("}")[-1]
    if root_tag in {'ServiceExceptionReport', 'ServiceException'}:
        raise RuntimeError("WMS service exception")

    if root_tag not in {'WMT_MS_Capabilities', 'WMS_Capabilities'}:
        raise RuntimeError("No Capabilities Element present: Root tag: {}".format(root_tag))

    if 'version' not in root.attrib:
        raise RuntimeError("WMS version cannot be identified.")
    version = root.attrib['version']
    wms['version'] = version

    layers = {}

    def parse_layer(element, crs=set(), styles={}):
        new_layer = {'CRS': crs,
                     'Styles': {}}
        new_layer['Styles'].update(styles)
        for tag in ['Name', 'Title']:
            e = element.find("./{}".format(tag))
            if e is not None:
                new_layer[e.tag] = e.text
        for tag in ['CRS', 'SRS']:
            es = element.findall("./{}".format(tag))
            for e in es:
                new_layer["CRS"].add(e.text)
        for tag in ['Style']:
            es = element.findall("./{}".format(tag))
            for e in es:
                new_style = {}
                for styletag in ['Title', 'Name']:
                    new_style[styletag] = element.find("./{}".format(styletag)).text
                new_layer["Styles"][new_style['Name']] = new_style

        if 'Name' in new_layer:
            layers[new_layer['Name']] = new_layer

        for sl in element.findall("./Layer"):
            parse_layer(sl,
                        new_layer['CRS'].copy(),
                        new_layer['Styles'])

    # Find child layers. CRS and Styles are inherited from parent
    top_layers = root.findall(".//Capability/Layer")
    for top_layer in top_layers:
        parse_layer(top_layer)

    wms['layers'] = layers
    return wms


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
    try:
        geom = shape(source['geometry'])
        # TODO multipolygons and holes!
        centroid = geom.centroid

        tms_url = source['properties']['url']
        parameters = {}

        if '{apikey}' in tms_url:
            return create_result(ResultStatus.WARNING, "URL requires apikey")

        if "{switch:" in tms_url:
            match = re.search(r'switch:?([^}]*)', tms_url)
            switches = match.group(1).split(',')
            tms_url = tms_url.replace(match.group(0), 'switch')
            parameters['switch'] = switches[0]

        min_zoom = 0
        max_zoom = 22

        if 'min_zoom' in source['properties']:
            min_zoom = int(source['properties']['min_zoom'])
        if 'max_zoom' in source['properties']:
            max_zoom = int(source['properties']['max_zoom'])

        zoom_failures = []
        zoom_success = []
        tested_zooms = set()

        async def test_zoom(zoom):
            tested_zooms.add(zoom)
            tile = mercantile.tile(centroid.x, centroid.y, zoom)

            query_url = tms_url
            if '{-y}' in tms_url:
                y = 2 ** zoom - 1 - tile.y
                query_url = query_url.replace('{-y}', str(y))
            elif '{!y}' in tms_url:
                y = 2 ** (zoom - 1) - 1 - tile.y
                query_url = query_url.replace('{!y}', str(y))
            else:
                query_url = query_url.replace('{y}', str(tile.y))
            parameters['x'] = tile.x
            parameters['zoom'] = zoom
            query_url = query_url.format(**parameters)
            await asyncio.sleep(0.5)
            tms_url_status = await test_url(query_url, session)
            if tms_url_status['status'] == ResultStatus.GOOD:
                zoom_success.append(zoom)
                return True
            else:
                zoom_failures.append(zoom)
                return False

        # Test min zoom. In case of failure, increase test range
        result = await test_zoom(min_zoom)
        if not result:
            for zoom in range(min_zoom + 1, min_zoom + 4):
                if zoom not in tested_zooms:
                    result = await test_zoom(zoom)
                    if result:
                        break

        # Test max_zoom. In case of failure, increase test range
        result = await test_zoom(max_zoom)
        if not result:
            for zoom in range(max_zoom, max_zoom - 4, -1):
                if zoom not in tested_zooms:
                    result = await test_zoom(zoom)
                    if result:
                        break

        tested_str = ",".join(list(map(str, sorted(tested_zooms))))
        if len(zoom_failures) == 0 and len(zoom_success) > 0:
            return create_result(ResultStatus.GOOD,
                                 "Zoom levels reachable. (Tested: {})".format(tested_str))
        elif len(zoom_failures) > 0 and len(zoom_success) > 0:
            not_found_str = ",".join(list(map(str, sorted(zoom_failures))))
            return create_result(ResultStatus.WARNING,
                                 "Zoom level {} not reachable. (Tested: {})".format(not_found_str,
                                                                                    tested_str))
        else:
            return create_result(ResultStatus.ERROR,
                                 "No zoom level reachable. (Tested: {})".format(tested_str))

    except Exception as e:
        return create_result(ResultStatus.ERROR, str(e))


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

    wms_warnings = []
    wms_errors = []

    wms_url = source['properties']['url']

    wms_args = {}
    u = urlparse(wms_url)
    url_parts = list(u)
    for k, v in parse_qsl(u.query):
        wms_args[k.lower()] = v

    if 'layers' not in wms_args:
        return create_result(ResultStatus.ERROR, "No layers specified in: {}".format(wms_url))

    def get_getcapabilitie_url(wms_version):

        get_capabilities_args = {'service': 'WMS',
                                 'request': 'GetCapabilities'}
        if wms_version is not None:
            get_capabilities_args['version'] = wms_version

        # Some server only return capabilities when the map parameter is specified
        if 'map' in wms_args:
            get_capabilities_args['map'] = wms_args['map']

        url_parts[4] = urlencode(list(get_capabilities_args.items()))
        return urlunparse(url_parts)

    # We first send a service=WMS&request=GetCapabilities request to server
    # According to the WMS Specification Section 6.2 Version numbering and negotiation, the server should return
    # the GetCapabilities XML with the highest version the server supports.
    # If this fails, it is tried to explicitly specify a WMS version
    exceptions = []
    wms = None
    for wmsversion in [None, '1.3.0', '1.1.1', '1.1.0', '1.0.0']:
        try:
            wms_getcapabilites_url = get_getcapabilitie_url(wmsversion)

            resp = await get_url(wms_getcapabilites_url, session, with_text=True)
            if resp.exception is not None:
                exceptions.append("WMS {}: Connection Error: {}".format(wmsversion, resp.exception))
                continue
            xml = resp.text
            wms = parse_wms(xml)
            if wms is not None:
                break
        except Exception as e:
            exceptions.append("WMS {}: Error: {}".format(wmsversion, str(e)))
            continue

    if wms is None:
        message = ["Could not access GetCapabilities:"] + exceptions
        return create_result(ResultStatus.ERROR, message)

    layer_arg = wms_args['layers']
    not_found_layers = []

    for layer_name in layer_arg.split(","):
        if layer_name not in wms['layers']:
            not_found_layers.append(layer_name)

    if 'styles' in wms_args:
        # default style needs not to be advertised by the server
        style = wms_args['styles']
        if not style == 'default':
            for layer_name in layer_arg.split(","):
                if style not in wms['layers'][layer_name]['Styles']:
                    wms_errors.append("Layer '{}' does not support style '{}'".format(layer_name, style))

    if len(not_found_layers) > 0:
        wms_errors.append("Layers '{}' not present.".format(",".join(not_found_layers)))

    if wms_args['version'] < wms['version']:
        wms_warnings.append("Query requests WMS version '{}', server supports '{}'".format(wms_args['version'],
                                                                                           wms['version']))

    if len(wms_errors) > 0:
        status = ResultStatus.ERROR
    elif len(wms_errors) == 0 and len(wms_warnings) == 0:
        status = ResultStatus.GOOD
    else:
        status = ResultStatus.WARNING

    if status == ResultStatus.GOOD:
        message = "Found layers"
    else:
        message = ["Error: {}".format(m) for m in wms_errors] + ["Warning: {}".format(m) for m in wms_warnings]

    return create_result(status, message)


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
            if response.exception is not None:
                return create_result(ResultStatus.ERROR, response.exception)
            xml = response.text
            wms = parse_wms(xml)
            return create_result(ResultStatus.GOOD, "")
    except Exception as e:
        return create_result(ResultStatus.ERROR, str(e))


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
            if response.exception is not None:
                return create_result(ResultStatus.ERROR, response.exception)

            xml = response.text
            wmts = WebMapTileService(wmts_url, xml=xml.encode('utf-8'))
            return create_result(ResultStatus.GOOD, "")
    except Exception as e:
        return create_result(ResultStatus.ERROR, str(e))


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
            if age > 30:
                result['imagery'] = create_result(ResultStatus.WARNING,
                                                  "Not checked due to age: {} years".format(age))
        if not 'imagery' in result:
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
    timeout = aiohttp.ClientTimeout(total=30)

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
