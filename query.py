import asyncio
import datetime
import glob
import json
import os
from collections import namedtuple
from io import StringIO
import aiofiles
import aiohttp
import validators
from shapely.geometry import shape, MultiPolygon, Point, box
import mercantile
from owslib.wmts import WebMapTileService
import warnings
import re
from aiohttp import ClientSession
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import xml.etree.ElementTree as ET

imagery_ignore = {
    'SG-2018-WMS': 'WMS server does not advertise layer OP_SG (2020-8-23)',
    'Bedzin-PL-aerial_image': 'Imagery only accessible from a Polish IP address. (2020-8-24)',
    'Bedzin-PL-buildings': 'Imagery only accessible from a Polish IP address. (2020-8-24)',
    'Bedzin-PL-addresses': 'Imagery only accessible from a Polish IP address. (2020-8-24)',
    'Ukraine-orto10000-2012': 'Works only from within Ukraine or with an Ukrainian proxy server. (2020-9-7)',
    'UkraineKyiv2014DZK': 'Works only from within Ukraine or with an Ukrainian proxy server. (2020-9-7)'
}


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


def get_http_headers(source):
    """ Extract http headers from source"""
    headers = {}
    if 'custom-http-headers' in source['properties']:
        key = source['properties']['custom-http-headers']['header-name']
        value = source['properties']['custom-http-headers']['header-value']
        headers[key] = value
    return headers


async def get_url(url: str, session: ClientSession, with_text=False, headers=None):
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
                print("GET {}".format(url), headers)
                async with session.request(method="GET", url=url, ssl=False, headers=headers) as response:
                    status = response.status
                    if with_text:
                        try:
                            text = await response.text()
                        except:
                            text = await response.read()
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


async def test_url(url: str, session: ClientSession, headers: dict = None):
    """
    Test if a url is reachable

    Parameters
    ----------
    url:  str
        Url to test
    session: ClientSession
        aiohttp ClientSession object
    headers: dict
        custom http headers

    Returns
    -------
    dict:
        Result dict created by create_result()
    """
    resp = await get_url(url, session, with_text=True, headers=headers)
    if resp.exception is not None:
        return create_result(ResultStatus.ERROR, resp.exception)
    else:
        status_code = resp[0]
        if status_code == 200:
            status = ResultStatus.GOOD
        # Some server retrun error 404 but still html
        elif status_code == 404 and "html" in resp.text:
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
    # Remove prefixes to make parsing easier
    # From https://stackoverflow.com/questions/13412496/python-elementtree-module-how-to-ignore-the-namespace-of-xml-files-to-locate-ma
    try:
        it = ET.iterparse(StringIO(xml))
        for _, el in it:
            _, _, el.tag = el.tag.rpartition('}')
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

    def parse_layer(element, crs=set(), styles={}, bbox=None):
        new_layer = {'CRS': crs,
                     'Styles': {},
                     'BBOX': bbox}
        new_layer['Styles'].update(styles)
        for tag in ['Name', 'Title', 'Abstract']:
            e = element.find("./{}".format(tag))
            if e is not None:
                new_layer[e.tag] = e.text
        for tag in ['CRS', 'SRS']:
            es = element.findall("./{}".format(tag))
            for e in es:
                new_layer["CRS"].add(e.text.upper())
        for tag in ['Style']:
            es = element.findall("./{}".format(tag))
            for e in es:
                new_style = {}
                for styletag in ['Title', 'Name']:
                    el = e.find("./{}".format(styletag))
                    if el is not None:
                        new_style[styletag] = el.text
                new_layer["Styles"][new_style['Name']] = new_style
        # WMS Version 1.3.0
        e = element.find("./EX_GeographicBoundingBox")
        if e is not None:
            bbox = [float(e.find("./{}".format(orient)).text.replace(',', '.'))
                    for orient in ['westBoundLongitude',
                                   'southBoundLatitude',
                                   'eastBoundLongitude',
                                   'northBoundLatitude']]
            new_layer['BBOX'] = bbox
        # WMS Version < 1.3.0
        e = element.find("./LatLonBoundingBox")
        if e is not None:
            bbox = [float(e.attrib[orient].replace(',', '.')) for orient in ['minx', 'miny', 'maxx', 'maxy']]
            new_layer['BBOX'] = bbox

        if 'Name' in new_layer:
            layers[new_layer['Name']] = new_layer

        for sl in element.findall("./Layer"):
            parse_layer(sl,
                        new_layer['CRS'].copy(),
                        new_layer['Styles'],
                        new_layer['BBOX'])

    # Find child layers. CRS and Styles are inherited from parent
    top_layers = root.findall(".//Capability/Layer")
    for top_layer in top_layers:
        parse_layer(top_layer)

    wms['layers'] = layers

    # Parse formats
    formats = []
    for es in root.findall(".//Capability/Request/GetMap/Format"):
        formats.append(es.text)
    wms['formats'] = formats

    # Parse access constraints and fees
    constraints = []
    for es in root.findall(".//AccessConstraints"):
        constraints.append(es.text)
    fees = []
    for es in root.findall(".//Fees"):
        fees.append(es.text)
    wms['Fees'] = fees
    wms['AccessConstraints'] = constraints

    return wms


async def check_tms(source, session: ClientSession):
    """
    Check TMS source

    Parameters
    ----------
    source : dict
        Source dictionary
    session : ClientSession
        aiohttp ClientSession object

    Returns
    -------
    list:
        Good messages
    list:
        Warning messages
    list:
        Error Messages

    """

    error_msgs = []
    warning_msgs = []
    info_msgs = []

    headers = get_http_headers(source)

    try:
        if 'geometry' in source and source['geometry'] is not None:
            geom = shape(source['geometry'])
            centroid = geom.representative_point()
        else:
            centroid = Point(0, 0)

        tms_url = source['properties']['url']

        def validate_url():
            url = re.sub(r'switch:?([^}]*)', 'switch', tms_url).replace('{', '').replace('}', '')
            return validators.url(url)

        if not validate_url():
            error_msgs.append("URL validation error: {}".format(tms_url))

        parameters = {}

        # {z} instead of {zoom}
        if '{z}' in source['properties']['url']:
            error_msgs.append('{z} found instead of {zoom} in tile url')
            return

        if '{apikey}' in tms_url:
            warning_msgs.append("Not possible to check URL, apikey is required.")
            return info_msgs, warning_msgs, error_msgs

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
            tms_url_status = await test_url(query_url, session, headers)
            if tms_url_status['status'] == ResultStatus.GOOD:
                zoom_success.append(zoom)
                return True
            else:
                zoom_failures.append(zoom)
                return False

        # Test min zoom. In case of failure, increase test range
        result = await test_zoom(min_zoom)
        if not result:
            for zoom in range(min_zoom + 1, min(min_zoom + 4, max_zoom)):
                if zoom not in tested_zooms:
                    result = await test_zoom(zoom)
                    if result:
                        break

        # Test max_zoom. In case of failure, increase test range
        result = await test_zoom(max_zoom)
        if not result:
            for zoom in range(max_zoom, max(max_zoom - 4, min_zoom), -1):
                if zoom not in tested_zooms:
                    result = await test_zoom(zoom)
                    if result:
                        break

        tested_str = ",".join(list(map(str, sorted(tested_zooms))))
        if len(zoom_failures) == 0 and len(zoom_success) > 0:
            info_msgs.append("Zoom levels reachable. (Tested: {}) "
                             "".format(tested_str))
        elif len(zoom_failures) > 0 and len(zoom_success) > 0:
            not_found_str = ",".join(list(map(str, sorted(zoom_failures))))
            warning_msgs.append("Zoom level {} not reachable. (Tested: {}) "
                                "Tiles might not be present at tested location: {},{}".format(not_found_str,
                                                                                              tested_str,
                                                                                              centroid.x,
                                                                                              centroid.y))
        else:
            error_msgs.append("No zoom level reachable. (Tested: {}) "
                              "Tiles might not be present at tested location: {},{}".format(tested_str,
                                                                                            centroid.x,
                                                                                            centroid.y))

    except Exception as e:
        error_msgs.append("Exception: {}".format(str(e)))

    return info_msgs, warning_msgs, error_msgs


async def check_wms(source, session: ClientSession):
    """
    Check WMS source

    Parameters
    ----------
    source : dict
        Source dictionary
    session : ClientSession
        aiohttp ClientSession object

    Returns
    -------
    list:
        Good messages
    list:
        Warning messages
    list:
        Error Messages

    """

    error_msgs = []
    warning_msgs = []
    info_msgs = []

    wms_url = source['properties']['url']
    headers = get_http_headers(source)

    params = ["{proj}", "{bbox}", "{width}", "{height}"]
    missingparams = [p for p in params if p not in wms_url]
    if len(missingparams) > 0:
        error_msgs.append("The following values are missing in the URL: {}".format(",".join(missingparams)))

    wms_args = {}
    u = urlparse(wms_url)
    url_parts = list(u)
    for k, v in parse_qsl(u.query, keep_blank_values=True):
        wms_args[k.lower()] = v

    def validate_wms_getmap_url():
        """
        Layers and styles can contain whitespaces. Ignore them here. They are checked against GetCapabilities later.
        """
        url_parts_without_layers = "&".join(["{}={}".format(key, value) for key, value in wms_args.items()
                                             if key not in {'layers', 'styles'}])
        parts = url_parts.copy()
        parts[4] = url_parts_without_layers
        url = urlunparse(parts).replace('{', '').replace('}', '')
        return validators.url(url)

    if not validate_wms_getmap_url():
        error_msgs.append("URL validation error: {}".format(wms_url))

    # Check mandatory WMS GetMap parameters (Table 8, Section 7.3.2, WMS 1.3.0 specification)
    missing_request_parameters = set()
    is_esri = 'request' not in wms_args
    if is_esri:
        required_parameters = ['f', 'bbox', 'size', 'imageSR', 'bboxSR', 'format']
    else:
        required_parameters = ['version', 'request', 'layers', 'bbox', 'width', 'height', 'format']

    for request_parameter in required_parameters:
        if request_parameter.lower() not in wms_args:
            missing_request_parameters.add(request_parameter)

    # Nothing more to do for esri rest api
    if is_esri:
        return info_msgs, warning_msgs, error_msgs

    if 'version' in wms_args and wms_args['version'] == '1.3.0':
        if 'crs' not in wms_args:
            missing_request_parameters.add('crs')
        if 'srs' in wms_args:
            error_msgs.append("WMS {} urls should not contain SRS parameter.".format(wms_args['version']))
    elif 'version' in wms_args and not wms_args['version'] == '1.3.0':
        if 'srs' not in wms_args:
            missing_request_parameters.add('srs')
        if 'crs' in wms_args:
            error_msgs.append("WMS {} urls should not contain CRS parameter.".format(wms_args['version']))
    if len(missing_request_parameters) > 0:
        missing_request_parameters_str = ",".join(missing_request_parameters)
        error_msgs.append("Parameter '{}' is missing in url.".format(missing_request_parameters_str))
        return info_msgs, warning_msgs, error_msgs
    # Styles is mandatory according to the WMS specification, but some WMS servers seems not to care

    if 'styles' not in wms_args:
        warning_msgs.append("Parameter 'styles' is missing in url. 'STYLES=' can be used to request default style.")

    def get_getcapabilitie_url(wms_version=None):

        get_capabilities_args = {'service': 'WMS',
                                 'request': 'GetCapabilities'}
        if wms_version is not None:
            get_capabilities_args['version'] = wms_version

        # Keep extra arguments, such as map or key
        for key in wms_args:
            if key not in {'version', 'request', 'layers', 'bbox', 'width', 'height', 'format', 'crs', 'srs', 'styles'}:
                get_capabilities_args[key] = wms_args[key]

        url_parts[4] = urlencode(list(get_capabilities_args.items()))
        return urlunparse(url_parts)

    # We first send a service=WMS&request=GetCapabilities request to server
    # According to the WMS Specification Section 6.2 Version numbering and negotiation, the server should return
    # the GetCapabilities XML with the highest version the server supports.
    # If this fails, it is tried to explicitly specify a WMS version
    exceptions = []
    wms = None
    for wmsversion in [None, '1.3.0', '1.1.1', '1.1.0', '1.0.0']:
        if wmsversion is None:
            wmsversion_str = "-"
        else:
            wmsversion_str = wmsversion

        try:
            wms_getcapabilites_url = get_getcapabilitie_url(wmsversion)

            resp = await get_url(wms_getcapabilites_url, session, with_text=True, headers=headers)
            if resp.exception is not None:
                exceptions.append("WMS {}: {}".format(wmsversion, resp.exception))
                continue
            xml = resp.text
            if isinstance(xml, bytes):
                # Parse xml encoding to decode
                try:
                    xml_ignored = xml.decode(errors='ignore')
                    str_encoding = re.search("encoding=\"(.*?)\"", xml_ignored).group(1)
                    xml = xml.decode(encoding=str_encoding)
                except Exception as e:
                    raise RuntimeError("Could not parse encoding: {}".format(str(e)))

            wms = parse_wms(xml)
            if wms is not None:
                break
        except Exception as e:
            exceptions.append("WMS {}: Error: {}".format(wmsversion_str, str(e)))
            continue

    if wms is None:
        for msg in exceptions:
            error_msgs.append(msg)
        return info_msgs, warning_msgs, error_msgs

    for access_constraint in wms['AccessConstraints']:
        info_msgs.append("WMS AccessConstraints: {}".format(access_constraint))
    for fee in wms['Fees']:
        info_msgs.append("WMS Fees: {}".format(fee))

    if source['geometry'] is None:
        geom = None
    else:
        geom = shape(source['geometry'])

    # Check layers
    if 'layers' in wms_args:
        layer_arg = wms_args['layers']
        not_found_layers = []
        layers = layer_arg.split(',')
        for layer_name in layer_arg.split(","):
            if layer_name not in wms['layers']:
                for wms_layer in wms['layers']:
                    if layer_name.lower() == wms_layer.lower():
                        warning_msgs.append("Layer '{}' is advertised by WMS server as '{}'".format(layer_name,
                                                                                                    wms_layer))
                not_found_layers.append(layer_name)

        if len(not_found_layers) > 0:
            error_msgs.append("Layers '{}' not advertised by WMS GetCapabilities request. "
                              "In rare cases WMS server do not advertise layers.".format(",".join(not_found_layers)))

        # Check source geometry against layer bounding box
        # Regardless of its projection, each layer should advertise an approximated bounding box in lon/lat.
        # See WMS 1.3.0 Specification Section 7.2.4.6.6 EX_GeographicBoundingBox
        if geom is not None and geom.is_valid:
            max_outside = 0.0
            for layer_name in layers:
                if layer_name in wms['layers']:
                    bbox = wms['layers'][layer_name]['BBOX']
                    geom_bbox = box(*bbox)
                    geom_outside_bbox = geom.difference(geom_bbox)
                    area_outside_bbox = geom_outside_bbox.area / geom.area * 100.0
                    max_outside = max(max_outside, area_outside_bbox)

            if max_outside > 100.0:
                error_msgs.append("{}% of geometry is outside of the layers "
                                  "bounding box.".format(round(area_outside_bbox, 2)))
            elif max_outside > 15.0:
                warning_msgs.append("{}% of geometry is outside of the layers "
                                    "bounding box.".format(round(area_outside_bbox, 2)))

        # Check styles
        if 'styles' in wms_args:
            style = wms_args['styles']
            # default style needs not to be advertised by the server
            if not (style == 'default' or style == '' or style == ',' * len(layers)):
                styles = wms_args['styles'].split(',')
                if not len(styles) == len(layers):
                    error_msgs.append("Not the same number of styles and layers.")
                else:
                    for layer_name, style in zip(layers, styles):
                        if (len(style) > 0 and not style == 'default' and layer_name in wms['layers'] and
                                style not in wms['layers'][layer_name]['Styles']):
                            error_msgs.append("Layer '{}' does not support style '{}'".format(layer_name, style))

        # Check CRS
        crs_should_included_if_available = {'EPSG:4326', 'EPSG:3857', 'CRS:84'}
        if 'available_projections' not in source['properties']:
            error_msgs.append("source is missing 'available_projections' element.")
        else:
            for layer_name in layer_arg.split(","):
                if layer_name in wms['layers']:
                    not_supported_crs = set()
                    for crs in source['properties']['available_projections']:
                        if crs.upper() not in wms['layers'][layer_name]['CRS']:
                            not_supported_crs.add(crs)

                    if len(not_supported_crs) > 0:
                        supported_crs_str = ",".join(wms['layers'][layer_name]['CRS'])
                        not_supported_crs_str = ",".join(not_supported_crs)
                        warning_msgs.append("Layer '{}': CRS '{}' not in: {}".format(layer_name,
                                                                                     not_supported_crs_str,
                                                                                     supported_crs_str))

                    supported_but_not_included = set()
                    for crs in crs_should_included_if_available:
                        if (crs not in source['properties']['available_projections'] and
                                crs in wms['layers'][layer_name]['CRS']):
                            supported_but_not_included.add(crs)

                    if len(supported_but_not_included) > 0:
                        supported_but_not_included_str = ','.join(supported_but_not_included)
                        warning_msgs.append("Layer '{}': CRS '{}' not included in available_projections but "
                                            "supported by server.".format(layer_name, supported_but_not_included_str))

    if wms_args['version'] < wms['version']:
        warning_msgs.append("Query requests WMS version '{}', server supports '{}'".format(wms_args['version'],
                                                                                           wms['version']))

    # Check formats
    imagery_format = wms_args['format']
    imagery_formats_str = "', '".join(wms['formats'])
    if imagery_format not in wms['formats']:
        error_msgs.append("Format '{}' not in '{}'.".format(imagery_format, imagery_formats_str))

    if 'category' in source['properties'] and 'photo' in source['properties']['category']:
        if 'jpeg' not in imagery_format and 'jpeg' in imagery_formats_str:
            warning_msgs.append("Server supports JPEG, but '{}' is used. "
                                "JPEG is typically preferred for photo sources, but might not be always "
                                "the best choice. "
                                "(Server supports: '{}')".format(imagery_format, imagery_formats_str))
    # elif 'category' in source['properties'] and 'map' in source['properties']['category']:
    #     if 'png' not in imagery_format and 'png' in imagery_formats_str:
    #         warning_msgs.append("Server supports PNG, but '{}' is used. "
    #                             "PNG is typically preferred for map sources, but might not be always "
    #                             "the best choice. "
    #                             "(Server supports: '{}')".format(imagery_format, imagery_formats_str))

    return info_msgs, warning_msgs, error_msgs


async def check_wms_endpoint(source, session: ClientSession):
    """
    Check WMS Endpoint source

    Currently it is only tested if a GetCapabilities request can be parsed.

    Parameters
    ----------
    source : dict
        Source dictionary
    session : ClientSession
        aiohttp ClientSession object

    Returns
    -------
    list:
        Good messages
    list:
        Warning messages
    list:
        Error Messages

    """

    error_msgs = []
    warning_msgs = []
    info_msgs = []

    wms_url = source['properties']['url']
    headers = get_http_headers(source)

    if not validators.url(wms_url):
        error_msgs.append("URL validation error: {}".format(wms_url))

    wms_args = {}
    u = urlparse(wms_url)
    url_parts = list(u)
    for k, v in parse_qsl(u.query, keep_blank_values=True):
        wms_args[k.lower()] = v

    def get_getcapabilitie_url(wms_version=None):

        get_capabilities_args = {'service': 'WMS',
                                 'request': 'GetCapabilities'}
        if wms_version is not None:
            get_capabilities_args['version'] = wms_version

        # Keep extra arguments, such as map or key
        for key in wms_args:
            if key not in {'version', 'request', 'layers', 'bbox', 'width', 'height', 'format', 'crs', 'srs'}:
                get_capabilities_args[key] = wms_args[key]

        url_parts[4] = urlencode(list(get_capabilities_args.items()))
        return urlunparse(url_parts)

    for wmsversion in [None, '1.3.0', '1.1.1', '1.1.0', '1.0.0']:
        try:
            url = get_getcapabilitie_url(wms_version=wmsversion)
            response = await get_url(url, session, with_text=True, headers=headers)
            if response.exception is not None:
                error_msgs.append(response.exception)
                return info_msgs, warning_msgs, error_msgs
            xml = response.text
            wms = parse_wms(xml)
            for access_constraint in wms['AccessConstraints']:
                info_msgs.append("WMS AccessConstraints: {}".format(access_constraint))
            for fee in wms['Fees']:
                info_msgs.append("WMS Fees: {}".format(fee))
            break
        except Exception as e:
            error_msgs.append("Exception: {}".format(str(e)))

    return info_msgs, warning_msgs, error_msgs


async def check_wmts(source, session):
    """
    Check WMTS source

    Parameters
    ----------
    source : dict
        Source dictionary
    session : ClientSession
        aiohttp ClientSession object

    Returns
    -------
    list:
        Good messages
    list:
        Warning messages
    list:
        Error Messages

    """
    error_msgs = []
    warning_msgs = []
    info_msgs = []

    try:
        wmts_url = source['properties']['url']
        headers = get_http_headers(source)

        if not validators.url(wmts_url):
            error_msgs.append("URL validation error: {}".format(wmts_url))

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            response = await get_url(wmts_url, session, with_text=True, headers=headers)
            if response.exception is not None:
                error_msgs.append(response.exception)
                return info_msgs, warning_msgs, error_msgs

            xml = response.text
            wmts = WebMapTileService(wmts_url, xml=xml.encode('utf-8'))
            info_msgs.append("Good")
    except Exception as e:
        error_msgs.append("Exception: {}".format(str(e)))

    return info_msgs, warning_msgs, error_msgs


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
    list:
        Good messages
    list:
        Warning messages
    list:
        Error Messages

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
    source_id = source['properties']['id']
    result['id'] = source_id

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

    # Check category
    if 'category' not in source['properties']:
        result['category'] = ''
    else:
        result['category'] = source['properties']['category']

    # Check imagery
    # Check imagery only for recent imagery
    if 'end_date' in source['properties']:
        age = datetime.date.today().year - int(source['properties']['end_date'].split("-")[0])
        if age > 30:
            result['imagery'] = create_result(ResultStatus.WARNING,
                                              "Not checked due to age: {} years".format(age))
    if 'imagery' not in result:
        if source_id in imagery_ignore:
            info_msgs = error_msgs = []
            warning_msgs = ["Ignored: {}".format(imagery_ignore[source_id])]
        elif "User-Agent" in source['properties']['url']:
            info_msgs = error_msgs = []
            warning_msgs = ["Not checked, URL includes User-Agent"]
        else:
            if source['properties']['type'] == 'tms':
                info_msgs, warning_msgs, error_msgs = await check_tms(source, session)
            elif source['properties']['type'] == 'wms':
                info_msgs, warning_msgs, error_msgs = await check_wms(source, session)
            elif source['properties']['type'] == 'wms_endpoint':
                info_msgs, warning_msgs, error_msgs = await check_wms_endpoint(source, session)
            elif source['properties']['type'] == 'wmts':
                info_msgs, warning_msgs, error_msgs = await check_wmts(source, session)
            else:
                info_msgs = error_msgs = []
                warning_msgs = ["{} is currently not checked.".format(source['properties']['type'])]

        messages = ["Error: {}".format(m) for m in error_msgs]
        messages += ["Warning: {}".format(m) for m in warning_msgs]
        messages += ["Info: {}".format(m) for m in info_msgs]

        if len(error_msgs) > 0:
            result['imagery'] = create_result(ResultStatus.ERROR, message=messages)
        elif len(error_msgs) == 0 and len(warning_msgs) == 0:
            result['imagery'] = create_result(ResultStatus.GOOD, message=messages)
        else:
            result['imagery'] = create_result(ResultStatus.WARNING, message=messages)

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
