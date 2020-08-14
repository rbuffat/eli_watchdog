import datetime
import html
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader

file_loader = FileSystemLoader('templates')
env = Environment(loader=file_loader)


def get_country_key(d):
    if len(d['directory']) == 1:
        region_key = d['directory'][0]
        source_key = region_key

    else:
        region_key = d['directory'][0]
        source_key = "_".join(d['directory'][1:])

    return {'country': source_key,
            'region': region_key}


def render_menu(data):

    r = defaultdict(set)
    for d in data:
        country_key = get_country_key(d)
        r[country_key['region']].add(country_key['country'])

    def country_path(region, country):
        if region == country:
            return region
        else:
            return " / ".join([region] + country.split("_"))

    regions = []
    for region in sorted(r):
        countries = [{'id': country, 'path': country_path(region, country)} for country in r[region]]
        regions.append({
            'name': region,
            'countries': sorted(countries, key=lambda country: country['path'])
        })

    template = env.get_template('menu.html')
    return template.render(regions=regions)


def render_overview(data):
    good = defaultdict(lambda: defaultdict(int))
    total = defaultdict(int)
    region2key = {}

    for d in data:
        region = " / ".join(d['directory'])
        region_key = get_country_key(d)
        region2key[region] = region_key

        total[region] += 1

        if d['imagery']['status'] in {'good', 'warning'}:
            good['imagery'][region] += 1

        if d['license_url']['status'] in {'good', 'warning'}:
            good['license_url'][region] += 1

        if d['privacy_policy_url']['status'] in {'good', 'warning'}:
            good['privacy_policy_url'][region] += 1

        if len(d['category']) > 0:
            good['category'][region] += 1

    def calc(cat, region_key):
        count = good[cat][region_key]
        tot = total[region_key]
        percent = count / tot * 100.0
        if percent == 100:
            status = "success"
        elif 90 <= percent < 100:
            status = "warning"
        else:
            status = "danger"
        return {
            'percent': "{}% ({} / {})".format(round(percent), count, tot),
            'status': status
        }

    def get_regionid(region):
        return "{}-{}".format(region2key[region]['region'],
                              region2key[region]['country'])

    regions = [{'regionid': get_regionid(region),
                'name': region,
                'imagery': calc('imagery', region),
                'license_url': calc('license_url', region),
                'privacy_policy_url': calc('privacy_policy_url', region),
                'category': calc('category', region)
                } for region in total]

    template = env.get_template('overview.html')
    return template.render(regions=regions)


def render_countries(data):
    template = env.get_template('country_sources.html')

    collect = defaultdict(list)
    for d in data:
        country_key = get_country_key(d)

        github_url = 'https://github.com/osmlab/editor-layer-index/tree/gh-pages/sources/{}/{}'.format(
            "/".join(d['directory']),
            d['filename'])

        def transform_result(result):
            messages = result['message']
            if isinstance(messages, str):
                messages = [messages]
            messages = [html.escape(msg) for msg in messages]
            result['message'] = messages
            return result

        collect[(country_key['region'], country_key['country'], " / ".join(d['directory']))].append({
            'name': d.get('name', 'Not available'),
            'url': github_url,
            'imagery': transform_result(d['imagery']),
            'license_url': transform_result(d['license_url']),
            'privacy_policy_url': transform_result(d['privacy_policy_url']),
            'type': d['type'],
            'category': d['category']
        })
    countries = [{'name': name,
                  'region': region,
                  'directory': directory,
                  'sources': collect[(region, name, directory)]} for region, name, directory in collect]

    return template.render(countries=countries)


def render(data):
    data = {'menu': render_menu(data),
            'countries': render_countries(data),
            'overview': render_overview(data)
            }

    template = env.get_template('main.html')
    template.globals['now'] = datetime.datetime.utcnow()

    with open("web/index.html", 'w') as f:
        f.write(template.render(data=data))
