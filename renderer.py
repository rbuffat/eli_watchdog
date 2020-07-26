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
    template = env.get_template('menu.html')

    r = defaultdict(set)
    for d in data:
        country_key = get_country_key(d)
        r[country_key['region']].add(country_key['country'])

    regions = []
    for region in sorted(r):
        regions.append({
            'name': region,
            'countries': sorted(r[region])
        })

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
            result['message'] = html.escape(result['message'])
            return result

        collect[(country_key['region'], country_key['country'])].append({
            'name': d.get('name', 'Not available'),
            'url': github_url,
            'imagery': transform_result(d['imagery']),
            'license_url': transform_result(d['license_url']),
            'privacy_policy_url': transform_result(d['privacy_policy_url'])
        })
    countries = [{'name': name, 'region': region, 'sources': collect[(region, name)]} for region, name in collect]

    return template.render(countries=countries)


def render(data):
    data = {'menu': render_menu(data),
            'countries': render_countries(data),
            }

    template = env.get_template('main.html')
    template.globals['now'] = datetime.datetime.utcnow()

    with open("web/index.html", 'w') as f:
        f.write(template.render(data=data))
