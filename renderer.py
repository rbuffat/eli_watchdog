import datetime
import html
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader

file_loader = FileSystemLoader('templates')
env = Environment(loader=file_loader)


def render_menu(data):
    template = env.get_template('menu.html')

    r = defaultdict(set)
    for d in data:
        if len(d['directory']) == 0:
            region_key = d['directory'][0]
            source_key = d['directory'][0]

        else:
            region_key = d['directory'][0]
            source_key = "_".join(d['directory'][1:])

        r[region_key].add(source_key)

    regions = []
    for region in sorted(r):
        regions.append({
            'name': region,
            'countries': sorted(r[region])
        })

    return template.render(regions=regions)


def render_countries(data):
    template = env.get_template('country_sources.html')

    # TODO sources with no sub directory
    collect = defaultdict(list)
    for d in data:

        if len(d['directory']) == 0:
            region_key = d['directory'][0]
            source_key = d['directory'][0]

        else:
            region_key = d['directory'][0]
            source_key = "_".join(d['directory'][1:])

        country_key = (source_key, region_key)
        github_url = 'https://github.com/osmlab/editor-layer-index/tree/gh-pages/sources/{}/{}'.format(
            "/".join(d['directory']),
            d['filename'])
        collect[country_key].append({
            'name': d.get('name', 'Not available'),
            'url': github_url,
            'imagery': html.escape(d.get('imagery', 'Not available')),
            'license_url': html.escape(d.get('license_url', 'Not available')),
            'privacy_policy_url': html.escape(d.get('privacy_policy_url', 'Not available'))
        })
    countries = [{'name': name, 'region': region, 'sources': collect[(name, region)]} for name, region in collect]

    return template.render(countries=countries)


def render(data):
    data = {'menu': render_menu(data),
            'countries': render_countries(data),
            }

    template = env.get_template('main.html')
    template.globals['now'] = datetime.datetime.utcnow()

    with open("web/index.html", 'w') as f:
        f.write(template.render(data=data))
