from collections import defaultdict
from jinja2 import Environment, FileSystemLoader

file_loader = FileSystemLoader('templates')
env = Environment(loader=file_loader)


def render_menu(data):
    template = env.get_template('menu.html')

    r = defaultdict(set)
    for d in data:
        if len(d['directory']) > 1:
            r[d['directory'][0]].add(d['directory'][1])

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
        if len(d['directory']) > 1:
            country_key = (d['directory'][1], d['directory'][0])
            github_url = 'https://github.com/osmlab/editor-layer-index/tree/gh-pages/sources/{}/{}'.format(
                "/".join(d['directory']),
                d['filename'])
            collect[country_key].append({
                'name': d.get('name', 'Not available'),
                'url': github_url,
                'imagery': d.get('imagery', 'Not available'),
                'license_url': d.get('license_url', 'Not available'),
                'privacy_policy_url': d.get('privacy_policy_url', 'Not available')
            })
    countries = [{'name': name, 'region': region, 'sources': collect[(name, region)]} for name, region in collect]

    return template.render(countries=countries)


def render(data):
    data = {'menu': render_menu(data),
            'countries': render_countries(data),
            }

    template = env.get_template('main.html')

    with open("web/index.html", 'w') as f:
        f.write(template.render(data=data))
