"""Dated club staff observations and roles proven within a coach's stint."""
from __future__ import annotations

from bs4 import BeautifulSoup

STAFF_PARSER_REVISION = 'staff-v2'
STAFF_COLUMNS = [
    'club_id', 'person_id', 'person_slug', 'name', 'section', 'role',
    'appointed_date', 'observed_day', 'observed_at',
]
STAFF_KEY = ('club_id', 'person_id', 'section', 'role', 'observed_day')
STAFF_TABLE = 'iceberg.bronze.transfermarkt_club_staff'
STAFF_RECEIPTS_TABLE = 'iceberg.ops.transfermarkt_club_staff_receipts_v1'


def parse_staff(html, club_id):
    """Keep every section and role, rejecting an incomplete table as a whole."""
    from scrapers.transfermarkt.scraper import _COACH_HREF_RE, _parse_tm_date

    soup = BeautifulSoup(html, 'html.parser')
    rows = []
    tables = []
    for headline in soup.select('.box > h2.content-box-headline'):
        table = headline.parent.find('table')
        if table is None:
            raise ValueError('staff section has no table')
        headers = [th.get_text(' ', strip=True) for th in table.select('thead th')]
        if not headers or headers[0] != 'Name/Position' or 'Appointed' not in headers:
            raise ValueError('staff headers changed')
        tables.append(table)
        body = table.find('tbody')
        if body is None or not body.find_all('tr', recursive=False):
            raise ValueError('staff section has no rows')
        for tr in body.find_all('tr', recursive=False):
            cells = tr.find_all('td', recursive=False)
            link = tr.find('a', href=_COACH_HREF_RE)
            inline = cells[0].find('table', class_='inline-table') if cells else None
            detail = inline.find_all('tr') if inline else []
            if len(cells) != len(headers) or link is None or len(detail) != 2:
                raise ValueError('incomplete staff row')
            role = detail[1].get_text(' ', strip=True)
            name = link.get_text(' ', strip=True)
            if not name or not role or role == '-':
                raise ValueError('staff identity or role missing')
            match = _COACH_HREF_RE.match(link['href'])
            rows.append({
                'club_id': str(club_id), 'person_id': match.group('id'),
                'person_slug': match.group('slug'), 'name': name,
                'section': headline.get_text(' ', strip=True), 'role': role,
                'appointed_date': _parse_tm_date(cells[headers.index('Appointed')].get_text(' ', strip=True)),
            })
    source_tables = [table for table in soup.find_all('table')
                     if table.find('thead', recursive=False) is not None]
    if len(tables) != len(source_tables):
        raise ValueError('unrecognised staff section')
    if not tables or not rows:
        # No observed authoritative-empty layout: a missing table is not empty staff.
        raise ValueError('staff tables missing or empty')
    unique = {}
    for row in rows:
        key = (row['person_id'], row['section'], row['role'])
        if key in unique and unique[key] != row:
            raise ValueError('conflicting duplicate staff row')
        unique[key] = row
    return list(unique.values())

