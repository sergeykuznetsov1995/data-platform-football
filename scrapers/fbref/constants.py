"""
FBref Constants
===============

Configuration constants for FBref scraper.
"""

# FBref base URL
BASE_URL = 'https://fbref.com'

# League configuration with competition IDs and URL slugs
LEAGUE_IDS = {
    'ENG-Premier League': {'comp_id': '9', 'slug': 'Premier-League'},
    'ESP-La Liga': {'comp_id': '12', 'slug': 'La-Liga'},
    'GER-Bundesliga': {'comp_id': '20', 'slug': 'Bundesliga'},
    'ITA-Serie A': {'comp_id': '11', 'slug': 'Serie-A'},
    'FRA-Ligue 1': {'comp_id': '13', 'slug': 'Ligue-1'},
    'UEFA-Champions League': {'comp_id': '8', 'slug': 'Champions-League'},
    'UEFA-Europa League': {'comp_id': '19', 'slug': 'Europa-League'},
    'INT-World Cup': {'comp_id': '1', 'slug': 'World-Cup'},
    # #920 Phase 3 — live-verified (docs/research/T0_tournaments_2027_2028_recon.md).
    # NB AFCON: path-год URL = ОФИЦИАЛЬНЫЙ год розыгрыша (розыгрыш «2023»
    # игрался в янв-2024, но живёт под /656/2023/) — сезонный ключ = path-год.
    'INT-European Championship': {'comp_id': '676', 'slug': 'European-Championship'},
    'INT-Africa Cup of Nations': {'comp_id': '656', 'slug': 'Africa-Cup-of-Nations'},
    'INT-Copa America': {'comp_id': '685', 'slug': 'Copa-America'},
}

# Available stat types for players (outfield)
#
# Removed (Apr 2026) — FBref restricted these stats: tables exist but ALL cells
# are empty (`<td class="iz"></td>`). Verified by counting NULL/empty rows in
# iceberg.bronze.fbref_player_{passing,passing_types,gca,defense,possession}:
# 22617/22617 rows had no data. See MEMORY.md "FBref Data Availability".
#   - 'passing'        — 22617 empty rows
#   - 'passing_types'  — 22617 empty rows
#   - 'gca'            — 22617 empty rows
#   - 'defense'        — 22603 empty rows
#   - 'possession'     — 22617 empty rows
PLAYER_STAT_TYPES = [
    'stats',           # Standard stats
    'shooting',        # Shooting stats
    'playingtime',     # Playing time
    'misc',            # Miscellaneous
]

# Source-advertised season routes that are intentionally not fetched.  Live
# audits found that these pages still exist but their statistical cells are
# restricted/empty, so following every nav link would spend one paid request
# per competition-season without producing data.  Discovery must skip them
# explicitly rather than misclassifying them as season overview pages.
UNAVAILABLE_SEASON_STAT_ROUTES = frozenset({
    'passing',
    'passing_types',
    'gca',
    'defense',
    'possession',
    'keepersadv',
})

# Available stat types for teams (squads)
#
# Removed (Apr 2026) — same restriction as players: tables exist with
# squad/90s/# pl filled but ALL stat cells empty (820 rows, 0 non-null).
TEAM_STAT_TYPES = [
    'stats',           # Standard stats
    'shooting',        # Shooting stats
    'playingtime',     # Playing time
    'misc',            # Miscellaneous
]

# Default rate limit (requests per minute)
DEFAULT_RATE_LIMIT = 20

# JavaScript to uncomment FBref statistical tables before HTML extraction.
# FBref stores stat tables inside HTML comments (<!-- <table>...</table> -->)
# inside various container divs. JavaScript on the page should uncomment them,
# but through proxies the JS files may not load or execute in time.
# This script:
# 1. Collects diagnostics about page state
# 2. Finds ALL comment nodes containing <table> anywhere in the document
# 3. Uncomments them by replacing the comment with actual DOM elements
FBREF_UNCOMMENT_TABLES_JS = """
(function() {
    var diag = {
        url: window.location.href,
        title: document.title,
        readyState: document.readyState,
        tables: document.querySelectorAll('table').length,
        allDivs: document.querySelectorAll('div[id^="all_"]').length,
        bodyLen: document.body ? document.body.innerHTML.length : 0,
        scripts: document.querySelectorAll('script').length,
        scriptsSrc: document.querySelectorAll('script[src]').length,
        comments: 0,
        uncommented: 0
    };

    // Walk DOM to find comment nodes containing tables
    try {
        var walker = document.createTreeWalker(
            document.body || document,
            NodeFilter.SHOW_COMMENT, null, false
        );
        var commentsToReplace = [];
        var node;
        while (node = walker.nextNode()) {
            diag.comments++;
            if (node.data && node.data.indexOf('<table') !== -1) {
                commentsToReplace.push(node);
            }
        }
        commentsToReplace.forEach(function(comment) {
            var parent = comment.parentNode;
            if (parent) {
                var temp = document.createElement('div');
                temp.innerHTML = comment.data;
                while (temp.firstChild) {
                    parent.insertBefore(temp.firstChild, comment);
                }
                parent.removeChild(comment);
                diag.uncommented++;
            }
        });
    } catch(e) {
        diag.walkerError = e.message;
    }

    diag.tablesAfter = document.querySelectorAll('table').length;
    return JSON.stringify(diag);
})()
"""
