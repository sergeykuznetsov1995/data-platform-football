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
}

# Available stat types for players (outfield)
PLAYER_STAT_TYPES = [
    'stats',           # Standard stats
    'shooting',        # Shooting stats
    'passing',         # Passing stats
    'passing_types',   # Pass types
    'gca',             # Goal and shot creation
    'defense',         # Defensive actions
    'possession',      # Possession stats
    'playingtime',     # Playing time
    'misc',            # Miscellaneous
]

# Goalkeeper-specific stat types
KEEPER_STAT_TYPES = [
    'keeper',          # Goalkeeper basic stats
    'keeper_adv',      # Goalkeeper advanced stats
]

# Player match-level stat types (per-game statistics)
PLAYER_MATCH_STAT_TYPES = [
    'summary',         # Basic match summary
    'passing',         # Match passing stats
    'defense',         # Match defense stats
    'possession',      # Match possession stats
    'misc',            # Match misc stats
]

# Available stat types for teams (squads)
TEAM_STAT_TYPES = [
    'stats',           # Standard stats
    'shooting',        # Shooting stats
    'passing',         # Passing stats
    'passing_types',   # Pass types
    'gca',             # Goal and shot creation
    'defense',         # Defensive actions
    'possession',      # Possession stats
    'playingtime',     # Playing time
    'misc',            # Miscellaneous
]

# Match data types
MATCH_DATA_TYPES = [
    'schedule',            # Match schedule
    'shot_events',         # Shot-level events
    'match_events',        # Match events (goals, cards, subs)
    'lineups',             # Match lineups
    'match_team_stats',    # Per-match team stats (team_stats + team_stats_extra)
    'match_player_stats',  # Per-match player stats (summary/passing/defense/...)
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
