from p2.utility.database_driver import PostgresDriver


def normalize_username(name: str) -> str:
    """Lowercase, collapse spaces/underscores into single dots.

    e.g. "John_Doe" / "John Doe" / "john..doe" -> "john.doe"
    This is the single source of truth for username normalization;
    reused by normalize_list() in user_registry.py so the write path
    (config) and read path (session/middleware lookups) never drift.
    """
    if not name:
        return ""
    s = name.strip().lower()
    s = s.replace(" ", ".").replace("_", ".")
    while ".." in s:
        s = s.replace("..", ".")
    return s.strip(".")


def _permission_letters(v, c, m, d):
    """Turn the four boolean DB columns into the list of granted letters."""
    return [letter for letter, granted in zip("vcmd", (v, c, m, d)) if granted]


query_get_user_permissions = """
    SELECT feature, v, c, m, d
    FROM user_management
    WHERE username = %s
      AND plant_code_id = %s
"""


def get_user_permissions(username: str, plant_code_id: str) -> dict:
    """
    Returns { feature: {permission_letters} } for a user, scoped by plant_code_id, e.g.
    {"modelfactory": {"v", "c"}, "symbolicai": {"v"}}
    """
    username = normalize_username(username)
    plant_code_id = plant_code_id.strip().lower()

    driver = PostgresDriver()
    driver.config.update({"database": "decisionops"})
    conn = driver.connect()
    cursor = conn.cursor()
    try:
        cursor.execute(query_get_user_permissions, (username, plant_code_id))
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    permissions = {}
    for feature, v, c, m, d in rows:
        normalized_feature = feature.strip().lower()
        permissions.setdefault(normalized_feature, set()).update(_permission_letters(v, c, m, d))

    return permissions


query_get_all_user_permissions = """
    SELECT feature, v, c, m, d, plant_code_id
    FROM user_management
    WHERE username = %s
"""


def build_permission_dict(rows) -> dict:
    """Convert (feature, v, c, m, d, plant_code_id) rows into
    { plant_code_id: { feature: [permission_letters] } }.

    Shared by permission_check.get_all_user_permissions() and
    PermissionRegistry.get_all_user_permissions() so the row-to-dict
    transformation only lives in one place.
    """
    permissions = {}
    for feature, v, c, m, d, plant_code_id in rows:
        normalized_feature = feature.strip().lower()
        normalized_plant = plant_code_id.strip().lower()
        permissions.setdefault(normalized_plant, {}).setdefault(normalized_feature, []).extend(
            _permission_letters(v, c, m, d)
        )
    return permissions


def get_all_user_permissions(username: str) -> dict:
    """
    Returns { plant_code_id: { feature: [permission_letters] } } for a user, e.g.
    {"PLANT_A": {"modelfactory": ["v", "c"], "symbolicai": ["v"]}}
    """
    username = normalize_username(username)

    driver = PostgresDriver()
    driver.config.update({"database": "decisionops"})
    conn = driver.connect()
    cursor = conn.cursor()
    try:
        cursor.execute(query_get_all_user_permissions, (username,))
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    return build_permission_dict(rows)