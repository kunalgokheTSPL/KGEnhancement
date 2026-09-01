from pydantic import BaseModel, Field, model_validator

from p0.user_management.authorization.permission_check import normalize_username


# PERMISSIONS MODEL
class Permissions(BaseModel):

    v: list[list[bool]]
    c: list[list[bool]]
    m: list[list[bool]]
    d: list[list[bool]]


# CONFIG REQUEST
class ConfigRequest(BaseModel):

    users: list[str] = Field(..., min_length=1)

    features: list[str] = Field(..., min_length=1)

    permissions: Permissions

    plant_code_id: str = Field(..., min_length=1)

    @model_validator(mode="after")
    def validate_permissions(self):

        self.plant_code_id = self.plant_code_id.strip().lower()
        user_count = len(self.users)
        feature_count = len(self.features)

        for permission_name in ("v", "c", "m", "d"):

            matrix = getattr(
                self.permissions,
                permission_name
            )

            if len(matrix) != user_count:
                raise ValueError(
                    f"'{permission_name}' must contain "
                    f"{user_count} rows"
                )

            for row_index, row in enumerate(matrix):

                if len(row) != feature_count:
                    raise ValueError(
                        f"'{permission_name}' row {row_index} "
                        f"must contain {feature_count} values"
                    )

        return self


# VIEW REQUEST
class ViewRequest(BaseModel):

    users: list[str] = Field(..., min_length=1)

    features: list[str] = Field(..., min_length=1)

    plant_code_id: str = Field(..., min_length=1)

    @model_validator(mode="after")
    def normalize_plant_code(self):
        self.plant_code_id = self.plant_code_id.strip().lower()
        return self


# NORMALIZE LIST
def normalize_list(
    values: list[str],
    value_name: str
) -> list[str]:
    """Trim/lowercase/dedupe values. Usernames additionally get the
    space/underscore -> dot normalization via normalize_username(),
    the same function used everywhere else a username is resolved
    (session lookups, middleware checks), so config-time and
    request-time normalization can never drift apart.
    """
    normalized_vals = []
    for value in values:
        if value and value.strip():
            s = normalize_username(value) if value_name == "users" else value.strip().lower()
            normalized_vals.append(s)

    normalized = list(
        dict.fromkeys(normalized_vals)
    )

    if not normalized:
        raise ValueError(
            f"At least one valid {value_name} is required"
        )

    return normalized


# CONFIG QUERY
query_config_request = """
    INSERT INTO user_management
    (
        username,
        feature,
        v,
        c,
        m,
        d,
        plant_code_id
    )

    SELECT
        u.username,
        f.feature,

        p.v[u.user_index][f.feature_index],
        p.c[u.user_index][f.feature_index],
        p.m[u.user_index][f.feature_index],
        p.d[u.user_index][f.feature_index],

        %s AS plant_code_id

    FROM
        unnest(%s::text[])
        WITH ORDINALITY
        AS u(
            username,
            user_index
        )

    CROSS JOIN
        unnest(%s::text[])
        WITH ORDINALITY
        AS f(
            feature,
            feature_index
        )

    CROSS JOIN (
        SELECT
            %s::boolean[][] AS v,
            %s::boolean[][] AS c,
            %s::boolean[][] AS m,
            %s::boolean[][] AS d
    ) AS p

    ON CONFLICT (
        username,
        feature,
        plant_code_id
    )

    DO UPDATE SET
        v = EXCLUDED.v,
        c = EXCLUDED.c,
        m = EXCLUDED.m,
        d = EXCLUDED.d;
"""


# VIEW QUERY
query_view_request = """
    WITH requested_users AS (

        SELECT
            username,
            user_index

        FROM
            unnest(%s::text[])
            WITH ORDINALITY
            AS u(
                username,
                user_index
            )
    ),

    requested_features AS (

        SELECT
            feature,
            feature_index

        FROM
            unnest(%s::text[])
            WITH ORDINALITY
            AS f(
                feature,
                feature_index
            )
    ),

    permission_matrix AS (

        SELECT
            u.user_index,

            array_agg(
                COALESCE(p.v, FALSE)
                ORDER BY f.feature_index
            ) AS v_row,

            array_agg(
                COALESCE(p.c, FALSE)
                ORDER BY f.feature_index
            ) AS c_row,

            array_agg(
                COALESCE(p.m, FALSE)
                ORDER BY f.feature_index
            ) AS m_row,

            array_agg(
                COALESCE(p.d, FALSE)
                ORDER BY f.feature_index
            ) AS d_row

        FROM requested_users u

        CROSS JOIN requested_features f

        LEFT JOIN user_management p
            ON p.username = u.username
            AND p.feature = f.feature
            AND p.plant_code_id = %s

        GROUP BY
            u.user_index
    )

    SELECT
        array_agg(
            v_row
            ORDER BY user_index
        ) AS v,

        array_agg(
            c_row
            ORDER BY user_index
        ) AS c,

        array_agg(
            m_row
            ORDER BY user_index
        ) AS m,

        array_agg(
            d_row
            ORDER BY user_index
        ) AS d

    FROM permission_matrix;
"""


# GET USER PERMISSIONS QUERY
query_get_user_permissions = """
    SELECT
        feature,
        v,
        c,
        m,
        d,
        plant_code_id

    FROM user_management

    WHERE username = %s

    ORDER BY feature;
"""