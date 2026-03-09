"""Stream type classes for tap-noaa-gfs."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_noaa_gfs.client import NoaaGfsStream

# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class UsersStream(NoaaGfsStream):
    """Define custom stream."""

    name = "users"
    primary_keys = ("id",)
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property(
            "id",
            th.StringType,
            description="The user's system ID",
        ),
        th.Property(
            "age",
            th.IntegerType,
            description="The user's age in years",
        ),
        th.Property(
            "email",
            th.StringType,
            description="The user's email address",
        ),
        th.Property("street", th.StringType),
        th.Property("city", th.StringType),
        th.Property(
            "state",
            th.StringType,
            description="State name in ISO 3166-2 format",
        ),
        th.Property("zip", th.StringType),
    ).to_dict()


class GroupsStream(NoaaGfsStream):
    """Define custom stream."""

    name = "groups"
    primary_keys = ("id",)
    replication_key = "modified"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("modified", th.DateTimeType),
    ).to_dict()
