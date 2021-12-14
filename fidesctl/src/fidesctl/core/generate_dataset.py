"""Module that generates valid dataset manifest files from various data sources."""
from typing import Dict, List

import sqlalchemy
from sqlalchemy.engine import Engine

from fideslang import manifests
from fideslang.models import Dataset, DatasetCollection, DatasetField
from .utils import echo_green, get_db_engine


def get_db_collections_and_fields(engine: Engine) -> Dict[str, Dict[str, List[str]]]:
    """
    Get the name of every field in every collection of a database.

    Args:
        `engine`: A sqlalchemy database connection Engine

    Returns:
        `db_tables`: A dictionary containing a mapping of every field in
        every collection of a database. Example:
            `{ schema_name: { schema_name.collection_name: [field_name_1, field_name_2, ...] } }`
    """
    inspector = sqlalchemy.inspect(engine)

    excluded_schemas = ["information_schema"]
    if engine.dialect.name == "mysql":
        # Exclude system DB schemas for MySQL databases
        excluded_schemas.extend(["mysql", "performance_schema", "sys"])

    schemas = [
        schema
        for schema in inspector.get_schema_names()
        if schema not in excluded_schemas
    ]

    db_tables: Dict[str, Dict[str, List]] = {}
    for schema in schemas:
        db_tables[schema] = {}
        for table in inspector.get_table_names(schema=schema):
            db_tables[schema][f"{schema}.{table}"] = [
                column["name"] for column in inspector.get_columns(table, schema=schema)
            ]

    return db_tables


def create_dataset_collections(
    db_tables: Dict[str, Dict[str, List[str]]]
) -> List[Dataset]:
    """
    Create a list of collections and their contained fields, formatted
    for a fidesctl manifest. Includes a placeholder description for each.
    """
    return [
        Dataset(
            fides_key=schema_name,
            name=schema_name,
            description=f"Fides Generated Description for Schema: {schema_name}",
            collections=[
                DatasetCollection(
                    name=table_name,
                    description=f"Fides Generated Description for Table: {table_name}",
                    fields=[
                        DatasetField(
                            name=column,
                            description=f"Fides Generated Description for Column: {column}",
                            data_categories=[],
                        )
                        for column in table
                    ],
                )
                for table_name, table in schema.items()
            ],
        )
        for schema_name, schema in db_tables.items()
    ]


def create_dataset(engine: Engine, collections: List[DatasetCollection]) -> Dataset:
    """
    Generate a partial dataset manifest, sans tables/fields,
    given a database engine.
    """
    url = engine.url
    name = url.database
    dataset = Dataset(
        fides_key=name,
        name=name,
        description=f"Fides Generated Description for Dataset: {name}",
        collections=collections,
    )
    return dataset


def generate_dataset(connection_string: str, file_name: str) -> None:
    """
    Given a database connection string, extract all tables/fields from it
    and write out a boilerplate dataset manifest.
    """
    db_engine = get_db_engine(connection_string)
    db_collections = get_db_collections_and_fields(db_engine)
    collections = create_dataset_collections(db_collections)
    manifests.write_manifest(file_name, [i.dict() for i in collections], "dataset")
    echo_green(f"Successfully generated dataset manifest: {file_name}")
