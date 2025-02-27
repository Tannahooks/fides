"""
This script is a utility for interactively annotating data categories in the dataset manifest
"""

from typing import Union, List

import click

## TODO: decouple this from fidesapi, it should be using API calls
from fidesapi.crud import list_resource
from fidesapi.sql_models import sql_model_map
from fidesctl.core import parse as core_parse
from fidesctl.core import visualize
from fideslang import manifests
from fideslang.models import Dataset, DatasetCollection, DatasetField, FidesKey
from fideslang.validation import FidesValidationError


class AnnotationAbortError(Exception):
    """
    Custom exception to handle mid annotation abort
    """


def validate_data_categories(
    categories: List[str], valid_categories: List[str]
) -> None:
    """
    Check to see if each category in a list is valid
    Args:
        categories: list of category strings to check
        valid_categories: list of valid categories

    Returns:

    """
    for category in categories:
        FidesKey.validate(category)
        if category not in valid_categories:
            raise ValueError


def get_data_categories_annotation(
    dataset_member: Union[Dataset, DatasetCollection, DatasetField],
    valid_categories: List[str],
    validate: bool = True,
) -> List[str]:
    """
    Request the user's input to supply a list of data categories

    Args:
        dataset_member: A member of a dataset [dataset, collection, field]
        valid_categories: a list of the valid data categories
        validate: flag to check user inputs for formatting and data category presence

    Returns:
        List of the user's input
    """
    msg = f"""Enter comma separated data categories for [{dataset_member.name}] [s: skip, q: quit]"""
    user_response = [input.strip() for input in click.prompt(msg).split(",")]

    if user_response == ["s"]:
        return []

    if user_response == ["q"]:
        if click.confirm(
            "Are you sure you want to quit annotating the dataset? (progress will be saved)"
        ):
            raise AnnotationAbortError
        return get_data_categories_annotation(dataset_member, valid_categories)

    if validate:
        try:
            validate_data_categories(user_response, valid_categories)
        except (FidesValidationError, ValueError):
            click.secho(
                f"[{user_response}] is not a valid data category, please re-confirm and try again!",
                fg="red",
            )
            user_response = get_data_categories_annotation(
                dataset_member, valid_categories
            )

    return user_response


def annotate_dataset(
    dataset_file: str,
    resource_type: str = "data_category",
    annotate_all: bool = False,
    validate: bool = True,
) -> None:
    """
    Given a dataset.yml-like file, walk the user through an interactive cli to provide data categories
    for members of the dataset that do not have any specified
    Args:
        dataset_file: the file name for the dataset to annotate
        resource_type: the type of data resource to point to for assistance (via visualization web page)
        annotate_all: flag to annotate all members of a dataset (default False: only annotate fields)
        validate: flag to check user inputs for formatting and data category presence

    Returns:
        Write the amended dataset file in place
    """
    output_dataset = []

    # Make the user aware of the data_categories visualizer
    click.secho(
        f"""For reference, open the data category visualizer at either (localhost if running container):
        {visualize.get_visualize_url(resource_type, "graphs")}
        {visualize.get_visualize_url(resource_type, "text")}
    """,
        fg="green",
    )

    datasets = core_parse.ingest_manifests(dataset_file)["dataset"]

    existing_categories = [
        resource.__dict__["fides_key"]
        for resource in list_resource(sql_model_map[resource_type])
    ]

    for dataset in datasets:
        current_dataset = Dataset(**dataset)
        click.secho(f"\n####\nAnnotating Dataset: [{current_dataset.name}]")

        if annotate_all and not current_dataset.data_categories:
            try:
                click.secho(f"Database [{current_dataset.name}] has no data categories")
                current_dataset.data_categories = get_data_categories_annotation(
                    dataset_member=current_dataset,
                    valid_categories=existing_categories,
                    validate=validate,
                )
            except AnnotationAbortError:
                break

        for table in current_dataset.collections:
            click.secho(f"####\nAnnotating Table: [{table.name}]\n")
            if annotate_all and not table.data_categories:
                try:
                    click.secho(f"Table [{table.name}] has no data categories")
                    table.data_categories = get_data_categories_annotation(
                        dataset_member=table,
                        valid_categories=existing_categories,
                        validate=validate,
                    )
                except AnnotationAbortError:
                    break

            for field in table.fields:
                if not field.data_categories:
                    click.secho(
                        f"Field [{table.name}.{field.name}] has no data categories\n"
                    )
                    try:
                        user_categories = get_data_categories_annotation(
                            dataset_member=field,
                            valid_categories=existing_categories,
                            validate=validate,
                        )
                        click.secho(
                            f"""Setting data categories for {table.name}.{field.name} to:
                        {user_categories}\n"""
                        )
                        field.data_categories = user_categories
                    except AnnotationAbortError:
                        break
    else:
        output_dataset.append(current_dataset.dict())
    manifests.write_manifest(dataset_file, output_dataset, "dataset")
