from .recipe_commons import get_recipe_settings_and_dictionary, get_recipe_input_datasets
from ..datasets.dataset_commons import get_dataset_column_datatypes_mapping


def define_pivot_recipe_aggregations(project,
                                     recipe_name,
                                     row_identifiers,
                                     columns_to_pivot,
                                     column_aggregations_mapping,
                                     pivoted_values_selection_strategy="TOP_N",
                                     max_number_of_pivoted_column_values=20,
                                     minimum_number_of_occurences=2,
                                     bool_compute_global_count=False,
                                     bool_recompute_schema_at_each_run=True):
    """
    Set aggregations done by a pivot recipe.
    :param project: dataikuapi.dss.project.DSSProject: A handle to interact with a project on the DSS instance.
    :param recipe_name: str: Name of the recipe.
    :param row_identifiers: list: List of the colunms that should be used as row identifiers.
    :param columns_to_pivot: list: List of the columns from which values should be pivoted.
    :param column_aggregations_mapping: dict: Mapping between the columns and the list of aggregations to apply to each one.
        Example: {'column_1': ['min', 'concat'], 'column_2': ['avg', 'sum']} 
    :param: pivoted_values_selection_strategy: str: Parameter setting the way pivoted values should be selected.
        It corresponds to the 'Pivoted values' section in the visual recipe.
        It must have a value in  '["TOP_N", "NO_LIMIT", "AT_LEAST_N_OCC"]'
        - 'TOP_N' corresponds to the choice 'most frequent' in the visual recipe.
        - 'NO_LIMIT' corresponds to the choice 'all' in the visual recipe.
        - 'AT_LEAST_N_OCC' corresponds to the choice 'occuring more than' in the visual recipe.
    :param max_number_of_pivoted_column_values: int: Sets the maximum number of values to pivot, in cases
       when (pivoted_values_selection_strategy == 'TOP_N')
    :param minimum_number_of_occurences: int: Sets the minimum number of values a column category must have
        to be pivoted, in cases when (pivoted_values_selection_strategy == 'AT_LEAST_N_OCC')
    :param bool_compute_global_count: bool: Precises whether the count of recors should be computed or not
         for each pivoted value.
    :param bool_recompute_schema_at_each_run: bool: Precises whether the recipe's schema should be recomputed at the runtime
        or not.        
    """
    print("Updating recipe '{}' aggregations ...".format(recipe_name))

    PIVOT_DEFAULT_AGGREGATIONS = {'avg': False,
                                  'column': '',
                                  'concat': False,
                                  'count': False,
                                  'countDistinct': False,
                                  'first': False,
                                  'last': False,
                                  'max': False,
                                  'min': False,
                                  'stddev': False,
                                  'sum': False,
                                  'type': ''}
    PIVOT_ALLOWED_AGGREGATIONS = ["avg", "concat", "count", "countDistinct", "first",
                                  "last", "max", "min", "stddev", "sum"]
    PIVOT_ALLOWED_VALUES_SELECTION_STRATEGY = ["TOP_N", "NO_LIMIT", "AT_LEAST_N_OCC"]
    recipe_settings, __ = get_recipe_settings_and_dictionary(project, recipe_name, False)
    recipe_json_payload = recipe_settings.get_json_payload()
    
    # Checking columns to pivot settings:
    for column_name in columns_to_pivot:
        if column_name in row_identifiers:
            log_message = "You can't pivot column '{}' as it is already used as a recipe rows identifier!".format(column_name)
            raise Exception(log_message)
    columns_to_aggregate = column_aggregations_mapping.keys()
    # Checking columns to aggregate settings:
    for column_name in columns_to_aggregate:
        if column_name in row_identifiers:
            log_message = "You can't aggregate column '{}' as it is already used as a recipe rows identifier!".format(column_name)
            raise Exception(log_message)
        elif column_name in columns_to_pivot:
            log_message = "You can't aggregate column '{}' as it is already used as a recipe column to pivot!".format(column_name)
            raise Exception(log_message)
    
    # Checking recipe pivoted values selection settings:
    if pivoted_values_selection_strategy not in PIVOT_ALLOWED_VALUES_SELECTION_STRATEGY:
        log_message = "Current pivoted_values_selection_strategy is '{}' and is not compatible with the "\
        "pivoted values selection strategies allowed by this function. Please a pivot values selection strategy"\
        "in '{}'".format(pivoted_values_selection_strategy, PIVOT_ALLOWED_VALUES_SELECTION_STRATEGY)
        raise Exception(log_message)
    
    # Setting row identifiers:
    recipe_json_payload["explicitIdentifiers"] = row_identifiers
    # Setting columns to pivot:
    recipe_json_payload["pivots"][0]["keyColumns"] = columns_to_pivot
    
    recipe_new_aggregations = []
    recipe_input_dataset = get_recipe_input_datasets(project, recipe_name)[0]
    recipe_input_dataset_column_datatypes = get_dataset_column_datatypes_mapping(project, recipe_input_dataset)
    for column_name in columns_to_aggregate:
        column_aggregations = column_aggregations_mapping[column_name]
        
        for aggregation in PIVOT_ALLOWED_AGGREGATIONS:
            pivot_column_aggregation = PIVOT_DEFAULT_AGGREGATIONS.copy()
            pivot_column_aggregation["column"] = column_name
            pivot_column_aggregation["type"] = recipe_input_dataset_column_datatypes[column_name]
            if aggregation in column_aggregations:
                pivot_column_aggregation[aggregation] = True
                recipe_new_aggregations.append(pivot_column_aggregation)
                
    # Setting recipe new aggregations:
    recipe_json_payload["pivots"][0]["valueColumns"] = recipe_new_aggregations
    
    # Setting recipe pivoted values selection settings: 
    recipe_json_payload["pivots"][0]["explicitValues"] = []
    recipe_json_payload["pivots"][0]["globalCount"] = bool_compute_global_count
    recipe_json_payload["pivots"][0]["valueLimit"] = pivoted_values_selection_strategy
    recipe_json_payload["pivots"][0]["topnLimit"] = max_number_of_pivoted_column_values
    recipe_json_payload["pivots"][0]["minOccLimit"] = minimum_number_of_occurences
    
    if bool_recompute_schema_at_each_run:
        recipe_json_payload["schemaComputation"] = "ALWAYS"
    else:
        recipe_json_payload["schemaComputation"] = "ONLY_IF_NO_METADATA"
        
    recipe_settings.set_json_payload(recipe_json_payload)
    recipe_settings.save()
    print("Recipe '{}' aggregations successfully updated !".format(recipe_name))
    pass
    
