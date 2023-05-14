from .recipe_commons import get_recipe_settings_and_dictionary


def compute_prepare_rename_step(column_to_rename, new_column_name):
    """
    Computes the JSON associated with a prepare 'rename' step.

    :param column_to_rename: str: Name of the column to rename.
    :param new_column_name: str: New name the column to rename should have. 
    
    :returns: rename_step: dict: JSON of the prepare 'rename' step.
    """
    rename_step = {"preview": False,
                   "metaType": "PROCESSOR",
                   "disabled": False,
                   "type": "ColumnRenamer",
                   "params": {"renamings": [
                       {"from": "{}".format(column_to_rename),
                        "to": "{}".format(new_column_name)}]},
                   "alwaysShowComment": False}
    return rename_step


def compute_prepare_formula_step(column_name, formula_expression):
    """
    Computes the JSON associated with a column applying a prepare recipe formula.

    :param column_to_rename: str: Name of the column to generate.
    :param formula_expression: str: Expression of the formula leading to the computed column, following the 
        DSS formula language (https://doc.dataiku.com/dss/latest/formula/index.html).
    
    :returns: formula_step: dict: JSON of the prepare 'formula' step.
    """
    formula_step = {'preview': False,
                    'metaType': 'PROCESSOR',
                    'disabled': False,
                    'type': 'CreateColumnWithGREL',
                    'params': {
                        'expression': formula_expression,
                        'column': column_name
                        },
                    'alwaysShowComment': False}
    return formula_step


def compute_prepare_keep_or_delete_step(columns_of_interest, bool_keep_columns):
    """
    Computes the JSON associated with a keep/delete prepare recipe step.

    :param columns_of_interest: list: Name of the column to generate.
    :param bool_keep_columns: bool: Parameter precising if the columns mentioned in 
        'columns_of_interest' should be kept (if it is equal to 'True')
        or deleted (if it is equal to 'False').
    
    :returns: formula_step: dict: JSON of the prepare 'keep/delete' step.
    """
    keep_or_delete_step = {'preview': False,
                           'metaType': 'PROCESSOR',
                           'disabled': False,
                           'type': 'ColumnsSelector',
                           'params': {'columns': columns_of_interest,
                                      'keep': bool_keep_columns,
                                      'appliesTo': 'COLUMNS'},
                           'alwaysShowComment': False}
    return keep_or_delete_step


def reset_prepare_recipe_steps(project, recipe_name):
    """
    Reset all steps of a prepare recipe .

    :param project: dataikuapi.dss.project.DSSProject: A handle to interact with a project on the DSS instance.
    :param :recipe_name: str: Name of the plugin recipe.
    """
    recipe_settings, __ = get_recipe_settings_and_dictionary(project, recipe_name, False)
    recipe_json_payload = recipe_settings.get_json_payload()
    recipe_json_payload["steps"] = []
    recipe_settings.set_json_payload(recipe_json_payload)
    recipe_settings.save()
    pass


def add_step_in_prepare_recipe(project, recipe_name, step, step_comment="",
                               show_step_comment=True):
    """
    Adds a step in a prepare recipe .

    :param project: dataikuapi.dss.project.DSSProject: A handle to interact with a project on the DSS instance.
    :param :recipe_name: str: Name of the plugin recipe.
    :param :step: dict: Definition of the prepare recipe step in JSON format.
    :param :step_comment: str: Comment to link to the recipe step.
    :param :show_step_comment: bool: Parameter precising whether the step comment should be displayed or not
        in the recipe.
    """
    recipe_settings, __ = get_recipe_settings_and_dictionary(project, recipe_name, False)
    recipe_json_payload = recipe_settings.get_json_payload()
    step["alwaysShowComment"] = show_step_comment
    if step_comment != "":
        step["comment"] = step_comment
    recipe_json_payload["steps"].append(step)
    recipe_settings.set_json_payload(recipe_json_payload)
    recipe_settings.save()
    pass


def compute_prepare_recipe_group_step(group_step_label, group_sub_steps, group_step_comment="",
                                      show_group_step_comment=True):
    """
    Computes the JSON associated with a prepare recipe grouping step.

    :param :group_step_label: str: Label to link to the group step.
    :param :group_sub_steps: list: List containing all the group sub-steps, each being in JSON format.
    :param :group_step_comment: str: Comment to link to the group step.
    :param :show_group_step_comment: bool: Parameter precising whether the group step comment should be displayed or not
        in the recipe.
    
    :returns: group_step: dict: JSON of the prepare grouping step.
    """
    group_step = {'metaType': 'GROUP',
                       'name': group_step_label,
                       'steps': group_sub_steps,
                       'alwaysShowComment': show_group_step_comment,
                       'preview': False,
                       'disabled': False}
    if group_step_comment != "":
        group_step["comment"] = group_step_comment
    return group_step


def compute_prepare_recipe_columns_percent_of_total_steps(columns_of_interest,
                                                          name_for_columns_sum,
                                                          grouped_step_label,
                                                          bool_remove_columns_sum,
                                                          bool_remove_initial_columns):
    """
    Takes a set of numerical columns to scale them by their sum. The computed column values then corresponds to the 
        percent of their total. All steps leading to this computation are grouped together in a prepare grouping step.

    :param :columns_of_interest: list: The set of columns to scale by their sum.
    :param :name_for_columns_sum: str: Name of the column corresponding to the sum of all columns
        defined in 'columns_of_interest'.
    :param :grouped_step_label: str: Label to link to the group step containing all computation sub-steps.
    :param :bool_remove_columns_sum: bool: Parameter precising whether the column 'name_for_columns_sum' should be removed
        after the computation or not.
    :param :bool_remove_initial_columns: bool: Parameter precising whether the columns defined in 
        'bool_remove_initial_columns' should be removed  after the computation or not.

    :returns: group_step: dict: JSON of the prepare grouping step.
    """
    all_steps = []
    total_column_expression = ""
    columns_last_index = len(columns_of_interest) - 1
    columns_percent_of_total_steps = []
    for column_index, column_name in enumerate(columns_of_interest):
        if column_index != columns_last_index:
            column_expression_end = " + "
        else:
            column_expression_end = ""
        total_column_expression += "if(isNonBlank({0}), {0}, 0){1}".format(column_name, column_expression_end)
        column_percent_of_total_expression = "if(isNonBlank({0}), {0}/{1}, 0)".format(column_name, name_for_columns_sum)
        column_percent_of_total_name = "{}_fraction".format(column_name)
        column_percent_of_total_step = compute_prepare_formula_step(column_percent_of_total_name,
                                                                    column_percent_of_total_expression)
        columns_percent_of_total_steps.append(column_percent_of_total_step)
    total_column_formula_step = compute_prepare_formula_step(name_for_columns_sum, total_column_expression)
    
    all_steps.append(total_column_formula_step)
    all_steps += columns_percent_of_total_steps
    columns_to_remove = []
    if bool_remove_initial_columns:
        columns_to_remove += columns_of_interest
    if bool_remove_columns_sum:
        columns_to_remove.append(name_for_columns_sum)
    if bool_remove_initial_columns or bool_remove_columns_sum:
        columns_deletion_step = compute_prepare_keep_or_delete_step(columns_to_remove, False)
        all_steps.append(columns_deletion_step)
    
    recipe_columns_percent_of_total_group_step = compute_prepare_recipe_group_step(grouped_step_label,
                                                                                   all_steps,
                                                                                   group_step_comment="",
                                                                                   show_group_step_comment=True)
    return recipe_columns_percent_of_total_group_step
