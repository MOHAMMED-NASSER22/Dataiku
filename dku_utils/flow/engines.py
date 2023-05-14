def get_flow_engines_priority(project):
    """Retrieves the engines priority set in a project settings.
    :param project: dataikuapi.dss.project.DSSProject: A handle to interact with a project on the DSS instance.
    :returns: flow_engines_priority: str: The prokect's engines priority.
    """
    flow_engines_priority = project.get_settings().settings["settings"]["recipeEnginesPreferences"]["enginesPreferenceOrder"]
    return flow_engines_priority