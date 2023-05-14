from .visual_ml_commons import get_deployed_model_active_version_id


def update_ml_task_classification_metric(ml_task_settings, classification_metric):
    """
    Updates the optimization metric set in a ml_task classification model.
    :param ml_task_settings: dataikuapi.dss.ml.DSSClassificationMLTaskSettings: DSS MLTask settings object.
    :param classification_metric: str: Classification metric to set. It must be in 
        ["F1", "ACCURACY", "PRECISION", "RECALL", "COST_MATRIX", "AUC", "LOG_LOSS", "CUMULATIVE_LIFT", "CUSTOM"]
        With: 
        - "F1": "F1 score"
        - "ACCURACY": "Accuracy",
        - "PRECISION": "Precision"
        - "RECALL": "Recall"
        - "COST_MATRIX": "Cost matrix"
        - "AUC": "Area Under the Curve"
        - "LOG_LOSS": "Log loss"
        - "CUMULATIVE_LIFT": "Cumulative lift"
        - "CUSTOM": "Custom code"
    """
    ALLOWED_CLASSIFICATION_METRICS = ["F1", "ACCURACY", "PRECISION", "RECALL", "COST_MATRIX",
                                      "AUC", "LOG_LOSS", "CUMULATIVE_LIFT", "CUSTOM"]
    if classification_metric not in ALLOWED_CLASSIFICATION_METRICS:
        log_message = "You can't use metric '{}' in this function! Allowed metrics are '{}'"\
        .format(classification_metric, ALLOWED_CLASSIFICATION_METRICS)
        raise Exception(log_message)
    ml_task_settings.set_metric(metric=classification_metric)
    ml_task_settings.save()
    pass


def update_binary_classification_ml_task_target_balance(ml_task_settings, classes_labels, class_1_frequency):
    """
    Updates the the target classes balance in a ml_task 'BINARY_CLASSIFICATION' model.
    :param ml_task_settings: dataikuapi.dss.ml.DSSClassificationMLTaskSettings: DSS MLTask settings object.
    :param classes_labels: dict: Dictionary mapping the 0 and 1 classes toward their labels. 
        - Example: classes_labels = {0: 'not_fraud', 1: 'fraud'}
    :param class_1_frequency: double: frequency of the class 1 in the target.
    """
    prediction_type = ml_task_settings.get_raw()["predictionType"]
    if prediction_type != "BINARY_CLASSIFICATION":
        log_message = "This function only works for predictions of type 'BINARY_CLASSIFICATION'!"
        "current prediction type is '{}'".format(prediction_type)
        raise Exception(log_message)
        
    ml_task_class_1_frequency = 10000 * class_1_frequency
    ml_task_class_0_frequency = 10000 - ml_task_class_1_frequency
    
    new_target_remapping = []
    for class_value in classes_labels.keys():
        class_label = classes_labels[class_value]
        if class_label == 1:
            class_frequency = ml_task_class_1_frequency
        else:
            class_frequency = ml_task_class_0_frequency
        new_target_remapping.append({'sourceValue': "{}".format(class_label),
                                     'mappedValue': class_value,
                                     'sampleFreq': class_frequency})
    ml_task_settings.get_raw()['preprocessing']['target_remapping'] = new_target_remapping
    ml_task_settings.save()
    pass


def get_deployed_model_used_threshold(project, deployed_model_id):
    """
    Retrieves the threshold used by the active version of a deployed model .
    
    :param project: dataikuapi.dss.project.DSSProject: A handle to interact with a project on the DSS instance.
    :param: deployed_model_id: str: ID of the deployed model.
    
    :returns deployed_model_used_threshold: double: Threshold used by the active version of the deployed model.
    """
    deployed_model = project.get_saved_model(deployed_model_id)
    model_active_version_id = get_deployed_model_active_version_id(project, deployed_model_id)
    deployed_model_version_details = deployed_model.get_version_details(model_active_version_id)
    deployed_model_used_threshold = deployed_model_version_details.details["perf"]["usedThreshold"]
    return deployed_model_used_threshold
