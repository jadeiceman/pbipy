import time

from requests import Session

from pbipy.resources import Resource
from pbipy import _utils

class DeploymentError(Exception):
    """Error raised when a Pipeline operation meets an error"""

class Pipeline(Resource):

    _REPR = [
        "id",
        "display_name",
        "description",
        "stages",
    ]

    def __init__(
        self, 
        id: str,
        session: Session,
        raw=None,
    ) -> None:
        super().__init__(id, session)
        
        self.resource_path = f"/pipelines/{self.id}"
        self.base_path = f"{self.BASE_URL}{self.resource_path}"

        if raw:
            self._load_from_raw(raw)
    
    def get_pipeline_operation(
        self,
        operation_id: str,
    ) -> dict:
        resource = self.base_path + f"/operations/{operation_id}"
        return _utils.get_raw(
            resource,
            self.session
        )
    
    def get_pipeline_operations(
        self,
    ) -> list[dict]:
        resource = self.base_path + f"/operations"
        return _utils.get_raw(
            resource,
            self.session
        )
    
    def deploy_all(
        self,
        source_stage_order: int,
        is_backward_deployment: bool = False,
        new_workspace: dict = None,
        note: str = None,
        options: dict = None,
        update_app_settings: dict = None, 
    ) -> dict:
        request = {
            'sourceStageOrder': source_stage_order,
            'isBackwardDeployment': is_backward_deployment,
            'newWorkspace': new_workspace,
            'note': note,
            'options': options,
            'updateAppSettings': update_app_settings
        }

        prepared_request = _utils.remove_no_values(request)
        resource = self.base_path + "/deploy"
        raw = _utils.post_raw(
            resource,
            self.session,
            prepared_request
        )

        return raw
    
    def deploy_all_and_wait(
        self,
        source_stage_order: int,
        is_backward_deployment: bool = False,
        new_workspace: dict = None,
        note: str = None,
        options: dict = None,
        update_app_settings: dict = None, 
        wait_time: int = 30,
    ) -> dict:        
        
        operation_id = self.deploy_all(
            source_stage_order=source_stage_order,
            is_backward_deployment=is_backward_deployment,
            new_workspace=new_workspace,
            note=note,
            options=options,
            update_app_settings=update_app_settings
        )['id']

        operation = self.get_pipeline_operation(operation_id=operation_id)
        status = operation.get('status', 'Unknown')

        while status not in ("Succeeded", "Failed"):
            time.sleep(wait_time)
            operation = self.get_pipeline_operation(operation_id=operation_id)
            status = operation.get('status', 'Unknown')

        if status == "Failed":
            raise DeploymentError
        
        return operation

    def selective_deploy(
        self,
        source_stage_order: int,
        dashboards: list[dict] = None,
        dataflows: list[dict] = None,
        datamarts: list[dict] = None,
        datasets: list[dict] = None,
        is_backward_deployment: bool = False,
        new_workspace: dict = None,
        note: str = None,
        options: dict = None,
        reports: list[dict] = None,
        update_app_settings: dict = None, 
    ) -> dict:
        request = {
            'sourceStageOrder': source_stage_order,
            'dashboards': dashboards,
            'dataflows': dataflows,
            'datamarts': datamarts,
            'datasets': datasets,
            'isBackwardDeployment': is_backward_deployment,
            'newWorkspace': new_workspace,
            'note': note,
            'options': options,
            'reports': reports,
            'updateAppSettings': update_app_settings
        }

        prepared_request = _utils.remove_no_values(request)
        resource = self.base_path + "/deploy"
        raw = _utils.post_raw(
            resource,
            self.session,
            prepared_request
        )

        return raw
    
    def selective_deploy_and_wait(
        self,
        source_stage_order: int,
        dashboards: list[dict] = None,
        dataflows: list[dict] = None,
        datamarts: list[dict] = None,
        datasets: list[dict] = None,
        is_backward_deployment: bool = False,
        new_workspace: dict = None,
        note: str = None,
        options: dict = None,
        reports: list[dict] = None,
        update_app_settings: dict = None, 
        wait_time: int = 30,
    ) -> dict:
        
        operation_id = self.selective_deploy(
            source_stage_order=source_stage_order,
            dashboards=dashboards,
            dataflows=dataflows,
            datamarts=datamarts,
            datasets=datasets,
            is_backward_deployment=is_backward_deployment,
            new_workspace=new_workspace,
            note=note,
            options=options,
            reports=reports,
            update_app_settings=update_app_settings
        )['id']

        operation = self.get_pipeline_operation(operation_id=operation_id)
        status = operation.get('status', 'Unknown')

        while status not in ("Succeeded", "Failed"):
            time.sleep(wait_time)
            operation = self.get_pipeline_operation(operation_id=operation_id)
            status = operation.get('status', 'Unknown')

        if status == "Failed":
            raise DeploymentError
        
        return operation
