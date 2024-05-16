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

    def assign_workspace(
        self,
        stage_order: int,
        workspace_id: str,
    ) -> None:
        assign_workspace_request = {
            "workspaceId": workspace_id
        }

        resource = self.base_path + f"/stages/{stage_order}/assignWorkspace"

        _utils.post(
            resource,
            self.session,
            assign_workspace_request
        )

    def delete_pipeline(
        self,
    ) -> None:
        resource = self.base_path

        _utils.delete(
            resource,
            self.session
        )

    def delete_pipeline_user(
        self,
        identifier: str,
    ) -> None:
        resource = self.base_path + f"/users/{identifier}"
        _utils.delete(
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
        deploy_all_request = {
            'sourceStageOrder': source_stage_order,
            'isBackwardDeployment': is_backward_deployment,
            'newWorkspace': new_workspace,
            'note': note,
            'options': options,
            'updateAppSettings': update_app_settings
        }

        prepared_request = _utils.remove_no_values(deploy_all_request)
        resource = self.base_path + "/deployAll"
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
    
    def get_pipeline_stage_artifacts(
        self,
        stage_order: int,
    ) -> dict:
        resource = self.base_path + f"/stages/{stage_order}/artifacts"
        return _utils.get_raw(
            resource,
            self.session,
        )
    
    def get_pipeline_stages(
        self,
    ) -> list[dict]:
        resource = self.base_path + "/stages"
        return _utils.get_raw(
            resource,
            self.session
        )
    
    def get_pipeline_users(
        self,
    ) -> list[dict]:
        resource = self.base_path + "/users"
        return _utils.get_raw(
            resource,
            self.session
        )

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
        selective_deploy_request = {
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

        prepared_request = _utils.remove_no_values(selective_deploy_request)
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
    
    def unassign_workspace(
        self,
        stage_order: int,
    ) -> None:
        resource = self.base_path + f"/stages/{stage_order}/unassignWorkspace"

        _utils.post(
            resource,
            self.session
        )

    def update_pipeline(
        self,
        description: str = None,
        display_name: str = None,
    ) -> dict:
        resource = self.base_path

        update_pipeline_request = {
            "description": description,
            "displayName": display_name,
        }

        prepared_request = _utils.remove_no_values(update_pipeline_request)

        _utils.patch(
            resource,
            self.session,
            prepared_request
        )

    def update_pipeline_user(
        self,
        identifier: str,
        principal_type: str,
        access_right: str = None,
    ) -> None:
        resource = self.base_path

        update_pipeline_user_request = {
            "identifier": identifier,
            "principal_type": principal_type,
            "access_right": access_right
        }

        prepared_request = _utils.remove_no_values(update_pipeline_user_request)

        _utils.post(
            resource,
            self.session,
            prepared_request
        )


