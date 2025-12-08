{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "dc8e2954-84b8-4724-aeef-0ec1e274222c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%pip install databricks-sdk==0.61.0\n",
    "%restart_python"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "f624ce16-6f5f-4d0e-b9f7-9673b49889c0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from databricks.sdk import WorkspaceClient\n",
    "from databricks.sdk.service.apps import App, AppDeployment\n",
    "from databricks.sdk.service.workspace import WorkspaceObjectAccessControlRequest, WorkspaceObjectPermissionLevel\n",
    "\n",
    "w = WorkspaceClient()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "1a313f72-f569-4d28-a887-f494bcf96a4b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "name = 'databricks-presto-converter'\n",
    "description = 'An application for converting presto SQL to Databricks SQL using sqlglot.'\n",
    "notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)\n",
    "default_source_code_path = '/Workspace' + notebook_path.rsplit('/', 1)[0] if notebook_path else None\n",
    "\n",
    "app = w.apps.create_and_wait(\n",
    "    app=App(\n",
    "        name=name,\n",
    "        description=description,\n",
    "        default_source_code_path=default_source_code_path,\n",
    "    )\n",
    ")\n",
    "print(app)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "b1b7cd95-0d48-43ab-ac07-f00c3768655e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "app_deployment = w.apps.deploy_and_wait(\n",
    "    app_name=name,\n",
    "    app_deployment=AppDeployment(\n",
    "        source_code_path=default_source_code_path\n",
    "    )\n",
    ")\n",
    "print(app_deployment)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "fefeb5bf-2f9d-4dbf-ac01-872bac1ec468",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "app_deployment = w.apps.deploy_and_wait(\n",
    "    app_name=name,\n",
    "    app_deployment=AppDeployment(\n",
    "        source_code_path=default_source_code_path\n",
    "    )\n",
    ")\n",
    "print(app_deployment)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "3887746f-c525-4636-bbdc-fa5475379b53",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "obj_info = w.workspace.get_status(default_source_code_path)\n",
    "\n",
    "update_permissions = w.workspace.update_permissions(\n",
    "    workspace_object_type='directories',\n",
    "    workspace_object_id=obj_info.object_id,\n",
    "    access_control_list=[\n",
    "        WorkspaceObjectAccessControlRequest(\n",
    "            service_principal_name=app.service_principal_client_id,\n",
    "            permission_level=WorkspaceObjectPermissionLevel.CAN_READ\n",
    "        )\n",
    "    ]\n",
    ")\n",
    "print(update_permissions)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "37c7103e-ad42-4606-b84b-e71d4355afb9",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "print(f'Application deployed successfully. URL: {app.url}')\n",
    "print(f'Service principal of the app: Name - {app.service_principal_name}, ID - {app.service_principal_client_id}')"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": {
    "hardware": {
     "accelerator": null,
     "gpuPoolId": null,
     "memory": null
    }
   },
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "4"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 2
   },
   "notebookName": "app_deployer.py",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
