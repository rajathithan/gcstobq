# Cloud Function code to trigger dataflow flex template
# Author: Rajathithan Rajasekar 
# Version : 1.0
# Date: 08/05/2023

from __future__ import annotations
from typing import Any
import google.auth
from google.auth.transport.requests import AuthorizedSession
import requests
import functions_framework

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])
projectId= "mercurial-smile-386805"
location="us-central1"
flex_launch_url = "https://dataflow.googleapis.com/v1b3/projects/{projectId}/locations/{location}/flexTemplates:launch".format(projectId=projectId,location=location)
templatePath = "gs://xmltestpoc/template/gcstobq-flex-template-v1.json"

parameters = {
    "launchParameter": {
    "jobName": "function-trigger",
    "containerSpecGcsPath": templatePath,
  }
}

def flex_rest_api_request(
    url: str, method: str = "POST", **kwargs: Any
) -> google.auth.transport.Response:
    authed_session = AuthorizedSession(CREDENTIALS)
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90
    return authed_session.request(method, url, **kwargs)


@functions_framework.http
def trigger_job(request: dict) -> str:
    request_json = request.get_json(silent=True)
    try:
        request_json['activity']
    except Exception as error:
        return f"No json object found, specifiy activity key-{error}\n"
    if request_json['activity'] == "start-job":    
        print("starting the rest-api request !!")  
        response = flex_rest_api_request(
            flex_launch_url,
            method="POST",
            json=parameters
        )
        if response.status_code == 400:
            raise requests.HTTPError(
                "Wrong arguments passed to Dataflow flex-template launch API."
                f"{response.headers} / {response.text}\n"
            )
        elif response.status_code == 401:
            raise requests.HTTPError(
                "You do not have a permission to perform this operation. "
                "Check if service account has necessary roles and permissions."
                f"{response.headers} / {response.text}\n"
            )
        elif response.status_code == 403:
            raise requests.HTTPError(
                "You do not have a permission to perform this operation. "
                f"{response.headers} / {response.text}\n"
            )
        elif response.status_code == 404:
            raise requests.HTTPError(
                "Check flex-template launch API for errors."
                f"{response.headers} / {response.text}\n"
            )
        elif response.status_code != 200:
            response.raise_for_status()
        else:
            return response.text
    else:
        return "Activity value is invalid, Please specify 'start-job'\n"