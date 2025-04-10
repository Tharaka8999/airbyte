#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


import requests
from abc import ABC
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping, MutableMapping, Optional
import requests
import math

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class FacebookCustomStream(HttpStream, ABC):
    url_base = "https://graph.facebook.com/v22.0/"  # Using latest stable version
    extra_params = None
    has_date_param = False

    def __init__(self, access_token: str, date_from: Optional[str], account_id: str, **kwargs):
        super().__init__(authenticator=access_token)
        self.access_token = access_token
        self.date_from = date_from or datetime.today().strftime("%Y-%m-%d")
        self.account_id = account_id

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        # Facebook uses cursors for pagination
        paging = decoded_response.get("paging", {})
        next_page_url = paging.get("next")

        if next_page_url:
            # Extract the 'after' cursor from the next page URL
            return {"after": paging.get("cursors", {}).get("after")}
        return None

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "access_token": self.access_token,
        }

        if self.has_date_param:
            params.update({
                "date_preset": "maximum",
                # "time_range": {
                #     "since": self.date_from,
                #     "until": datetime.today().strftime("%Y-%m-%d")
                # }
            })

        if self.extra_params:
            params.update(self.extra_params)

        if next_page_token:
            params.update(**next_page_token)

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield response_json


class IncrementalFacebookCustomStream(FacebookCustomStream, ABC):
    state_checkpoint_interval = 1000
    cursor_field = "updated_time"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        if not latest_record:
            return current_stream_state or {}

        latest_record_date = latest_record.get(self.cursor_field)
        if not latest_record_date:
            return current_stream_state or {}

        current_state_date = current_stream_state.get(self.cursor_field, "")

        # Convert string dates to datetime for comparison
        latest_record_dt = datetime.strptime(latest_record_date, "%Y-%m-%dT%H:%M:%S%z")
        current_state_dt = datetime.strptime(current_state_date, "%Y-%m-%dT%H:%M:%S%z") if current_state_date else datetime.min.replace(
            tzinfo=timezone.utc)

        # Use the more recent date
        max_date = max(latest_record_dt, current_state_dt)

        return {self.cursor_field: max_date.strftime("%Y-%m-%dT%H:%M:%S%z")}

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)

        if stream_state and self.cursor_field in stream_state:
            params["filtering"] = [{
                "field": self.cursor_field,
                "operator": "GREATER_THAN",
                "value": stream_state[self.cursor_field]
            }]

        return params


class Campaigns(FacebookCustomStream):
    """
    API Doc: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign
    """
    primary_key = "id"
    has_date_param = True
    extra_params = {
        "fields": [
            'id',
            'name',
            'status',
            'effective_status',
            'objective',
            'buying_type',
            'daily_budget',
            'budget_remaining',
            'configured_status',
            'bid_strategy',
            'start_time',
            'created_time',
            'updated_time'
        ]
    }
    use_cache = True

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"act_{self.account_id}/campaigns"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("data", [])


# Basic incremental stream
# class IncrementalFacebookCustomStream(FacebookCustomStream, ABC):
#     """
#     TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
#          if you do not need to implement incremental sync for any streams, remove this class.
#     """
#
#     # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
#     state_checkpoint_interval = None
#
#     @property
#     def cursor_field(self) -> str:
#         """
#         TODO
#         Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
#         usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.
#
#         :return str: The name of the cursor field.
#         """
#         return []
#
#     def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
#         """
#         Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
#         the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
#         """
#         return {}


class Employees(IncrementalFacebookCustomStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    cursor_field = "start_date"

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "employee_id"

    def path(self, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return "employees"

    # def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
    #     """
    #     TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.
    #
    #     Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
    #     This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
    #     section of the docs for more information.
    #
    #     The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
    #     necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
    #     This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.
    #
    #     An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
    #     craft that specific request.
    #
    #     For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
    #     this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
    #     till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
    #     the date query param.
    #     """
    #     raise NotImplementedError("Implement stream slices or delete this method!")


# Source
class SourceFacebookCustom(AbstractSource):

    @staticmethod
    def get_authenticator(config: Mapping[str, Any]):
        """Extract access token from config and create an authenticator."""
        # Try to get token from nested credentials first, then direct access_token
        token = config.get("credentials", {}).get("access_token")
        if not token:
            token = config.get("access_token")
        return TokenAuthenticator(token=token)

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Verify the connection to Facebook Graph API by making a test call.
        Returns a tuple of (success: bool, error_message: str).
        """
        try:
            authenticator = self.get_authenticator(config)
            # Make a test call to Facebook Graph API's /me endpoint
            response = requests.get(
                url="https://graph.facebook.com/v22.0/me",  # Using latest stable version
                headers=authenticator.get_auth_header(),
                params={"fields": "id,name"}  # Basic fields to verify access
            )
            response.raise_for_status()

            # Check if we got valid response
            user_data = response.json()
            if "id" in user_data:
                return True, None
            return False, "Unable to verify Facebook Graph API access"

        except requests.exceptions.RequestException as e:
            return False, f"Connection test failed: {repr(e)}"
        except Exception as e:
            return False, f"Unexpected error during connection test: {repr(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        access_token = config.get("credentials", {}).get("access_token")
        account_id = config.get("credentials", {}).get("account_id")
        auth = TokenAuthenticator(token=access_token)  # Oauth2Authenticator is also available if you need oauth support
        return [Campaigns(authenticator=auth, date_from="2022-01-01", account_id=account_id, access_token=access_token), ]
