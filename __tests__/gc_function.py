# pylint: skip-file
"""Registered passthrough function used in CI."""
from globus_compute_sdk import Client
from globus_compute_sdk.serialize import JSONData


def passthrough(*args, **kwargs):
    return kwargs


gcc = Client(data_serialization_strategy=JSONData())
function_uuid = gcc.register_function(passthrough, public=True)
print(f"Registered passthrough function UUID: {function_uuid}")
