
# standard
import json
import pytest
import re
# external
import requests
import responses as mock_responses
# local
import halo_requests


########################
# HaloAuthorizer tests #
########################

def test_halo_authorizer():
    mock_responses.start()
    mock_responses.add(
        method=mock_responses.POST,
        url=re.compile(r".*?mockurl_auth.*?"),
        json={"token_type": "mock_token_type", "access_token": "MOCKTOKENSTRING12345", "expires_in": 111},
        status=201)

    halo_authorizer = halo_requests.HaloAuthorizer(
        url="https://mockurl_auth.com",
        tenant="mock_tenant",
        client_id="mock_id",
        secret="mock_secret")

    halo_client_token = halo_authorizer.get_token(scope="edit:customers")
    assert "token_type" in halo_client_token and halo_client_token["token_type"]
    assert "access_token" in halo_client_token and halo_client_token["access_token"]

    mock_responses.stop()


def test_halo_authorizer_fail():
    mock_responses.start()
    mock_responses.add(
        method=mock_responses.POST,
        url=re.compile(r".*?mockurl_auth_fail.*?"),
        json={},
        status=400)
    mock_responses.add(
        method=mock_responses.POST,
        url=re.compile(r".*?mockurl_auth_fail.*?"),
        json={},
        status=400)
    mock_responses.add(
        method=mock_responses.POST,
        url=re.compile(r".*?mockurl_auth_fail.*?"),
        json={},
        status=400)

    halo_authorizer = halo_requests.HaloAuthorizer(
        url="https://mockurl_auth_fail.com",
        tenant="mock_tenant",
        client_id="mock_id",
        secret="mock_secret")

    with pytest.raises(ConnectionError):
        halo_authorizer.get_token(scope="edit:customers")

    mock_responses.stop()


# #######################
# # HaloInterface tests #
# #######################

api_session = halo_requests.HaloSession(token="MOCKTOKENSTRING12345")


def test_api_get():
    api_interface_get = halo_requests.HaloInterface(
        url="https://mockurl_get.com",
        endpoint="mock_endpoint",
        fatal_fail=False)

    mock_responses.start()
    mock_responses.add(
        method=mock_responses.GET,
        url=re.compile(r".*?mockurl_get.*?"),
        body='{"record_count": 10, "data": {"page": 1}}',
        status=200)
    mock_responses.add(
        method=mock_responses.GET,
        url=re.compile(r".*?mockurl_get.*?"),
        body='{"record_count": 10, "data": {"page": 2}}',
        status=200)
    mock_responses.add(
        method=mock_responses.GET,
        url=re.compile(r".*?mockurl_get.*?"),
        body='{"record_count": 0}',
        status=200)

    paginated_responses = api_interface_get.get(
        session=api_session,
        parameters={"parameter1": 1, "parameter2": 2})

    assert len(paginated_responses) == 2
    assert paginated_responses[0].json()["data"]["page"] == 1
    assert paginated_responses[1].json()["data"]["page"] == 2

    mock_responses.stop()


def test_api_get_fail_nonfatal():
    api_interface_nonfatal = halo_requests.HaloInterface(
        url="https://mockurl_get_fail_nonfatal.com",
        endpoint="mock_endpoint",
        fatal_fail=False)

    api_interface_nonfatal.get(
        session=api_session,
        parameters={"parameter1": 1, "parameter2": 2})


def test_api_get_fail_fatal():
    api_interface_fatal = halo_requests.HaloInterface(
        url="https://mockurl_get_fail_fatal.com",
        endpoint="mock_endpoint",
        fatal_fail=True)

    with pytest.raises(requests.exceptions.ConnectionError):
        api_interface_fatal.get(
            session=api_session,
            parameters={"parameter1": 1, "parameter2": 2})


def test_api_post():
    api_interface_post = halo_requests.HaloInterface(
        url="https://mockurl_post.com",
        endpoint="mock_endpoint",
        fatal_fail=True)

    mock_responses.start()
    mock_responses.add(
        method=mock_responses.POST,
        url=re.compile(r".*?mockurl_post.*?"),
        status=201)

    api_interface_post.post(
        session=api_session,
        json={"value1": 1, "value2": 2})

    mock_responses.stop()


def test_api_post_fail():
    api_interface_post_fail = halo_requests.HaloInterface(
        url="https://mockurl_post_fail.com",
        endpoint="mock_endpoint",
        fatal_fail=True)

    mock_responses.start()
    mock_responses.add(
        method=mock_responses.POST,
        url=re.compile(r".*?mockurl_post_fail.*?"),
        status=400)

    mock_responses.add(
        method=mock_responses.POST,
        url=re.compile(r".*?mockurl_post_fail.*?"),
        status=400)

    mock_responses.add(
        method=mock_responses.POST,
        url=re.compile(r".*?mockurl_post_fail.*?"),
        status=400)

    with pytest.raises(ConnectionError):
        api_interface_post_fail.post(
            session=api_session,
            json={"value1": 1, "value2": 2})

    mock_responses.stop()


def test_api_mock_post():
    api_interface_mock_post = halo_requests.HaloInterface(
        url="https://mockurl_mock_post.com",
        endpoint="mock_endpoint",
        fatal_fail=True)

    mock_response = api_interface_mock_post.mock_post(
        1, "text2", True,
        int_kwarg=1,
        text_kwarg="text2",
        bool_kwarg=False)

    mock_response.json()
    assert json.loads(mock_response.json()["args"]) == [1, "text2", True]
    assert mock_response.json()["int_kwarg"] == 1
    assert mock_response.json()["text_kwarg"] == "text2"
    assert mock_response.json()["bool_kwarg"] is False


#######################
# parse_clients tests #
#######################

def test_parse_client():
    mock_client1 = {
        "id": 1,
        "name": "Mock client1",
        "toplevel_id": 1,
        "toplevel_name": "Mock toplevel1",
        "inactive": False,
        "colour": "#E27300",
        "use": "client",
        "client_to_invoice": 0,
        "stopped": 0,
        "customertype": 0,
        "ref": "",
        "ticket_invoices_for_each_site": False}

    mock_client2 = {
        "id": 2,
        "name": "Mock client2",
        "toplevel_id": 1,
        "toplevel_name": "Mock toplevel1",
        "inactive": False,
        "colour": "#E27300",
        "use": "client",
        "client_to_invoice": 0,
        "stopped": 0,
        "customertype": 0,
        "ref": "",
        "ticket_invoices_for_each_site": False}

    api_interface_mock_clients = halo_requests.HaloInterface(
        url="https://mockurl_mock_clients.com",
        endpoint="mock_endpoint",
        fatal_fail=True)

    response1 = api_interface_mock_clients.mock_post(clients=[mock_client1])
    response2 = api_interface_mock_clients.mock_post(clients=[mock_client2])

    mock_input = [response1, response2]
    parsed_clients = halo_requests.parse_clients(mock_input)
    assert parsed_clients[0] == mock_client1
    assert parsed_clients[1] == mock_client2


#########################
# parse_toplevels tests #
#########################

def test_parse_toplevel():

    mock_toplevel1 = {
        "id": 1,
        "guid": "b817d2fe-8da4-dc97-dfe3-0c6a5bf6c31b",
        "name": "Mock toplevel1",
        "organisation_id": 1,
        "organisation_name": "Mock Org Group",
        "long_name": "Mock Org Group - Mock toplevel1",
        "type": 0}

    mock_toplevel2 = {
        "id": 2,
        "guid": "39356d7a-ff54-0fef-f193-101e2d22fd8b",
        "name": "Mock toplevel1",
        "organisation_id": 1,
        "organisation_name": "Mock Org Group",
        "long_name": "Mock Org Group - Mock toplevel1",
        "type": 0}

    api_interface_mock_toplevels = halo_requests.HaloInterface(
        url="https://mockurl_mock_toplevels.com",
        endpoint="mock_endpoint",
        fatal_fail=True)

    response1 = api_interface_mock_toplevels.mock_post(tree=[mock_toplevel1])
    response2 = api_interface_mock_toplevels.mock_post(tree=[mock_toplevel2])

    mock_input = [response1, response2]
    parsed_toplevels = halo_requests.parse_toplevels(mock_input)
    assert parsed_toplevels[0] == mock_toplevel1
    assert parsed_toplevels[1] == mock_toplevel2
