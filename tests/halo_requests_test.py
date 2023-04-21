
# standard
import pytest
import re
# external
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
        url=re.compile(r".*"),
        json={"token_type": "mock_token_type", "access_token": "MOCKTOKENSTRING12345", "expires_in": 111},
        status=201)

    halo_authorizer = halo_requests.HaloAuthorizer(
        url="https://mockurl.com",
        tenant="mock_tenant",
        client_id="mock_id",
        secret="mock_secret")

    halo_client_token = halo_authorizer.get_token(scope="edit:customers")
    assert "token_type" in halo_client_token and halo_client_token["token_type"]
    assert "access_token" in halo_client_token and halo_client_token["access_token"]


def test_halo_authorizer_fail():
    mock_responses.start()
    mock_responses.add(
        method=mock_responses.POST,
        url=re.compile(r".*"),
        json={},
        status=400)
    mock_responses.add(
        method=mock_responses.POST,
        url=re.compile(r".*"),
        json={},
        status=400)
    mock_responses.add(
        method=mock_responses.POST,
        url=re.compile(r".*"),
        json={},
        status=400)

    halo_authorizer = halo_requests.HaloAuthorizer(
        url="https://mockurl.com",
        tenant="mock_tenant",
        client_id="mock_id",
        secret="mock_secret")

    with pytest.raises(ConnectionError):
        halo_client_token = halo_authorizer.get_token(scope="edit:customers")


#######################
# HaloInterface tests #
#######################

api_interface = halo_requests.HaloInterface(
    url="https://mockurl.com",
    endpoint="mock_endpoint")

api_session = halo_requests.HaloSession(token="MOCKTOKENSTRING12345")


def test_api_get():
    mock_responses.start()
    mock_responses.add(
        method=mock_responses.GET,
        url=re.compile(r".*"),
        json={"record_count": "10"},
        status=200)
    mock_responses.add(
        method=mock_responses.GET,
        url=re.compile(r".*"),
        json={"record_count": "0"},
        status=200)

    ########################## Continue here: paginated response
    paginated_responses = api_interface.get(
        session=api_session,
        parameters={"parameter1": 1, "parameter2": 2})

