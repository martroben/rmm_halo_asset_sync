
# standard
import pytest
import re
# external
import responses as mock_responses
# local
import halo_requests


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
