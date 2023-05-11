# standard
import re
# local
import nsight_requests
# external
import responses as mock_responses


def test_get_clients():
    mock_responses.start()
    mock_responses.add(
        method=mock_responses.GET,
        url=re.compile(r"https://mockurl_nableapi.com"),
        body="mock xml",
        status=200)

    response = nsight_requests.get_clients(
        url="https://mockurl_nableapi.com",
        api_key="mock_api_key")

    assert response.text == "mock xml"
    mock_responses.stop()


def test_get_clients_fail():
    response = nsight_requests.get_clients(
        url="https://mockurl_nableapi.com",
        api_key="mock_api_key")

    assert response is None


def test_parse_clients():
    nsight_id1 = "111111"
    name1 = "Mock client 1"
    nsight_id2 = "222222"
    name2 = "Mock client 2"
    nsight_client_mock_xml = f'<?xml version="1.0" encoding="ISO-8859-1"?>\n<result created="2021-04-11T12:24:35+08:00" host="mockapi.nable.com" status="OK"><items>\n<client><name><![CDATA[{name1}]]></name><clientid>{nsight_id1}</clientid><view_dashboard>1</view_dashboard><view_wkstsn_assets>1</view_wkstsn_assets><dashboard_username><![CDATA[admin@mockclient1.com]]></dashboard_username><timezone><![CDATA[USA]]></timezone><creation_date/><server_count>10</server_count><workstation_count>20</workstation_count><mobile_device_count>30</mobile_device_count><device_count>60</device_count></client><client><name><![CDATA[{name2}]]></name><clientid>{nsight_id2}</clientid><view_dashboard>0</view_dashboard><view_wkstsn_assets>0</view_wkstsn_assets><dashboard_username><![CDATA[none]]></dashboard_username><timezone/><creation_date>2020-08-31</creation_date><server_count>1</server_count><workstation_count>2</workstation_count><mobile_device_count>3</mobile_device_count><device_count>6</device_count></client></items></result>\n'

    mock_responses.start()
    mock_responses.add(
        method=mock_responses.GET,
        url=re.compile(r"https://mockapi.nable.com"),
        body=nsight_client_mock_xml,
        status=200)

    response = nsight_requests.get_clients(
        url="https://mockapi.nable.com",
        api_key="mock_api_key")

    nsight_client_data = nsight_requests.parse_clients(response)
    assert {"name": name1, "nsight_id": nsight_id1} in nsight_client_data
    assert {"name": name2, "nsight_id": nsight_id2} in nsight_client_data


def test_parse_clients_empty():
    nsight_client_mock_xml = f'<?xml version="1.0" encoding="ISO-8859-1"?>\n<result created="2021-04-11T12:24:35+08:00" host="mockapi.nable.com" status="OK"><items></items></result>\n'

    mock_responses.start()
    mock_responses.add(
        method=mock_responses.GET,
        url=re.compile(r"https://mockapi.nable.com"),
        body=nsight_client_mock_xml,
        status=200)

    response = nsight_requests.get_clients(
        url="https://mockapi.nable.com",
        api_key="mock_api_key")

    nsight_client_data = nsight_requests.parse_clients(response)
    assert isinstance(nsight_client_data, list)
    assert len(nsight_client_data) == 0
