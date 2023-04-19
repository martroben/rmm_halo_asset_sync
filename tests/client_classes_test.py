import client_classes


def test_equality():
    nsight_client = client_classes.NsightClient({"name": "Test client", "nsight_id": 111})
    halo_client = client_classes.HaloClient({"name": "Test client", "id": 222, "toplevel_id": 333})
    assert nsight_client == halo_client


def test_add_comparison_variable():
    client_classes.Client.comparison_variables += ["toplevel_id"]
    nsight_client = client_classes.NsightClient({"name": "Test client", "nsight_id": 111})
    nsight_client.toplevel_id = 333
    halo_client = client_classes.HaloClient({"name": "Test client", "id": 222, "toplevel_id": 333})
    assert nsight_client == halo_client


def test_initialize_toplevel():
    halo_toplevel = client_classes.HaloToplevel({"name": "Test toplevel", "id": 333})
    assert halo_toplevel.name == "Test toplevel"
    assert halo_toplevel.toplevel_id == 333
