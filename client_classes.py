
class Client:
    """
    Root class for Client objects.
    Defines what variables should be used for comparing clients from different sources.
    """
    comparison_variables = ["name"]

    def __eq__(self, other):
        matching_fields = [str(self.__getattribute__(variable)).lower() == str(other.__getattribute__(variable)).lower()
                           for variable in self.comparison_variables]
        return all(matching_fields)


class NsightClient(Client):
    """
    Class for N-sight client objects.
    Initiates from N-sight API Client xml.
    Can output json payload for Halo Client post request.
    """
    toplevel_id = ""
    halo_colour = "#a75ded"         # N-able purple to quickly distinguish Clients synced from N-sight

    def __init__(self, client_xml):
        self.nsight_id = client_xml.find("./clientid").text
        self.name = client_xml.find("./name").text

    def __repr__(self):
        return f"{self.name} (N-sight id: {self.nsight_id})"

    def get_post_payload(self):
        payload = {
            "name": self.name,
            "toplevel_id": str(self.toplevel_id),
            "colour": self.halo_colour}
        return payload


class HaloClient(Client):
    """
    Class for Halo client objects.
    Initiates from Halo API Client json.
    """
    def __init__(self, client):
        self.halo_id = client["id"]
        self.name = client["name"]
        self.toplevel_id = client["toplevel_id"]

    def __repr__(self):
        return f"{self.name} (Halo id: {self.halo_id})"


class HaloToplevel(Client):
    """
    Class for Halo toplevel objects. (One level above Clients.)
    Initiates from Halo API Toplevel json.
    """
    def __init__(self, toplevel):
        self.toplevel_id = toplevel["id"]
        self.name = toplevel["name"]

    def __repr__(self):
        return f"{self.name} (Toplevel id: {self.toplevel_id})"
