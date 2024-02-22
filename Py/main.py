import json


def main():
    endpoint_request_header =get_credentials_details()
    print(endpoint_request_header)

def get_credentials_details():
    with open("connexion/belgium.json") as cred:
            credentials = json.loads(cred)
    return credentials


#def get_endpoint_response(**kwargs):
#        status_code = 0
#        while status_code != 200:
#                endpoint_request_response = requests.get(kwargs["endpoint_url"], headers=["endpoint_request_header"],params=["endpoint_request_parameters"])
#                status_code = endpoint_request_response.status_code
#        return endpoint_request_response.json()
