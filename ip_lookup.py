import requests

def lookup_ip(ip_address):
    ip_lookup_url="http://ipwho.is/"+ip_address

    response=requests.get(ip_lookup_url)

    if response.status_code==200:
        return response.json()
    else:
        return {'ip_lookup_error':response.status_code}