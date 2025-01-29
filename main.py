import requests
import json
import time
from bs4 import BeautifulSoup

api_url = "https://app1bhunakshaodisha.nic.in/bhunaksha/ScalarDatahandler"
base_params = {"OP": "5", "state": "21"}

def find_districts():
    api = "https://app1bhunakshaodisha.nic.in/bhunaksha/"
    response = requests.get(api)
    soup = BeautifulSoup(response.text, 'html.parser')
    select_tag = soup.find("select", {"id": "level_1"})
    districts = {option["value"]: option.text for option in select_tag.find_all("option")}
    print(districts)
    return districts

def find_tehsils(selections):
    api = "https://app1bhunakshaodisha.nic.in/bhunaksha/ScalarDatahandler"
    params = {
        "OP":"2",
        "level":"2",
        "selections":selections,
        "state":"21"
    }
    response = requests.get(api,params)
    soup = BeautifulSoup(response.text, 'html.parser')
    select_tag = soup.find("select", {"id": "level_2"})
    tehsils = {option["value"]: option.text for option in select_tag.find_all("option")}
    print(tehsils)
    return tehsils

def find_RI(district_no, tehsil_no):
    api = "https://app1bhunakshaodisha.nic.in/bhunaksha/ScalarDatahandler"
    district_no = str(district_no)
    tehsil_no = str(tehsil_no)
    params = {
        "OP":"2",
        "level":"3",
        "selections":district_no + "," + tehsil_no,
        "state":"21"
    }
    response = requests.get(api,params)
    soup = BeautifulSoup(response.text, 'html.parser')
    select_tag = soup.find("select", {"id": "level_3"})
    RIs = {option["value"]: option.text for option in select_tag.find_all("option")}
    print(RIs)
    return RIs

def find_villages(district_no, tehsil_no, RI_no):
    api = "https://app1bhunakshaodisha.nic.in/bhunaksha/ScalarDatahandler"
    district_no = str(district_no)
    tehsil_no = str(tehsil_no)
    RI_no = str(RI_no)
    
    params = {
        "OP":"2",
        "level":"4",
        "selections":district_no + "," + tehsil_no + "," + RI_no,
        "state":"21"
    }
    response = requests.get(api,params)
    soup = BeautifulSoup(response.text, 'html.parser')
    select_tag = soup.find("select", {"id": "level_4"})
    villages = {option["value"]: option.text for option in select_tag.find_all("option")}
    print(villages)
    return villages

districts = find_districts()

for district in districts:
    for tehsil in find_tehsils(district):
        for RI in find_RI(district, tehsil):
            find_villages(district, tehsil, RI)
            