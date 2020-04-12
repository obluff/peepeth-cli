from web3 import Web3
import json
import requests
import pandas as pd 
from functools import reduce

class PeepethClient:
    def __init__(self, 
                 web3url,
                 contract_adr,
                 contract_abi_path,
                 blocks_back = 50):
        
        self.w3 = Web3(Web3.HTTPProvider(web3url))
        self.contract_abi = self.load_json(contract_abi_path)
        self.peep_contract = self.w3.eth.contract(address=contract_adr,
                                                  abi=self.contract_abi) 
        
    
    def refresh(self):
        event_df = None
        events = self.get_historical_events()
        
        res = []
        for tx in events:
            data = self.get_input_data_from_transaction(tx)
            function, data = self.decode_function_input(data)
            if function == "saveBatch":
#                print("WOAH")
#                print(data.keys())
                continue
            data_to_parse = self.get_data_from_ipfs(data['_ipfsHash'])
            end_res = self.parse_signed_actions(data_to_parse['signedActions'])
            res.append(end_res)
                            
       # print(res)
        return reduce(lambda x, y: x + y, res)

            
            ##self.dfs(events)
    def decode_function_input(self, data):
        return self.peep_contract.decode_function_input(data)
    
    def parse_signed_actions(self, signed_actions):
        """recursively parses the signed actions
           
           dfs i think? lmao
        
           need to refactor this"""
        
        if signed_actions == []:
            return [] 
        
        if type(signed_actions) == dict:
            if "signedActions" in signed_actions:
                return self.parse_signed_actions(signed_actions['signedActions'])
            else:
                return [signed_actions]
        
        action, data = list(signed_actions[0].items())[0]
        if "ipfs" in data:
            new_url = self.get_data_from_ipfs(data['ipfs'])
            return self.parse_signed_actions(new_url) \
                   + self.parse_signed_actions(signed_actions[1:])
        else:
            return [data] + self.parse_signed_actions(signed_actions[1:])
         
    @staticmethod    
    def load_json(path):
        with open(path, "r") as r:
            loaded_json = json.loads(r.read())
        r.close()
        return loaded_json
    
    @staticmethod
    def get_data_from_ipfs(ipfs_hash):
        url_to_request = f"https://ipfs.io/ipfs/{ipfs_hash}"
        return requests.get(url_to_request) \
                       .json()
        
    
    def get_input_data_from_transaction(self, tx):
        return self.w3.eth.getTransaction(tx['transactionHash'])['input']
    
    def get_historical_events(self):
        """gonna remove this hardcoding after i get it """
        latest_block_number = self.w3.eth.getBlock("latest")["number"]
        return self.peep_contract.events \
                                 .PeepethEvent() \
                                 .getLogs(fromBlock=latest_block_number - 10000,    
                                          toBlock="latest")        
    
    