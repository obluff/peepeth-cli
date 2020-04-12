from web3 import Web3
import json
import requests
import pandas as pd 

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
                print("WOAH")
                print(data.keys())
                continue
            data_to_parse = self.get_data_from_ipfs(data['_ipfsHash'])
            res.append(data_to_parse)
       # print(res)
        return res

            
            ##self.dfs(events)
    def decode_function_input(self, data):
        return self.peep_contract.decode_function_input(data)
    
    @staticmethod
    def parsed_signed_actions(ipfs_data):
        new_res = []
        for item in ipfs_data['signedActions']:
            for action, dta in item.items():
                if 'ipfs' not in dta:
                    continue
                new = dta
                new['type'] = action
                new_res.append(new)
        return new_res
        
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
        latest_block_number = self.w3.eth.getBlock("latest")["number"]
        return self.peep_contract.events \
                                 .PeepethEvent() \
                                 .getLogs(fromBlock=latest_block_number - 10000,    
                                          toBlock="latest")        
    
    