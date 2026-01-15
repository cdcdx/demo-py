# nftmint ABI
contract_abi_nftmint = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "buyer", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "superior", "type": "address"},
            {"indexed": False, "internalType": "uint8", "name": "boxId", "type": "uint8"},
            {"indexed": False, "internalType": "uint256", "name": "ethAmount", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "sxpAmount", "type": "uint256"},
            {"indexed": False, "internalType": "uint256[]", "name": "tokenIds", "type": "uint256[]"}
        ],
        "name": "NFTPurchased",
        "type": "event"
    },
    {
        "inputs": [
            { "internalType": "address", "name": "", "type": "address" }
        ],
        "name": "userPurchases",
        "outputs": [
            { "internalType": "uint256", "name": "", "type": "uint256" }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {"internalType": "address","name": "buyer","type": "address"},
            {"internalType": "address","name": "superior","type": "address"}
        ],
        "name": "genesis",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function"
    }
]