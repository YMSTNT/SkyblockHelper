#!/bin/sh

pip install -r requirements.txt
cd data
echo 'Enter your Hypixel API key:'
read api_key
echo $api_key > api_key.txt
git clone https://github.com/Moulberry/NotEnoughUpdates-REPO
