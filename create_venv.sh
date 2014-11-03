#!/bin/bash
virtualenv venv
source venv/bin/activate
pip install beautifulsoup4 requests matplotlib redis
read -p "We will now install components for ipython notebook. If you do not want them press ctrl-c now"
pip install ipython tornado pyzmq jinja2
