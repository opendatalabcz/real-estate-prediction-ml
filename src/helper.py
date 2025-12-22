import time
import random
import json
import logging
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Any
from tqdm import tqdm
import os
import traceback
import re

category_type_to_url = {
    0: "vse", 1: "prodej", 2: "pronajem", 3: "drazby"
}

category_main_to_url = {
    0: "vse", 1: "byt", 2: "dum", 3: "pozemek", 4: "komercni", 5: "ostatni"
}

category_sub_to_url = {
    2: "1+kk", 3: "1+1", 4: "2+kk", 5: "2+1", 6: "3+kk", 7: "3+1", 8: "4+kk",
    9: "4+1", 10: "5+kk", 11: "5+1", 12: "6-a-vice", 16: "atypicky", 47: "pokoj",
    37: "rodinny", 39: "vila", 43: "chalupa", 33: "chata", 35: "pamatka",
    40: "na-klic", 44: "zemedelska-usedlost", 19: "bydleni", 18: "komercni",
    20: "pole", 22: "louka", 21: "les", 46: "rybnik", 48: "sady-vinice",
    23: "zahrada", 24: "ostatni-pozemky", 25: "kancelare", 26: "sklad",
    27: "vyrobni-prostor", 28: "obchodni-prostor", 29: "ubytovani",
    30: "restaurace", 31: "zemedelsky", 38: "cinzovni-dum", 49: "virtualni-kancelar",
    32: "ostatni-komercni-prostory", 34: "garaz", 52: "garazove-stani",
    50: "vinny-sklep", 51: "pudni-prostor", 53: "mobilni-domek", 36: "jine-nemovitosti"
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
