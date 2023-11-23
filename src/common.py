# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

# @file    common.py
# @author  Michael Behrisch
# @date    2023-01-03

import argparse
import http.client
import json
import pprint
import sys
import time


class ConnectionProvider:
    def __init__(self, token_file, url):
        with open(token_file, encoding="utf8") as tokens:
            self._connections = [(http.client.HTTPSConnection(url), { 'X-Api-Key': t.strip() })
                                 for t in tokens.readlines() if t.strip()]
        self._index = 0
        self._num_queries = 0

    def request(self, path, method='GET', payload='', retries=0, required=None):
        for _ in range(retries + 1):
            self._num_queries += 1
            time.sleep(1.1 / len(self._connections))
            conn, headers = self._connections[self._index]
            self._index = (self._index + 1) % len(self._connections)
            conn.request(method, path, payload, headers)
            response = json.loads(conn.getresponse().read())
            if response.get("message") == "Too Many Requests":
                print("Warning:", response["message"], file=sys.stderr)
                continue
            if "errorMessage" in response:
                print("Error on %s %s." % (path, payload), response["errorMessage"],
                      response.get("errorType"), response.get("stackTrace"), file=sys.stderr)
            elif required and required not in response:
                print("Format error on %s %s." % (path, payload), file=sys.stderr)
                pprint.pp(response, sys.stderr)
            return response
        return {}

    def print_stats(self):
        print(len(self._connections), "connections", self._num_queries, "queries")


def get_options(args=None):
    parser = argparse.ArgumentParser()
    # Berlin as in https://github.com/DLR-TS/sumo-berlin
    parser.add_argument("-b", "--bbox", default="12.78509,52.17841,13.84308,52.82727",
                        help="bounding box to retrieve in geo coordinates west,south,east,north")
    parser.add_argument("-u", "--url", default="telraam-api.net",
                        help="Download from the given Telraam server")
    parser.add_argument("-t", "--token-file", default="telraam-token.txt",
                        metavar="FILE", help="Read Telraam API token from FILE")
    parser.add_argument("-j", "--json-file", default="sensor-geojson.js",
                        metavar="FILE", help="Write Geo-JSON output to FILE")
    parser.add_argument("--camera", action="store_true", default=False,
                        help="include individual cameras")
    parser.add_argument("-d", "--database", default="backup.db",
                        help="Database output file or URL")
    parser.add_argument("-r", "--retry", type=int, default=1,
                        help="number of retries on failure")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="increase verbosity, twice enables verbose sqlalchemy output")
    return parser.parse_args(args=args)