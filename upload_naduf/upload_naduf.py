# upload_naduf
# This program serves to upload the NADUF dataset from a specific
# file- and directory structure to ERIC, the internal research data
# repository of Eawag.

# Copyright (C) 2020 Harald von Waldow and Eawag

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""
Usage: upload_naduf.py [-t] [-v VERSION]
       upload_naduf.py -h

Options:
    --version, -v VERSION      Version of the upload of the form YYYY-n, where n is an 
                               incrementing index, reset to 1 when a new years starts.
                               When omitted, the latest version found in the base of
                               the staging directory is used.
    --test, -t                 Switches to test mode (Not documented)
    --help, -h                 This help.

"""

from docopt import docopt
import sys
import os
import json
from string import Template
import re
import zipfile
import shutil
import ckanapi
import xml.etree.ElementTree as ET
import hashlib
import time
from mimetypes import guess_type

#import requests
# import codecs

# json-file containing a list of files and/or directories to be uploaded.
# See the file itself for the expected format.
FILELIST = "files.json"

PACKAGE_TEMPLATE = "package_template.json"

#STAGING_BASE = "/cifs/eaw-div/rdm_staging/naduf/"
STAGING_BASE = "/Users/uschoene/desktop/staging"

STATIONS_KML = 'naduf_stations.kml'

# Has to be in source directory
DESCRIPTION = "description.txt"

PKG_BASENAME = "naduf-national-long-term-surveillance-of-swiss-rivers"
PKG_BASETITLE = "NADUF - National long-term surveillance of Swiss rivers"

HOST="https://data.eawag.ch"
APIKEY = os.environ["CKAN_APIKEY_PROD1"]

class Upload:
    def __init__(self, args, staging_base):
        self.staging_base = staging_base
        self.version = self._get_version(args)
        self.is_test = bool(args.get('--test'))
        self.staging = os.path.join(self.staging_base, self.version)
        if not os.path.isdir(self.staging):
            raise Exception('Staging directory "{}" not found'
                            .format(self.staging))

        self.upload = os.path.join(self.staging, 'upload')
        if not os.path.isdir(self.upload):
            raise Exception('Upload directory "{}" not found'
                            .format(self.upload))
        
        self.filelist = self._read_filelist()
        
        self.stations_kml = os.path.join(self.staging, 'sources', STATIONS_KML)
        if not os.path.isfile(self.stations_kml):
            raise Exception('KML-file for stations: "{}" not found'
                            .format(self.stations_kml))
        self.description = os.path.join(self.staging, 'sources', DESCRIPTION)
        if not os.path.isfile(self.description):
            raise Exception('description file: "{}" not found'
                            .format(self.description))

        self.pkg_name = '{}-{}'.format(PKG_BASENAME, self.version)
        self.pkg_basetitle  = '{} ({})'.format(PKG_BASETITLE, self.version)
        self.conn = self._get_conn()
        
    def _get_version(self, args):
        def _latest_version():
            versions = os.listdir(self.staging_base)
            versions = [v for v in versions if  re.match(r"\d{4}", v)]
            versions.sort()
            try:
                return versions[-1]
            except IndexError:
                raise Exception('No suitable input-directories found below {}'
                                .format(self.staging_base))

        version = args.get('-v') or _latest_version()
        return version

    def _read_filelist(self):
        return json.load(open(os.path.join(self.staging, FILELIST), 'r'))

    def _read_template(self):
        with open(os.path.join(self.staging, PACKAGE_TEMPLATE), 'r') as fp:
            template = json.load(fp)
        template = self._modify_testmode(template)
        return(template)

    def _modify_testmode(self, template):
        if self.is_test:
            # Change to test-target in repository
            template["owner_org"] = "research-data-management"
            template["private"] = True
        else:
            pass
        return template

    def _get_conn(self):
        return ckanapi.RemoteCKAN(HOST, apikey=APIKEY)

    def _chksum(self, filenam):
        hash_sha = hashlib.sha256()
        print ("\nCalculating checksum for {} ...".format(filenam))
        t0 = time.time()
        with open(filenam, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hash_sha.update(chunk)
            digest = hash_sha.hexdigest()
            deltat = time.time() - t0
            print('\ttime: {} seconds'.format(deltat))
            print('\t{}: {}'.format('sha256', digest))
        return digest
    
    def transform_files(self):

        """performs actions on local files or folders before copying to upload.
        Currently implemented actions: 'zip' and 'copy'

        """ 
        locfiles =  [f for f in self.filelist
                     if re.match("file://", f.get('url') or "")]
        for f in locfiles:
            sourcepath = Template(f['url'][7:]).substitute({'STAGING': self.staging})
            targetpath = os.path.join(self.upload, f['name'])
            if f['action'] == 'zip':
                with zipfile.ZipFile(targetpath, 'w') as zipf:
                    print("Creating zip-file {}".format(targetpath))
                    if os.path.isdir(sourcepath):
                        for root, dirs, files in os.walk(sourcepath):
                            for fn in files:
                                arcname = os.path.join(os.path.split(root)[1], fn)
                                print("\tadding to zip-file: {}".format(os.path.join(root, fn)))
                                zipf.write(os.path.join(root, fn), arcname)
                    elif os.path.isfile(sourcepath):
                        print("\tadding to zip-file: {}".format(sourcepath))
                        zipf.write(sourcepath, os.path.basename(targetpath))
                    else:
                        raise Exception('"{}" is neither path nor directory.')
            elif f['action'] == 'copy':
                print('copy file {} -> {}'.format(sourcepath, targetpath))
                shutil.copyfile(sourcepath, targetpath)

    def get_locations(self):
        locations = ''
        tree = ET.parse(self.stations_kml)
        root = tree.getroot()
        ns = '{http://www.opengis.net/kml/2.2}'
        test = './/'+ns+'Placemark//'+ns+'name'
        for child in root.findall(test):
            text = child.text
            locations += '{}\n'.format(text)
        return(locations)

    def get_coordinates(self):
        """Extracts GeoJSON for station-locations from KML"""
        coordinates = []
        tree = ET.parse(self.stations_kml)
        root = tree.getroot()
        # get namespace-url
        ns = re.match(r'({.*}).*', root.tag).group(1)

        for point in root.iter(ns + 'Point'):
            coordinates.extend(
                [coord.text for coord in point.findall(ns + 'coordinates')])
        coordinates = [coord.split(',')[0:2] for coord in coordinates]
        coordinates = [[float(c) for c in point] for point in coordinates]
        geojson = {'type': "MultiPoint",
                   'coordinates': coordinates}
        geojson = json.dumps(geojson)
        return(geojson)

    def create_package(self):
        pkg = self._read_template()
        pkg['timerange'] = open(
            os.path.join(self.staging, "TIMERANGE"), 'r').readline().strip()
        pkg['geographic_name'] = self.get_locations()
        pkg['spatial'] = self.get_coordinates()
        desc = open(self.description).readlines()
        pkg['notes'] = ''.join(desc)
        pkg['name'] = self.pkg_name
        pkg['title'] = self.pkg_basetitle
        return pkg

    def upload_package(self, pkg):
        print("\nUploading package ...")
        try:
            self.conn.call_action('package_create', data_dict=pkg)
        except  ckanapi.ValidationError:
            self.conn.call_action('package_update', data_dict=pkg)

    def mk_resource_meta(self):
        for f in self.filelist:
            meta = {}
            filepath = os.path.join(self.upload, f['name'])
            if not os.path.isfile(filepath):
                raise Exception("resource {} doesn't exist".format(filepath))
            meta['size'] = os.stat(filepath).st_size
            meta['hash'] = self._chksum(filepath)
            meta['hashtype'] = 'sha256'
            meta['mimetype'] = guess_type(filepath, strict=False)[0]
            meta['restricted_level'] = 'public'
            meta['resource_type'] = f['resource_type']
            meta['name'] = f['name']
            yield (filepath, meta)

    def upload_resources(self):
        for meta in self.mk_resource_meta():
            source = meta[0]
            filename = os.path.split(source)[1]
            meta = meta[1]
            meta['package_id'] = self.pkg_name
            if filename:
                print("\nuploading {} ({})".format(meta['name'], meta['size']))
                time0 = time.time()
                self.conn.call_action(
                    'resource_create', meta,
                    files={'upload': open(source, 'rb')},
                    requests_kwargs={'verify': False})
                print('\ttotal time: {} s'.format(time.time()-time0))
            elif meta.get('link'):
                meta['url'] = meta['link']
                del meta['link']
                self.conn.call_action('resource_create', meta)
                print("\ncreated {}".format(meta['name']))
            else:
                raise Exception("Can't create resource, something is wrong"
                                "with the record:\n"
                                "{}".format(meta))

def main():
    args = docopt(__doc__)
    u = Upload(args, STAGING_BASE)
    u.transform_files()
    pkg = u.create_package()
    u.upload_package(pkg)
    u.upload_resources()


if __name__ == "__main__":
    main()
