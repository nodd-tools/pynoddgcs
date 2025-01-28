import subprocess
from google.cloud import storage
import google.auth
from datetime import datetime, timedelta
import base64
import requests
import urllib
import sys
import os
import json


class GCS(object):
    """
    Helper class for uploading and download files from Google Cloud Storage
    """
    def __init__(self):
        self.client = storage.Client.create_anonymous_client()
        self.authenticated = False

    def download(self, bucket, source, destination = None):
        """
        Downloads a file from GCS, handling both public and private buckets.
        
        Parameters
        ----------
        bucket: str
            the bucket name
        source: str
            the file path within the bucket
        destination: str | None
            the file path to which we would like to download the file.
            default to source path within current working directory
        """
        bucket = self.client.bucket(bucket)
        blob = bucket.blob(source)
        if destination is None:
            destination = os.path.join(os.getcwd(), source)
        blob.download_to_filename(destination)

    def authenticate(self):
        """
        Get user's default credentials on this machine.

        Returns nothing, sets the values into object attributes
        """
        # Use default credentials from the gcloud environment
        try:
            credentials, project = google.auth.default()
        except google.auth.DefaultCredentialsError:
            gcloud_login()
            credentials, project = google.auth.default()

        # Use the credentials to create a storage client
        self.authenticated = True
        self.client = storage.Client(credentials=credentials)

    def check_auth(self):
        """
        Authenticates the user if not already authenticated.
        See the `authenticate` method.
        """
        if not self.authenticated:
            self.authenticate()

    def upload(self, bucket, source, destination):
        """
        Uploads a file to GCS
        
        Parameters
        ----------
        bucket: str
            the bucket name
        source: str
            the file path on the local machine
        destination: str
            the file path within the bucket to which we would like 
            to upload the file.
        """
        # Use default credentials from the gcloud environment
        self.check_auth()

        # Upload the file
        bucket = self.client.bucket(bucket)
        blob = bucket.blob(destination)
        blob.upload_from_filename(source)

        print(f"File {source} uploaded to {destination}.")

    def upload_string(self, bucket, contents, destination):
        """
        Uploads a string to GCS as a file
        
        Parameters
        ----------
        bucket: str
            the bucket name
        source: str
            the file path on the local machine
        destination: str
            the file path within the bucket to which we would like 
            to upload the file.
        """
        # Use default credentials from the gcloud environment
        self.check_auth()

        # Upload the file
        bucket = self.client.bucket(bucket)
        blob = bucket.blob(destination)
        print(contents)
        blob.upload_from_string(contents)

        print(f"Contents uploaded to {destination}.")


def gcloud_login():
    """
    Helper method to use the gcloud API to get user credentials
    Requires the gcloud CLI tool to work:
    https://cloud.google.com/sdk/docs/install

    This method should redirect the user to a browser where they can sign in
    to their Google Account and grant permissions to the gcloud CLI

    Once the user is authenticated, we can use access tokens from the gcloud CLI
    to do gcloud stuff on the user's behalf.
    """
    subprocess.run(
        ["gcloud", "auth", "application-default", "login"],
        shell=True
    )

CACHED_ACCESS_TOKEN = None
def get_access_token() -> str:
    """Gets an access token using the gcloud CLI tool
    We can then use this access token to perform gcloud operations
    on behalf of the user.

    We cache the access token for future use, as this process
    may make network requests which are slow.    

    Returns
    -------
    access_token : str
        an access token that can be used to perform google cloud requests.
    """
    if CACHED_ACCESS_TOKEN is None:
        result = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True
        )
        if result.returncode != 0:
            # getting the access token failed, probably because the user
            # isn't logged in.  Log the user in, and then try again.
            gcloud_login()
            input('''
                please complete gcloud login with your browser 
                and then hit the any key to continue
            ''')
            return get_access_token()
        CACHED_ACCESS_TOKEN = result.stdout.strip()
    return CACHED_ACCESS_TOKEN

