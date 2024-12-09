import subprocess

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
        ["gcloud", "auth", "login"],
        shell=True
    )

def get_access_token() -> str:
    """Gets an access token using the gcloud CLI tool
    We can then use this access token to perform gcloud operations
    on behalf of the user.

    Returns
    -------
    access_token : str
        an access token that can be used to perform google cloud requests.
    """
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
    return result.stdout.strip()
