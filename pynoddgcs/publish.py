from google.cloud import storage
import pycocotools.coco
import os
import urllib.parse
import io
import json
from .connect import GCS

# https://storage.googleapis.com/nmfs_odp_pifsc/PIFSC/SOD/MOUSS/jpg/20161014_205317_2/20161014.205850.178.003437.jpg
GCS_ROOT = 'https://storage.googleapis.com'
class NODDCOCODataset(object):
    """
    Class for validating and uploading annotations in COCO format.
    
    Parameters
    ----------
    coco: pycocotools.coco.COCO
        a COCO-format file
    dataset_root: str
        the root url path for datasets, including the bucket name, etc.
        consider computing with `dataset_path` function.
    """

    def __init__(self, coco_file: str, dataset_root: str, bucket:str): 
        self.coco_file = coco_file
        self.coco = pycocotools.coco.COCO(coco_file)
        self.coco_root = os.path.split(self.coco_file)[0]
        self.bucket = bucket
        self.relative_gcs_path = dataset_root
        self.gcs = GCS()

    def compute_urls(self):
        """
        Compute and update the urls within this COCO file to reflect the
        expected location of the files within GCS
        """
        for i, image in self.coco.imgs.items():
            # discard the drive letter, if present
            file_name = os.path.splitdrive(image['file_name'])[1]
            splitfile = split_filename(file_name)
            image['coco_url'] = join_urlpath(
                GCS_ROOT,
                self.bucket,
                self.relative_gcs_path, 
                *splitfile
            )

    def unnest_filenames(self, file_separator = '_'):
        """
        pycocotools doesn't appreciated "nested" `file_name attributes.
        To this end, we replace the `file_name` attribute to be "unnested",
        by simply replacing instances of '/' with some other separator.

        Note that the URL will still potentially contain nested paths.
        This method is idempotent if the file_name attribute is unnested.

        Parameters
        ----------
        file_separator: str
            the string we use to replace '/' in the `file_name`
        """
        # COCO file_names should not be nested
        for i, image in self.coco.imgs.items():
            # discard the drive letter, if present
            file_name = os.path.splitdrive(image['file_name'])[1]
            splitfile = split_filename(file_name)
            image['file_name'] = file_separator.join(splitfile)

    def upload_images(self):
        """
        Upload the images in this COCO metadata file to GCS.
        The files should be located at the location specified with 
        the `file_name` attribute, either absolute or relative to 
        the location of the COCO file.
        """
        for i, image in self.coco.imgs.items():
            print(image['file_name'])
            # discard the drive letter, if present
            file_name = os.path.splitdrive(image['file_name'])[1]
            splitfile = split_filename(file_name)
            destination = join_urlpath(
                self.relative_gcs_path, *splitfile
            )
            if os.path.isabs(image['file_name']):
                source = image['file_name']
            else:
                source = os.path.join(self.coco_root, image['file_name'])
            self.gcs.upload(self.bucket, source, destination)
    
    def upload_coco(self):
        """
        Upload the COCO file, but adjusted so that file urls point to files
        in the GCS bucket.
        """
        self.compute_urls()
        self.unnest_filenames()
        newcoco = json.dumps(self.coco.dataset)
        destination = join_urlpath(
            self.relative_gcs_path, 'annotations.json'
        )
        self.gcs.upload_string(self.bucket, newcoco, destination)

    def upload(self):
        """
        Upload this dataset, first the images, then the adjusted COCO file.
        """
        print("uploading images")
        self.upload_images()
        print("uploading coco file")
        self.upload_coco()

def dataset_path(datasets_root, organization, project):
    """
    Get a url path for uploading/downloading GCS NODD data based on
    the bucket, organization, etc.

    ```
    >>> dataset_path('bar', 'pickles+fish', 'project')
    'bar/pickles%2Bfish/project'
    
    ```

    Parameters
    ----------
    datasets_root: str
        the root directory within the bucket where we are hosting datasets
    organization: str
        the first-level fixed-depth directory for organizing datasets
    project: str
        the second-level fixed-depth directory for organizing datasets
    """
    return join_urlpath(
        datasets_root, organization, project)

def join_urlpath(*paths):
    """
    Joins a bunch of strings into a slash-delimited url path.

    ``` 
    >>> join_urlpath('foo', 'bar', 'pickles+fish', 'project')
    'foo/bar/pickles%2Bfish/project'
    
    >>> join_urlpath('http://foo', 'bar', 'pickles+fish', 'project')
    'http://foo/bar/pickles%2Bfish/project'

    >>> join_urlpath('http://foo/bar', 'pickles+fish/project')
    'http://foo/bar/pickles%2Bfish/project'
    
    ```

    Parameters
    ----------
    *paths: list[str]
        a variable-length list of path elements to join

    Returns
    -------
    url: str
        the joined url
    """
    url = '/'.join(s.strip('/') for s in paths)
    return urllib.parse.quote(url, safe=':/')
        
def split_filename(filename):
    """
    Splits a filename into all of its component directory structure

    Does the same thing as `os.path.split`, but completely splits the 
    directory structure into all parts, instead of just two (head/tail)

    ```
    
    >>> split_filename('foo/bar/pickles')
    ['foo', 'bar', 'pickles']

    ```

    Parameters
    ----------
    filename: str
        A filename to split

    Returns
    -------
    paths: list[str]
        The completely split list of directories 
        (and possibly the terminating filename)
    """
    paths = []
    tail = "totally_arbitrary"
    while filename and tail:
        filename, tail = os.path.split(filename)
        if tail and tail != '.' and tail != '..':
            paths.append(tail)
    return paths[::-1]