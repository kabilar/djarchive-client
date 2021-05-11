
import os
import logging

import posixpath as ufs

from itertools import chain
from itertools import repeat

from minio import Minio
from datajoint import config as cfg
from tqdm import tqdm


log = logging.getLogger(__name__)


class DJArchiveClient(object):
    def __init__(self, **kwargs):
        '''
        Create a DJArchiveClient.
        Normal client code should use the 'client' method.
        '''

        self.bucket = kwargs['bucket']
        self.endpoint = kwargs['endpoint']
        self.access_key = kwargs['access_key']
        self.secret_key = kwargs['secret_key']

        self.client = Minio(self.endpoint, access_key=self.access_key,
                            secret_key=self.secret_key)

    @classmethod
    def client(cls, admin=False):
        '''
        Create a DJArchiveClient.

        Currently:

            Admin usage expects dj.config['custom'] values for:

              - djarchive.access_key
              - djarchive.secret_key

            Client and admin usage allow overriding dj.config['custom']
            defaults for:

              - djarchive.bucket
              - djarchive.endpoint

        The configuration mechanism is expected to change to allow for
        more general purpose client usage without requiring extra
        configuration.
        '''

        dj_custom = cfg.get('custom', {})

        cfg_defaults = {
            'djarchive.bucket': 'djhub.vathes.datapub.elements',
            'djarchive.endpoint': 's3.djhub.io'
        }

        create_args = {k: {**cfg_defaults, **dj_custom}.get(
            'djarchive.{}'.format(k), None)
                       for k in ('endpoint', 'access_key', 'secret_key',
                                 'bucket')}

        return cls(**create_args)

    def upload(name, revision, source_directory):
        '''
        upload contents of source_directory as the dataset of name/revision

        (currently placeholder for API design)
        '''
        raise NotImplementedError('upload not implemented')

    def redact(name, revision):
        '''
        redact (revoke) dataset publication of name/revision

        (currently placeholder for API design)

        XXX: workflow data safety concerns?
        '''
        raise NotImplementedError('redaction not implemented')

    def datasets(self):
        '''
        return the available datasets as a generator of dataset names
        '''

        # s3://bucket/dataset -> generator(('dataset'))

        for ds in (o for o in self.client.list_objects(self.bucket)
                   if o.is_dir):
            yield ds.object_name.rstrip('/')

    def revisions(self, dataset=None):
        '''
        return the list of available dataset revisions as a generator
        of (dataset_name, dataset_revision) tuples.
        '''
        def _revisions(dataset):

            # s3://bucket/dataset/revision ->
            #    generator(('dataset', 'revision'), ...)

            pfx = '{}/'.format(dataset)

            for ds in (o for o in self.client.list_objects(
                    self.bucket, prefix=pfx) if o.is_dir):

                yield tuple(ds.object_name.rstrip('/').split(ufs.sep))

        nfound = 0

        datasets = (dataset,) if dataset else self.datasets()

        for ds in datasets:
            for i, r in enumerate(_revisions(ds), start=1):
                nfound = i
                yield r

        if dataset and not nfound:

            msg = 'dataset {} not found'.format(dataset)
            log.debug(msg)
            raise FileNotFoundError(msg)

    def download(self, dataset_name, revision, target_directory,
                 create_target=False, display_progress=False):

        '''
        download a dataset's contents into the top-level of target_directory.

        when create_target is specified, target_directory and parents
        will be created, otherwise, an error is signaled.
        '''

        os.makedirs(target_directory, exist_ok=True) if create_target else None

        if not os.path.exists(target_directory):

            msg = 'target_directory {} does not exist'.format(target_directory)
            log.warning(msg)
            raise FileNotFoundError(msg)

        pfx = ufs.join(dataset_name, revision)

        # main download loop -
        #
        # iterate over objects,
        # convert full source path to source subpath,
        # construct local path and create local subdirectory in the target
        # then fetch the object into the local path.
        #
        # local paths are dealt with using OS path for native support,
        # paths in the s3 space use posixpath since these are '/' delimited

        nfound = 0

        obj_iter = self.client.list_objects(
            self.bucket, recursive=True, prefix=pfx)

        for obj in obj_iter:

            assert not obj.is_dir  # assuming dir not in recursive=True list

            spath = obj.object_name  # ds/rev/<...?>/thing

            ssubp = spath.replace(  # <...?>/thing
                ufs.commonprefix((pfx, spath)), '').lstrip('/')

            # target_directory/<...?>/thing
            lpath = os.path.join(target_directory, *ssubp.split(ufs.sep))
            lsubd, _ = os.path.split(lpath)

            # ensure we are not creating outside of target_directory
            assert (os.path.commonprefix((target_directory, lpath))
                    == target_directory)

            xfer_msg = 'transferring {} to {}'.format(spath, lpath)

            log.debug(xfer_msg)

            if display_progress:
                print(xfer_msg)

            os.makedirs(lsubd, exist_ok=True)

            self.fget_object(spath, lpath, display_progress=display_progress)

            nfound += 1

        if not nfound:

            msg = 'dataset {} revision {} not found'.format(
                dataset_name, revision)

            log.debug(msg)

            raise FileNotFoundError(msg)

    def fget_object(self, spath, lpath, display_progress=False):

        statb = self.client.stat_object(self.bucket, spath)

        chunksz = 1024 ** 2  # 1 MiB (TODO? configurable)

        nchunks, leftover = statb.size // chunksz, statb.size % chunksz

        chunker = (chain(repeat(chunksz, nchunks), (leftover,)) if leftover
                   else repeat(chunksz, nchunks))

        chunker = tqdm(chunker, unit='MiB', ncols=60,
                       disable=not display_progress,
                       total=nchunks + 1 if leftover else nchunks)

        offset = 0
        with open(lpath, 'wb') as fh:
            for chunk in chunker:
                dat = self.client.get_object(
                    self.bucket, spath, offset=offset, length=chunk)
                fh.write(dat.data)
                offset += chunk


client = DJArchiveClient.client  # export factory method as utility function
